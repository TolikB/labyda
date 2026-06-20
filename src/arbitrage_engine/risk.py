from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

_STATE_FILE_LOCK = threading.Lock()
LOGGER = logging.getLogger(__name__)
PauseCallback = Callable[[], Awaitable[None]]


class AsyncRiskStateStore(Protocol):
    async def load_risk_state(self) -> dict[str, Any] | None: ...

    async def save_risk_state(self, state: dict[str, Any]) -> None: ...


class GlobalRiskController:
    """Process-wide execution circuit with durable, manually reset pause state."""

    def __init__(
        self,
        max_daily_loss_usd: float | Decimal,
        max_consecutive_api_errors: int,
        state_path: str | Path | None = None,
        state_store: AsyncRiskStateStore | None = None,
    ) -> None:
        self._max_daily_loss_usd = Decimal(str(max_daily_loss_usd))
        self._max_consecutive_api_errors = max_consecutive_api_errors
        self._state_path = Path(state_path) if state_path is not None else None
        self._state_store = state_store
        self._lock = asyncio.Lock()
        self._pause_callbacks: list[PauseCallback] = []
        self.daily_loss_usd = Decimal(0)
        self.consecutive_api_errors = 0
        self.paused = False
        self.pause_reason: str | None = None
        self.loss_day = datetime.now(UTC).date()
        self._load()

    async def initialize(self) -> None:
        if self._state_store is None:
            return
        try:
            state = await self._state_store.load_risk_state()
            if state is None:
                await self._persist_external()
                return
            self.loss_day = datetime.fromisoformat(str(state.get("loss_day", self.loss_day))).date()
            self.daily_loss_usd = max(Decimal(0), Decimal(str(state.get("daily_loss_usd", 0))))
            self.consecutive_api_errors = max(0, int(state.get("consecutive_api_errors", 0)))
            self.paused = bool(state.get("paused", False))
            self.pause_reason = str(state["pause_reason"]) if state.get("pause_reason") else None
        except Exception as exc:
            self.paused = True
            self.pause_reason = f"risk state could not be loaded from durable store: {exc}"
            LOGGER.exception("risk_store_load_failed_pausing_execution")

    def register_pause_callback(self, callback: PauseCallback) -> None:
        self._pause_callbacks.append(callback)

    def is_paused(self) -> bool:
        return self.paused

    async def record_realized_result(
        self,
        profit_usd: float | Decimal,
        fees_usd: float | Decimal = Decimal(0),
    ) -> bool:
        net_result = Decimal(str(profit_usd)) - max(Decimal(0), Decimal(str(fees_usd)))
        if net_result >= 0:
            return False
        async with self._lock:
            self._roll_loss_day_forward()
            self.daily_loss_usd += abs(net_result)
            newly_paused = self._set_paused_if_limit_reached(
                f"daily realized loss ${self.daily_loss_usd:.2f} reached limit ${self._max_daily_loss_usd:.2f}"
            )
            self._persist()
        await self._persist_external()
        if newly_paused:
            await self._run_pause_callbacks()
        return newly_paused

    async def record_api_error(self) -> bool:
        async with self._lock:
            self.consecutive_api_errors += 1
            newly_paused = False
            if self.consecutive_api_errors >= self._max_consecutive_api_errors and not self.paused:
                self.paused = True
                self.pause_reason = f"{self.consecutive_api_errors} consecutive execution API errors"
                newly_paused = True
            self._persist()
        await self._persist_external()
        if newly_paused:
            await self._run_pause_callbacks()
        return newly_paused

    async def pause(self, reason: str) -> bool:
        async with self._lock:
            newly_paused = not self.paused
            self.paused = True
            self.pause_reason = reason
            self._persist()
        await self._persist_external()
        if newly_paused:
            await self._run_pause_callbacks()
        return newly_paused

    async def reset_api_errors(self) -> None:
        async with self._lock:
            if self.consecutive_api_errors:
                self.consecutive_api_errors = 0
                self._persist()
        await self._persist_external()

    async def resume(self) -> None:
        """Explicit operator action; automatic day rollover never resumes trading."""
        async with self._lock:
            current_day = datetime.now(UTC).date()
            if current_day == self.loss_day and self.daily_loss_usd >= self._max_daily_loss_usd:
                raise RuntimeError(
                    "Cannot resume risk on the same UTC day while the daily-loss limit remains exceeded"
                )
            if current_day != self.loss_day:
                self.loss_day = current_day
                self.daily_loss_usd = Decimal(0)
            self.consecutive_api_errors = 0
            self.paused = False
            self.pause_reason = None
            self._persist()
        await self._persist_external()

    def _roll_loss_day_forward(self) -> None:
        current_day = datetime.now(UTC).date()
        if current_day != self.loss_day and not self.paused:
            self.loss_day = current_day
            self.daily_loss_usd = Decimal(0)

    def _set_paused_if_limit_reached(self, reason: str) -> bool:
        if self.daily_loss_usd < self._max_daily_loss_usd or self.paused:
            return False
        self.paused = True
        self.pause_reason = reason
        return True

    async def _run_pause_callbacks(self) -> None:
        # Registration order is deliberate: execution routers cancel orders
        # first, then the reconciliation callback snapshots venue state.
        for callback in self._pause_callbacks:
            try:
                await callback()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.error("risk_pause_callback_failed", extra={"_error": str(exc)})

    def _load(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            with _STATE_FILE_LOCK:
                payload = json.loads(self._state_path.read_text(encoding="utf-8"))
            state = payload.get("global_risk", {})
            if not isinstance(state, dict):
                return
            self.loss_day = datetime.fromisoformat(str(state.get("loss_day", self.loss_day))).date()
            self.daily_loss_usd = max(Decimal(0), Decimal(str(state.get("daily_loss_usd", 0))))
            self.consecutive_api_errors = max(0, int(state.get("consecutive_api_errors", 0)))
            self.paused = bool(state.get("paused", False))
            self.pause_reason = str(state["pause_reason"]) if state.get("pause_reason") else None
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self.paused = True
            self.pause_reason = f"risk state could not be loaded: {exc}"
            LOGGER.exception("risk_state_load_failed_pausing_execution", extra={"_path": str(self._state_path)})

    def _persist(self) -> None:
        if self._state_path is None:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        with _STATE_FILE_LOCK:
            payload: dict[str, object] = {}
            if self._state_path.exists():
                try:
                    loaded = json.loads(self._state_path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        payload = loaded
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    payload = {}
            payload["global_risk"] = {
                "loss_day": self.loss_day.isoformat(),
                "daily_loss_usd": str(self.daily_loss_usd),
                "consecutive_api_errors": self.consecutive_api_errors,
                "paused": self.paused,
                "pause_reason": self.pause_reason,
            }
            temporary_path = self._state_path.with_name(f"{self._state_path.name}.tmp")
            with temporary_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, self._state_path)

    async def _persist_external(self) -> None:
        if self._state_store is None:
            return
        await self._state_store.save_risk_state(
            {
                "loss_day": self.loss_day.isoformat(),
                "daily_loss_usd": self.daily_loss_usd,
                "consecutive_api_errors": self.consecutive_api_errors,
                "paused": self.paused,
                "pause_reason": self.pause_reason,
            }
        )
