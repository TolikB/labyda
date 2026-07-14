from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from .config import AppConfig
from .connectors.base import BinaryMarketClient
from .execution import ExecutionRouter
from .market_mapping import route_key
from .models import ExitSignal, OpenPosition, first_leg_token_for_route, second_leg_token_for_route
from .positions import PositionLedger
from .quant import calculate_binary_position_profit

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .settlement import SettlementService


class PositionManager:
    def __init__(
        self,
        *,
        config: AppConfig,
        polymarket: BinaryMarketClient,
        predict_fun: BinaryMarketClient | None,
        execution: ExecutionRouter | None,
        sx_bet: BinaryMarketClient | None = None,
        sx_execution: ExecutionRouter | None = None,
        myriad: BinaryMarketClient | None = None,
        myriad_execution: ExecutionRouter | None = None,
        predict_myriad_execution: ExecutionRouter | None = None,
        predict_sx_execution: ExecutionRouter | None = None,
        sx_myriad_execution: ExecutionRouter | None = None,
        ledger: PositionLedger | None = None,
        settlement_service: SettlementService | None = None,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._predict_fun = predict_fun
        self._execution = execution
        self._sx_bet = sx_bet
        self._sx_execution = sx_execution
        self._myriad = myriad
        self._myriad_execution = myriad_execution
        self._predict_myriad_execution = predict_myriad_execution
        self._predict_sx_execution = predict_sx_execution
        self._sx_myriad_execution = sx_myriad_execution
        self._reported_unresolved_entries: set[str] = set()
        self._settlement_service = settlement_service
        self._ledger = ledger or (
            execution.ledger
            if execution is not None
            else sx_execution.ledger
            if sx_execution is not None
            else myriad_execution.ledger
            if myriad_execution is not None
            else predict_myriad_execution.ledger
            if predict_myriad_execution is not None
            else predict_sx_execution.ledger
            if predict_sx_execution is not None
            else sx_myriad_execution.ledger
            if sx_myriad_execution is not None
            else PositionLedger()
        )

    async def run_once(self) -> None:
        if self._settlement_service is not None:
            await self._settlement_service.run_once()
        for position in self._ledger.all():
            try:
                route = self._route_for_position(position)
                if route is None:
                    continue
                execution, first_leg, second_leg = route
                if position.status == "entry_pending":
                    if position.market.symbol not in self._reported_unresolved_entries:
                        self._reported_unresolved_entries.add(position.market.symbol)
                        LOGGER.critical(
                            "unresolved_entry_intent_requires_reconciliation",
                            extra={"_symbol": position.market.symbol},
                        )
                    continue
                if position.status == "unwind_pending":
                    await execution.retry_pending_unwind(position)
                    continue
                if position.status == "partial_exit_pending":
                    await execution.retry_partial_exit(position)
                    continue
                if position.status != "open":
                    continue
                if not self._config.auto_close.enabled:
                    continue
                await self._check_open_position(position, execution, first_leg, second_leg)
            except Exception:
                LOGGER.exception(
                    "position_monitoring_failed",
                    extra={"_symbol": position.market.symbol, "_status": position.status},
                )

    def market_data_targets(self) -> dict[str, set[str]]:
        return self._ledger.market_data_targets()

    async def _check_open_position(
        self,
        position: OpenPosition,
        execution: ExecutionRouter,
        first_leg: BinaryMarketClient,
        second_leg: BinaryMarketClient,
    ) -> None:
        route = route_key(execution._first_leg_label, execution._second_leg_label)  # noqa: SLF001
        first_token_id = first_leg_token_for_route(position.market, route) or ""
        second_token_id = second_leg_token_for_route(position.market, route) or ""
        first_book, second_book = await asyncio.gather(
            first_leg.watch_order_book(first_token_id),
            second_leg.watch_order_book(second_token_id),
        )
        first_exit = first_book.best_bid.price
        second_exit = second_book.best_bid.price
        first_net_exit, second_net_exit = execution.net_exit_values(position.market, first_exit, second_exit)
        first_gross_entry, second_gross_entry = execution.gross_entry_values(
            position.market,
            position.polymarket_entry_price,
            position.predict_fun_entry_price,
        )
        exit_spread_decimal = Decimal(1) - (first_net_exit + second_net_exit)
        if exit_spread_decimal >= Decimal(str(self._config.auto_close.exit_spread_pct)):
            return
        profit_pct, profit_usd = calculate_binary_position_profit(
            entry_total_cost=first_gross_entry + second_gross_entry,
            exit_total_value=first_net_exit + second_net_exit,
            payout_contracts=position.polymarket_contracts,
        )
        await execution.handle_exit_signal(
            ExitSignal(
                position=position,
                polymarket_exit_price=Decimal(str(first_exit)),
                predict_fun_exit_price=Decimal(str(second_exit)),
                profit_pct=profit_pct,
                profit_usd=profit_usd,
                exit_spread=float(exit_spread_decimal),
            )
        )

    def _route_for_position(
        self, position: OpenPosition
    ) -> tuple[ExecutionRouter, BinaryMarketClient, BinaryMarketClient] | None:
        if (
            position.market.venue_a_label == "Predict.fun"
            and position.market.venue_b_label == "Myriad"
            and self._myriad is not None
            and self._predict_fun is not None
            and self._predict_myriad_execution is not None
        ):
            return self._predict_myriad_execution, self._predict_fun, self._myriad
        if (
            position.market.venue_a_label == "Predict.fun"
            and position.market.venue_b_label == "SX Bet"
            and self._predict_fun is not None
            and self._sx_bet is not None
            and self._predict_sx_execution is not None
        ):
            return self._predict_sx_execution, self._predict_fun, self._sx_bet
        if (
            position.market.venue_a_label == "SX Bet"
            and position.market.venue_b_label == "Myriad"
            and self._myriad is not None
            and self._sx_bet is not None
            and self._sx_myriad_execution is not None
        ):
            return self._sx_myriad_execution, self._sx_bet, self._myriad
        if (
            position.market.venue_b_label == "Myriad"
            and self._myriad is not None
            and self._myriad_execution is not None
        ):
            return self._myriad_execution, self._polymarket, self._myriad
        if (
            self._execution is not None
            and self._predict_fun is not None
            and position.market.venue_b_label == "Predict.fun"
        ):
            return self._execution, self._polymarket, self._predict_fun
        if (
            self._sx_execution is not None
            and self._sx_bet is not None
            and position.market.venue_b_label == "SX Bet"
        ):
            return self._sx_execution, self._polymarket, self._sx_bet
        return None
