from __future__ import annotations

from .config import AppConfig
from .connectors.base import BinaryMarketClient, PolymarketClient, PredictFunClient
from .execution import ExecutionRouter
from .models import ExitSignal, OpenPosition
from .positions import PositionLedger
from .quant import calculate_binary_position_profit


class PositionManager:
    def __init__(
        self,
        *,
        config: AppConfig,
        polymarket: PolymarketClient,
        predict_fun: PredictFunClient | None,
        execution: ExecutionRouter | None,
        myriad: PredictFunClient | None = None,
        myriad_execution: ExecutionRouter | None = None,
        predict_myriad_execution: ExecutionRouter | None = None,
        ledger: PositionLedger | None = None,
    ) -> None:
        self._config = config
        self._polymarket = polymarket
        self._predict_fun = predict_fun
        self._execution = execution
        self._myriad = myriad
        self._myriad_execution = myriad_execution
        self._predict_myriad_execution = predict_myriad_execution
        self._ledger = ledger or (
            execution.ledger
            if execution is not None
            else myriad_execution.ledger
            if myriad_execution is not None
            else PositionLedger()
        )

    async def run_once(self) -> None:
        if not self._config.auto_close.enabled:
            return

        for position in self._ledger.all():
            route = self._route_for_position(position)
            if route is None:
                continue
            execution, first_leg, second_leg = route
            if position.status == "unwind_pending":
                await execution.retry_pending_unwind(position)
                continue
            if position.status == "partial_exit_pending":
                await execution.retry_partial_exit(position)
                continue
            await self._check_open_position(position, execution, first_leg, second_leg)

    async def _check_open_position(
        self,
        position: OpenPosition,
        execution: ExecutionRouter,
        first_leg: BinaryMarketClient,
        second_leg: BinaryMarketClient,
    ) -> None:
        first_book = await first_leg.watch_order_book(position.market.polymarket_token_id)
        second_book = await second_leg.watch_order_book(position.market.predict_fun_token_id)
        first_exit = first_book.best_bid.price
        second_exit = second_book.best_bid.price
        exit_spread = 1.0 - (first_exit + second_exit)
        if exit_spread >= self._config.auto_close.exit_spread_pct:
            return
        profit_pct, profit_usd = calculate_binary_position_profit(
            entry_total_cost=position.polymarket_entry_price + position.predict_fun_entry_price,
            exit_total_value=first_exit + second_exit,
            payout_contracts=position.polymarket_contracts,
        )
        await execution.handle_exit_signal(
            ExitSignal(
                position=position,
                polymarket_exit_price=first_exit,
                predict_fun_exit_price=second_exit,
                profit_pct=profit_pct,
                profit_usd=profit_usd,
                exit_spread=exit_spread,
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
        if position.market.venue_b_label == "Myriad" and self._myriad is not None and self._myriad_execution is not None:
            return self._myriad_execution, self._polymarket, self._myriad
        if self._execution is not None and self._predict_fun is not None:
            return self._execution, self._polymarket, self._predict_fun
        return None
