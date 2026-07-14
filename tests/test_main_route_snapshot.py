from datetime import UTC, datetime, timedelta

from arbitrage_engine.main import _build_route_market_snapshot
from arbitrage_engine.models import BinarySide, MarketSpec


def test_build_route_market_snapshot_synthesizes_predict_sx_from_predict_and_sx_families() -> None:
    expires_at = datetime.now(UTC) + timedelta(hours=2)
    markets = [
        MarketSpec(
            symbol="Will Team A win?",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-no",
            predict_fun_side=BinarySide.NO,
            polymarket_market_id="poly-market",
            predict_fun_market_id="predict-market",
            predict_fun_fee_rate_bps=19,
            venue_b_label="Predict.fun",
            expires_at=expires_at,
        ),
        MarketSpec(
            symbol="Will Team A win?",
            target_label="NO",
            polymarket_token_id="poly-no",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="sx-yes",
            predict_fun_side=BinarySide.YES,
            polymarket_market_id="poly-market",
            predict_fun_market_id="sx-market",
            venue_b_label="SX Bet",
            expires_at=expires_at,
        ),
    ]

    snapshot = _build_route_market_snapshot(markets)

    predict_sx = next(
        market
        for market in snapshot
        if market.venue_a_label == "Predict.fun" and market.venue_b_label == "SX Bet"
    )
    assert predict_sx.polymarket_token_id == "predict-no"
    assert predict_sx.polymarket_side is BinarySide.NO
    assert predict_sx.polymarket_market_id == "predict-market"
    assert predict_sx.predict_fun_token_id == "sx-yes"
    assert predict_sx.predict_fun_side is BinarySide.YES
    assert predict_sx.predict_fun_market_id == "sx-market"
