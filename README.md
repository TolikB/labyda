# HFT Crypto Arbitrage Engine

Async Python engine for monitoring Polymarket price-target markets against CeFi futures order books and routing delta-neutral arbitrage signals.

Default mode is safe dry-run:

```json
{
  "isTest": true,
  "max_order_size_usd": 100.0,
  "min_net_spread": 0.05
}
```

## Layout

- `src/arbitrage_engine/quant.py` - spread, slippage, sizing, and signal math.
- `src/arbitrage_engine/execution.py` - dry-run and production execution router.
- `src/arbitrage_engine/connectors/` - Polymarket and CeFi adapter boundaries.
- `src/arbitrage_engine/telegram.py` - async Telegram Bot API notifier.
- `src/arbitrage_engine/engine.py` - orchestration loop.
- `src/arbitrage_engine/positions.py` - in-memory open position ledger for exit logic.
- `tests/` - unit tests for the deterministic core.

## Configuration

Copy `config.example.json` to `config.json` and fill credentials through environment variables or direct config values.

```powershell
python -m arbitrage_engine.main --config config.json
```

Production trading requires implementing and validating exchange-specific credentials, permissions, and market metadata. Keep `isTest=true` until both legs are tested on small markets.

## Auto Close

Set `auto_close.enabled=true` and provide `expires_at` per market. When an open position is inside `close_before_expiry_seconds` and the Polymarket best bid implies profit above `take_profit_pct` (`0.10` by default), the router closes the Polymarket leg and sends the reverse market order for the CeFi hedge. In `isTest=true`, it only sends the Telegram report.
