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

For a single cycle:

```powershell
python -m arbitrage_engine.main --config config.json --once
```

Production trading requires implementing and validating exchange-specific credentials, permissions, and market metadata. Keep `isTest=true` until both legs are tested on small markets.

Required live credentials:

- `BINANCE_API_KEY` / `BINANCE_API_SECRET` for Binance USD-M futures.
- `POLYMARKET_PRIVATE_KEY` for CLOB signing.
- `POLYMARKET_FUNDER_ADDRESS` when the signing key differs from the funded Polymarket wallet, or when using a proxy/deposit wallet.
- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` for notifications.

Each market needs `symbol`, `target_label`, Polymarket YES/NO side, and CeFi futures symbol. If `polymarket_token_id` and `condition_id` are empty, the bot discovers them from the public Polymarket Gamma API before starting. If `tick_size` and `neg_risk` are omitted, the connector fetches them from Polymarket by `condition_id` before posting an order.

## Auto Close

Set `auto_close.enabled=true` and provide `expires_at` per market. When an open position is inside `close_before_expiry_seconds` and the Polymarket best bid implies profit above `take_profit_pct` (`0.10` by default), the router closes the Polymarket leg and sends the reverse market order for the CeFi hedge. In `isTest=true`, it only sends the Telegram report.
