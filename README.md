# Binary Prediction Arbitrage Engine

Async Python engine for binary arbitrage between Polymarket, Predict.fun, and Myriad Markets. The active strategy buys opposite outcomes for the same event across every supported pair: `Polymarket ↔ Predict.fun`, `Polymarket ↔ Myriad`, and `Predict.fun ↔ Myriad`.

Default mode is safe dry-run:

```json
{
  "isTest": true,
  "position_size_usd": 100.0,
  "min_net_spread": 0.10,
  "signal_alert_cooldown_seconds": 900
}
```

## Core Rule

Entry is allowed only when:

```text
P_first_venue + P_second_venue + slippage + fees < 1.0 - min_net_spread
```

With the default `min_net_spread=0.10`, entry requires spread strictly above `10%`. Any signal with combined cost at or above `$0.90` per `$1.00` payout is rejected.

## Layout

- `src/arbitrage_engine/quant.py` - binary spread, orderbook/AMM fills, slippage cap, sizing.
- `src/arbitrage_engine/execution.py` - dry-run and production two-leg router.
- `src/arbitrage_engine/position_manager.py` - open position supervisor, exit checks, partial-close retries, unwind retries.
- `src/arbitrage_engine/connectors/polymarket.py` - Polymarket CLOB SDK + WebSocket orderbook.
- `src/arbitrage_engine/connectors/predict_fun.py` - Predict.fun API boundary.
- `src/arbitrage_engine/market_discovery.py` - Polymarket Gamma resolver.
- `src/arbitrage_engine/matcher.py` - semantic matcher with 30-minute expiry hard-stop.
- `src/arbitrage_engine/positions.py` - JSON persisted open position ledger.
- `src/arbitrage_engine/myriad_discovery.py` - Myriad market resolver.
- `src/arbitrage_engine/connectors/myriad.py` - Myriad EIP-712 CLOB connector.

## Run

Install dependencies, copy `config.example.json` to `config.json`, then fill `.env`.

```powershell
python -m pip install -e ".[dev]"
copy config.example.json config.json
copy .env.example .env
```

```powershell
python -m arbitrage_engine.main --config config.json --once
python -m arbitrage_engine.main --config config.json
```

Required live secrets:

- `POLYMARKET_PRIVATE_KEY`
- `PREDICT_FUN_PRIVATE_KEY`
- `PREDICT_FUN_API_KEY` for Predict.fun mainnet REST order submission
- `MYRIAD_API_KEY`
- `MYRIAD_PRIVATE_KEY`
- Optional `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` for notifications

`config.example.json` uses public defaults for Polymarket CLOB, Predict.fun mainnet REST, Myriad API, and BNB RPC. Override `predict_fun.rpc_url`, `predict_fun.api_base_url`, `myriad_markets.rpc_url`, `myriad_markets.api_url`, or `web3_networks.bnb.rpc_url` in `config.json` if you use private infrastructure.

Polymarket metadata is resolved from Gamma when `polymarket_token_id` is empty. Predict.fun metadata is resolved from the Predict.fun markets API when `predict_fun_token_id` is empty. Myriad metadata is resolved from `/markets` when `myriad_market_id` is empty. The matchers require compatible expiry windows and use semantic title/outcome matching before a market is accepted.

Predict.fun execution uses `predict-sdk`: the connector builds a marketable SDK order, signs it locally as EIP-712 with `PREDICT_FUN_PRIVATE_KEY`, then submits the signed order to the Predict.fun REST API. The private key is never sent to the API. Balance checks use the Predict.fun USDT collateral address from the SDK unless `predict_fun.collateral_token_address` is explicitly set.

Myriad execution is a BNB Chain CLOB flow: the connector builds a Myriad order, signs the EIP-712 payload locally with `MYRIAD_PRIVATE_KEY`, and submits `{ order, signature, network_id, time_in_force }` to the Myriad API with `MYRIAD_API_KEY`. Myriad orders use IOC so stale orders do not rest in the book. The configured collateral token balance is checked on-chain through `myriad_markets.rpc_url`.

Predict.fun and Myriad are treated as hybrid CLOB venues, not AMMs. Order placement and cancellation are off-chain REST calls with locally signed EIP-712 orders; balance and collateral operations are on-chain through BNB Chain RPC. The bot polls REST order books for market data.

Decimals are handled explicitly:

- collateral balances use 6 decimals;
- order `amount` uses 18 decimals;
- order `price` uses 18 decimals;
- large integer order fields are serialized as strings in REST payloads where the API expects JSON.

## Auto Close

When enabled, auto-close compares the combined exit bids of both binary legs. A position is closed when the remaining market spread is below `auto_close.exit_spread_pct`, which defaults to `2%`. In `isTest=true`, it only sends the Telegram report and removes the simulated position from the local ledger.

Open positions are checked by `PositionManager`, separate from new signal scanning. It walks the persisted ledger each cycle, selects the correct venue route for each position, retries pending unwind/partial exits, and closes positions when the exit rule is met.

In production, close handling is leg-aware. If one exit leg fills and the other does not, the ledger marks only the filled leg as closed and retries only the remaining leg on later cycles. A full close notification is sent only after both legs are confirmed closed.

## Notifications

Signal notifications are throttled per pair by `signal_alert_cooldown_seconds` and default to one alert per 15 minutes. Open and close notifications are separate:

- signal detected: spread currently passes filters;
- position opened: both entry legs filled, with approximate expected profit;
- position closed: both exit legs filled, with realized profit based on confirmed exit prices.

## Timeouts And Recovery

`polymarket_fill_timeout_ms` defaults to 500 ms. `predict_fun_fill_timeout_ms` and `myriad_fill_timeout_ms` default to 4000 ms for BNB Chain backed CLOB execution. Config validation allows Polymarket down to 300 ms and BNB-backed venues down to 3600 ms. Limit prices still protect against fills worse than the submitted price.

If the second entry leg fails after the first leg is already filled, the bot attempts an automatic first-leg unwind using the current best bid from the live order book. If immediate unwind does not fill, the position is saved as `unwind_pending` and retried automatically on later cycles.

## Liquidity Guard

Position sizing is controlled by `position_size_usd`. The bot splits that target across the two legs, walks the full order book, and uses weighted average fill price for spread calculations. If the full target size cannot be filled, or price impact exceeds `1.5%`, the signal is rejected instead of shrinking the order size.

Before a production entry, the router checks available balance for both venues and subtracts capital already reserved by open positions in the local ledger. Multiple positions can be opened across markets/routes as long as the venue balances cover the next position.
