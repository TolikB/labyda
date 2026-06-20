# Binary Prediction Arbitrage Engine

Async Python engine for binary arbitrage between Polymarket, Predict.fun, and Myriad Markets. The active strategy buys opposite outcomes for the same event across every supported pair: `Polymarket ↔ Predict.fun`, `Polymarket ↔ Myriad`, and `Predict.fun ↔ Myriad`.

Default mode is safe dry-run:

```json
{
  "isTest": true,
  "position_size_usd": 100.0,
  "min_entry_spread_pct": 0.08,
  "signal_alert_cooldown_seconds": 900
}
```

Set `scan_all=true` to build the candidate catalog from every valid Predict.fun market returned by the API. An empty `markets` array, an empty market symbol, or `symbol: "*"` also enables this mode. In scan-all mode `markets` is not used as a text filter; Polymarket and Myriad discovery then resolve matching markets from the full catalog. Set `scan_all=false` with explicit market symbols to use the filtered list.

When enabled, Predict.fun discovery uses the authenticated Mainnet endpoint `GET /v1/markets`; the deprecated unauthenticated `/markets` fallback is not used.

Predict.fun is optional. Set `predict_fun.enabled=false`, or leave `PREDICT_FUN_API_KEY` empty, to run the Polymarket/Myriad-only route. In that mode Predict.fun discovery, clients, balance checks, execution routes, and position-manager routes are not created. Myriad must be enabled and configured so at least one hedge venue remains active.

## Core Rule

Entry is allowed only when:

```text
P_first_venue + P_second_venue + slippage + fees < 1.0 - min_net_spread
```

With the example `min_entry_spread_pct=0.08`, entry requires spread strictly above `8%`. Any signal with combined cost at or above `$0.92` per `$1.00` payout is rejected.

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
python -m arbitrage_engine.main --config config.json --resume-risk-only
```

Required live secrets:

- `POLYMARKET_PRIVATE_KEY`
- `PREDICT_FUN_PRIVATE_KEY`
- `PREDICT_FUN_API_KEY` for Predict.fun mainnet REST order submission
- `MYRIAD_API_KEY` (optional; raises the public API rate limit)
- `MYRIAD_PRIVATE_KEY`
- Optional `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` for notifications

`config.example.json` uses public defaults for Polymarket CLOB, Predict.fun mainnet REST, Myriad API, and BNB RPC. Override `predict_fun.rpc_urls`, `predict_fun.api_base_url`, `myriad_markets.rpc_urls`, `myriad_markets.api_url`, or `web3_networks.bnb.rpc_urls` in `config.json` if you use private infrastructure. The legacy singular `rpc_url` key is still accepted; `rpc_urls` enables failover across multiple nodes.

Polymarket metadata is resolved from an in-memory snapshot when `polymarket_token_id` is empty. Startup downloads the complete cursor-paginated CLOB catalog and enriches external Myriad IDs through batched Gamma requests, then `resolve()` performs no HTTP requests. The snapshot refreshes atomically every five minutes; a failed refresh marks it unusable for subsequent resolution until a complete refresh succeeds. Active engine markets are not hot-replaced. Predict.fun metadata is resolved from the Predict.fun markets API when `predict_fun_token_id` is empty. Myriad metadata is resolved from `/markets` when `myriad_market_id` is empty. The matchers require compatible expiry windows and use strict title/outcome matching before a market is accepted.

Predict.fun execution uses `predict-sdk`: the connector builds a marketable SDK order, signs it locally as EIP-712 with `PREDICT_FUN_PRIVATE_KEY`, then submits the signed order to the Predict.fun REST API. The private key is never sent to the API. Balance checks use the Predict.fun USDT collateral address from the SDK unless `predict_fun.collateral_token_address` is explicitly set.

Order creation uses the current `{data: ...}` API envelope with fill-or-kill market orders. Status is polled by the returned order hash, while cancellation uses the returned order id through `POST /v1/orders/remove`. Market-specific `feeRateBps` discovered from Predict.fun is used for signing and profitability calculations; `predict_fun.fee_rate_bps` is the fallback for explicitly configured markets.

Myriad execution is a BNB Chain CLOB flow: the connector builds a Myriad order, signs the EIP-712 payload locally with `MYRIAD_PRIVATE_KEY`, and submits `{ order, signature, network_id, time_in_force }` to the Myriad API. `MYRIAD_API_KEY` is optional and increases rate limits. Arbitrage orders use FAK so unfilled quantity does not rest in the book. Prices are quantized down to Myriad's 0.01 tick grid. The configured collateral token balance is checked on-chain through `myriad_markets.rpc_url`.

Predict.fun and Myriad are treated as hybrid CLOB venues, not AMMs. Order placement and cancellation are off-chain REST calls with locally signed EIP-712 orders; balance and collateral operations are on-chain through BNB Chain RPC. Myriad order books are WebSocket-first with one semaphore-limited REST bootstrap per market. `myriad_markets.order_book_ttl_ms` defaults to 300 ms and `websocket_stale_after_ms` defaults to 1500 ms.

If Predict.fun REST orderbook reads fail and `predict_fun.market_abi_path` is configured, the Predict.fun connector falls back to direct RPC reserve reads. Discovery is also optional when token ids are provided explicitly in `config.json`; in that mode stale discovery endpoints do not block startup.

Decimals are handled explicitly:

- Polymarket USDC collateral uses 6 decimals;
- BNB collateral balances read `decimals()` dynamically from the ERC-20 contract before scaling;
- order `amount` uses 18 decimals;
- order `price` uses 18 decimals;
- large integer order fields are serialized as strings in REST payloads where the API expects JSON.

## Auto Close

When enabled, auto-close compares the combined exit bids of both binary legs. A position is closed when the remaining market spread is below `auto_close.exit_spread_pct`, which defaults to `1.5%`. In `isTest=true` or `shadow_mode=true`, it only sends a throttled Telegram report. Persisted positions are never deleted without confirmed live exit fills.

Open positions are checked by `PositionManager`, separate from new signal scanning. It walks the persisted ledger each cycle, selects the correct venue route for each position, retries pending unwind/partial exits, and closes positions when the exit rule is met.

In production, close handling is leg-aware. If one exit leg fills and the other does not, the ledger marks only the filled leg as closed and retries only the remaining leg on later cycles. A full close notification is sent only after both legs are confirmed closed.

Order fill polling returns an `ExecutionReport` containing `requested_amount`, `amount_filled`, `remaining_amount`, and status. If the second entry leg fills partially, the matched quantity remains as the hedged position and emergency unwind sells only the unmatched first-leg delta.

## Notifications

Signal notifications are throttled per pair by `signal_alert_cooldown_seconds` and default to one alert per 15 minutes. Open and close notifications are separate:

- signal detected: spread currently passes filters;
- position opened: both entry legs filled, with approximate expected profit;
- position closed: both exit legs filled, with realized profit based on confirmed exit prices.

Signal notifications include clickable links for the two venues in the active route. URLs are captured during startup discovery or read from explicit `*_url` market configuration; formatting a signal performs no network requests and does not delay order submission.

Set `telegram.log_raw_signal_books=true` only for short diagnostic windows. It records both triggering books and venue payloads; the production default is `false` to avoid oversized logs.

## Timeouts And Recovery

`polymarket_fill_timeout_ms` defaults to 500 ms. `predict_fun_fill_timeout_ms` and `myriad_fill_timeout_ms` default to 4000 ms for BNB Chain backed CLOB execution. Config validation allows Polymarket down to 300 ms and BNB-backed venues down to 3600 ms. Limit prices still protect against fills worse than the submitted price.

If the second entry leg fails after the first leg is already filled, the bot attempts an automatic first-leg unwind using the current best bid from the live order book. If immediate unwind does not fill, the position is saved as `unwind_pending` and retried automatically on later cycles.

Before either live entry leg is submitted, an fsync-backed `entry_pending` intent is written to the position ledger. If the process restarts with an unresolved intent, global risk starts paused and requires operator reconciliation before `--resume-risk-only` can clear the stop. Non-filled orders are cancelled and polled again for `cancel_reconcile_timeout_ms` so late partial fills are not silently discarded.

## Liquidity Guard

Position sizing is controlled by `position_size_usd`. The bot splits that target across the two legs, walks the full order book, and uses weighted average fill price for spread calculations. If the full target size cannot be filled, or price impact exceeds `1.5%`, the signal is rejected instead of shrinking the order size.

Before a production entry, the router checks available balance for both venues and subtracts capital already reserved by open positions in the local ledger. Multiple positions can be opened across markets/routes as long as the venue balances cover the next position.

## Global Risk Stop

All execution routes share one durable risk controller. Capital is reserved atomically before either leg is submitted; entry legs are then submitted concurrently. Reaching `max_daily_loss_usd` or `max_consecutive_api_errors` pauses every route, cancels tracked active orders, clears pending reservations, and requires the explicit `--resume-risk-only` operator command before trading can continue. Guard and transaction-timeout events include market metrics in Telegram notifications. Execution latency is emitted as structured `execution_pipeline_latency` records through a non-blocking logging queue.

`max_concurrent_market_evaluations` bounds scan-all work. Polymarket, Predict.fun, and Myriad bootstrap HTTP traffic is bounded and all clients reuse long-lived `aiohttp` sessions. Polymarket discovery uses sequential 1,000-market CLOB pages plus Gamma ID batches of up to 50, eliminating individual Gamma lookups. Set `shadow_mode=true` to exercise discovery, books, matching, sizing, and alerts with order submission and production balance gates disabled.

Every parsed order book carries its venue update timestamp when available, otherwise its local receipt timestamp. Both signal evaluation and production preflight reject either leg older than `max_orderbook_age_seconds`; configuration validation enforces the production-safe range `1.5`–`2.0` seconds (default `2.0`). Socket PONG/heartbeat frames never refresh book timestamps. A background heartbeat checks active Polymarket and Myriad subscriptions every `websocket_heartbeat_interval_seconds`; streams without actual market-data updates for `websocket_stale_after_seconds` are reconnected and reported to Telegram.

`max_production_price_impact` is the global safety ceiling applied to every venue-specific slippage setting. Looser venue settings are accepted but produce a startup warning and are capped explicitly. Persisted entry prices remain raw exchange fill prices; entry and exit fees are applied exactly once when profitability and realized PnL are calculated.
