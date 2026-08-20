# SX Bet V3 Cutover

## Implemented contract

The runtime is dual-stack. `sx_bet.api_version` selects the connector:

- `v2`: the existing mainnet `/orders/fill/v2` integration
- `v3`: OBv3 aggregated books, eight-field EIP-712 orders, proxy balances,
  account fee metadata, and V3 reconciliation endpoints

V3 support includes:

- `GET /metadata/obv3` with runtime domain, chain, token, ladder, and limits
- `GET /orderbook-v3/snapshot` and `orderbook_v3:{marketHash}` full-book replacement
- strictly increasing book `version` handling and positioned Centrifugo recovery
- `GET /user/realtime-token-v3/api-key` using `x-sx-api-key`
- proxy readiness from `GET /user/proxy`
- signer/proxy/token/escrow-bound balance validation from `GET /user/balance-v3`
- per-account `takerPayoutFee` and `refundFee` from `GET /user/fees-v3`
- `POST /orders-v3` taker orders with `FOK`, `waitForOutcome=true`, and the
  documented 15-second maximum wait to cover live-event betting delays
- local EIP-712 digest verification against the returned `orderId`
- 32-byte hex salts and short signed expiry that exceeds the largest live
  `bettingDelay` from metadata for immediate taker orders
- unknown-ACK reconciliation by the locally computed digest
- restart-safe BUY/SELL order restoration from durable PostgreSQL intents,
  validated against the venue market and outcome fields
- fail-closed handling while a `FILLED` order is still missing indexed fills
- `TIMEOUT` outcomes remain in-flight and are reconciled instead of being
  misclassified as cancelled
- cancellation through `DELETE /orders-v3`
- orders, fills, positions, and settlement reconciliation through the V3 APIs;
  fill lookup uses the documented `orderId` filter with a bounded `startDate`
  compatibility fallback and always verifies the signed order id locally

Normal bot startup never deploys or funds a proxy. Those are explicit account
operations and remain outside automated trading.

## Current cutover gate

Until the official V3 mainnet cutover, production stays explicitly on V2:

```json
{
  "sx_bet": {
    "api_version": "v2",
    "environment": "mainnet",
    "allow_v3_mainnet": false
  }
}
```

The V3 client rejects mainnet before `2026-08-25T15:00:00Z` and also requires
`allow_v3_mainnet=true`. The timestamp is a conservative literal conversion of
the documented `10:00 AM EST` cutover. Toronto remains usable for read-only and
testnet validation before then. The connector factory rejects V2 mainnet after
that timestamp so a stale deployment cannot silently continue on retired APIs.

Signed previews expose only non-sensitive order fields plus a SHA-256 signature
fingerprint. Salt and signature are never written to operator artifacts. For SX
routes, discovery stores the earliest venue cutoff and execution refuses to
submit inside the final 15-second kickoff safety window.

## Toronto proof

Use a separate V3 testnet key if authenticated account checks are required:

```json
{
  "sx_bet": {
    "api_version": "v3",
    "environment": "toronto",
    "allow_v3_mainnet": false,
    "time_in_force": "FOK",
    "api_base_url": "https://api.toronto.sx.bet",
    "ws_url": "wss://realtime.toronto.sx.bet/connection/websocket"
  }
}
```

Read-only schema check:

```bash
ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 \
python -m pytest \
  tests/test_live_schema_contracts.py::LiveSchemaContractTests::test_sx_bet_v3_toronto_read_only_contracts -q
```

Probe one V3 market without submitting an order:

```bash
python scripts/sx_bet_probe.py \
  --api-version v3 \
  --api-base-url https://api.toronto.sx.bet
```

## Mainnet activation checklist

1. Create a new V3 API key. A V2 key is not reusable.
2. Deploy the OBv3 proxy and confirm `GET /user/proxy` returns `deployed=true`.
3. Move the funded canary balance into the proxy and confirm
   `availableAmount + pendingAvailableAmount` with `GET /user/balance-v3`.
4. Change the production SX block to `api_version=v3`, `environment=mainnet`,
   `time_in_force=FOK`, and `allow_v3_mainnet=true` only after cutover.
5. Run the live schema contract with the V3 key, then `discovery overlap`,
   all-market readiness, and production audit in risk-pause/shadow mode.
6. Resume only `bot-clob-hft` for the funded canary after proxy balance, fee,
   signed preview, reconciliation, and risk gates all pass.

`IOC` remains available for explicit partial-fill policy in non-funded testing,
but production config validation requires `FOK` for SX V3 funded execution.

## Official references

- [Migrate to V3](https://docs.sx.bet/developers/migrate-to-v3)
- [Posting orders](https://docs.sx.bet/developers/posting-orders)
- [Taking liquidity](https://docs.sx.bet/developers/taking-liquidity)
- [Order lifecycle](https://docs.sx.bet/developers/order-lifecycle)
- [Fetching odds](https://docs.sx.bet/developers/odds)
- [Reading balances](https://docs.sx.bet/developers/balances-and-ledger)
- [Fees](https://docs.sx.bet/developers/fees)
