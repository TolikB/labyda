# Opinion.trade Production Plan

Staged onboarding for Opinion.trade as the fifth venue and its four routes:
`polymarket_opinion`, `predict_opinion`, `sx_opinion`, `opinion_myriad`.

Companion to [`PRODUCTION_RUNBOOK.md`](PRODUCTION_RUNBOOK.md). Where the two
disagree, the runbook wins.

## Current conclusion

**Not fundable, but no longer for code reasons.** The connector is complete for
market data, discovery, reconciliation, balances and settlement, and the code
passes the full local bundle. What remains is evidence, not implementation: no
venue account exists, so nothing has been executed against the live venue. All
four routes are disabled in `routes` and `funded_routes`, and none appears in
`QUOTE_ARB_EXPECTED_FUNDED_ROUTES` in `production_closeout.sh`, so no release can
fund them by configuration alone.

Everything below written against the SDK is **unverified against the live
venue**. It was implemented from `opinion-clob-sdk` 0.7.0 source, which is a far
better source than prose documentation but is still not the venue itself.

## What the code already proves

- Market data matches the venue's real wire format. The `market.depth.diff`
  schema was taken from `opinion_clob_sdk.websocket_models.MarketDepthDiffMessage`,
  not from prose: one flat price level per frame, tagged `msgType`. A frame that
  is not a depth diff cannot reach the book, and a single level cannot seed one —
  only a REST snapshot can.
- Order construction is local, deterministic, and pinned against the real SDK
  model by `tests/test_opinion.py::test_order_payload_matches_the_vendored_sdk_contract`.
- Submission is fail-closed in both directions: a provable pre-transport refusal
  raises `OrderSubmissionRejected`, and everything ambiguous — including a
  transport timeout, which the SDK reports through the same wrapper as a refusal —
  raises `OpinionSubmissionUnknown` so the intent settles `UNKNOWN` and risk pauses.
- Balances are read on-chain, at the Safe address the SDK names as order maker,
  with decimals read from the token rather than assumed.
- Reconciliation implements the full contract, and the account fingerprint
  satisfies `canonical_external_baseline_payload`, so an external baseline can be
  captured if the funded wallet is not virgin.

## Remaining production gaps

| # | Gap | Consequence |
|---|---|---|
| 1 | No venue account | Blocks everything below. |
| 2 | Settlement is implemented but never executed | Redemption goes through `SafeConditionalTokensRedemption` against the Safe. The condition id is fetched from the venue and substituted for the market id at settlement time; that mapping has never been exercised on a real resolved market. |
| 3 | `taker_fee_rate_bps` is a placeholder | 400 bps is an assumption. `scripts/opinion_balance_and_order_preview.py` now reports the FeeManager settings the chain actually holds; replace the config with those before any funded window. |
| 4 | Route economics are pre-calibration placeholders | `route_floors` and `gas_units_by_route` were derived structurally from comparable routes, erring strict. Gas must be re-derived from a measured Safe transaction. |
| 5 | `persists_order_id_before_submission()` is `False` | Accepted residual risk, documented in the connector: the SDK signs internally and never exposes the digest, so there is no venue-agreed id to persist before the POST. |

## Plan of record

### Phase 1 — local code and contract audit

Before any VM action, and after any connector change:

```bash
PYTHONPATH=src python -m pytest tests/ -q
python -m mypy src tests
python -m ruff check src tests scripts
python -m compileall -q src tests scripts
bash -n ops/production_closeout.sh
```

Fail the phase on any regression in the shared execution, reconciliation, or
balance contracts — not only in Opinion tests.

### Phase 2 — account and live schema proof

1. Obtain an API key; fund a BNB Chain wallet with USDT collateral and BNB gas.
   The Safe is the order maker and holds collateral; the EOA derived from
   `OPINION_PRIVATE_KEY` only signs. Both must be configured.
2. Capture the balance evidence set:
   ```bash
   python scripts/opinion_balance_and_order_preview.py --config config.production.quote_arb.json
   ```
   Record: Safe address, signer address, collateral token address, raw
   `balanceOf`, decimals, scaled balance, connector `get_cash_balance()`, and the
   runtime effective balance. Any mismatch between them is a hard blocker.
3. Re-run against a live market with `--market-id/--token-id` and confirm the
   order book, constraints, `conditionId`, and both outcome token ids.
4. Connect to the WebSocket and **log one raw frame**. Confirm it matches
   `MarketDepthDiffMessage`. There is no sequence number, only a timestamp, so
   gap detection is impossible by construction — record that as accepted.
5. Read the real fee with `get_fee_rates(token_id)` and replace the
   `taker_fee_rate_bps` placeholder.
6. Measure gas for one Safe order and one redemption; replace the
   `gas_units_by_route` placeholders.
7. Determine whether the venue self-settles or requires an explicit claim. The
   `claimStatus` field on `PositionData` and the SDK's `redeem(market_id)`
   indicate an explicit claim. **This answer sets Phase 3's scope.**
8. Run the read-only contract suite, which is already written and currently
   skipping. It asserts the catalogue shape, the order book pricing inside the
   probability range and uncrossed, the market id -> 32-byte condition id
   mapping redemption depends on, and the account endpoints:
   ```bash
   ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 ARB_REQUIRE_OPINION_AUTH_CONTRACTS=1      python -m pytest tests/test_live_schema_contracts.py -q -k opinion
   ```
   Nothing in it can submit an order: the config it builds has no signing key.

### Phase 3 — settlement and redemption *(implemented; unverified)*

Implemented by reusing `SafeConditionalTokensRedemption`, which already exists
for Polymarket's Safe topology, rather than wrapping the SDK's `redeem()`. The
SDK version blocks up to 120 s inside `wait_for_transaction_receipt`, which would
stall the settlement loop, and it does not integrate with the engine's
`RedemptionReport` contract. The reused helper is async, uses the project's RPC
failover and nonce manager, and its `reconcile` already enforces the property
that matters: a confirmed receipt is not proof, so it re-reads the Safe's
Conditional Tokens balance and reports `UNKNOWN` while winnings remain claimable.

The one Opinion-specific piece is the identifier mapping. Settlement requests are
built from `MarketSpec`, which carries the numeric Opinion market id, while
Conditional Tokens is keyed by the 32-byte condition id. `_resolved_settlement_request`
fetches that mapping from the venue and substitutes it; a missing, malformed or
non-numeric id fails closed rather than reaching the chain.

No Opinion exemption was added to `_automatic_redemption_status`: that path means
"redemption is not required for this venue", which is false here.

Remaining verification, once a market resolves:

1. `production verify` reports `automatic_redemption_support:Opinion` as a pass.
   Do **not** accept `settlement_status:Opinion` as corroboration — it reported
   green against the old stub too.
2. A real resolved market yields `RESOLVED` from the on-chain payout vectors, and
   a genuinely void market yields `VOID`.
3. A redemption submits, confirms, and leaves zero Safe exposure — and a
   deliberately re-run redemption reports `UNKNOWN` rather than a false confirm.

### Phase 4 — shadow proof

Enable `routes.*_opinion`, keep `funded_routes.*_opinion` false. Deploy with
`DEPLOY_HEALTH_POLICY=safe_paused_shadow_bootstrap`, both services in `shadow`,
`LIVE_TRADING_CONFIRM=NO`.

Then mapping coverage, previewing before applying:

```bash
arbitrage-admin --config config.production.quote_arb.json discovery overlap --persist-candidates
arbitrage-admin --config config.production.quote_arb.json mappings review --operator <name>
arbitrage-admin --config config.production.quote_arb.json mappings approve-safe-candidates --operator <name> --route polymarket_opinion
```

**Expect `predict_opinion` and `sx_opinion` coverage to be thin or empty.** They
are synthesized from two Polymarket-anchored families, so they exist only where
Polymarket, Opinion, and Predict/SX all carry the same canonical market with
complementary outcomes. Zero coverage here is a finding, not a failure — it means
those routes are not yet worth funding.

### Phase 5 — calibration

```bash
CI_VERIFIED_COMMIT_SHA=<sha> CALIBRATION_REQUIRE_CONFIGURED_RESERVE=NO ./ops/production_closeout.sh
```

Failure at the pre-live audit for missing reserves is expected on the first run.
Take the p95 from `shadow-calibration-quote_arb.json`, set
`adverse_move_p95_pct_by_route` **in a new tracked commit**, never with
`--write-config` on the VM. Re-run with the reserve check enabled; every funded
route needs ≥10 000 valid evaluations in the 3600 s window.

### Phase 6 — funded canary, one route at a time

Start with `polymarket_opinion`: Polymarket is the deepest liquidity anchor and
so the likeliest to produce a natural fill inside the window.

Per route:

1. Add the route to `funded_routes` **and** to
   `QUOTE_ARB_EXPECTED_FUNDED_ROUTES` in `ops/production_closeout.sh`, in one
   tracked commit. Both must agree or the wrapper aborts.
2. Re-check exposure limits. Each added funded route raises simultaneous
   exposure; confirm `max_total_notional_usd`, `max_venue_exposure_usd`, and
   `max_open_positions` still bound the larger set.
3. Confirm `full_capacity_funding_readiness.ready === true` in
   `all-market-readiness.json`.
4. Operator sign-off, credential rotation, balance checks.
5. ```bash
   CREDENTIAL_ROTATION_CONFIRMED=YES ENABLE_FUNDED_CANARY=YES \
   FUNDED_CANARY_TARGET=quote_arb CI_VERIFIED_COMMIT_SHA=<sha> \
   ./ops/production_closeout.sh
   ```
   Do not pass `DURATION_SECONDS` or `CALIBRATION_DURATION_SECONDS`.

Only after a clean window may the next route be promoted.

## Acceptance gates

- Control routes keep a clean baseline throughout.
- `production-audit-final.json`: `passed: true`, `audit_scope: "post_window_paused"`,
  and `live_canary_evidence:<route>` resolving to `real_order_evidence` or
  `safe_no_trade` for every funded route.
- `/health/live=200`, `/health/ready=200`, `arbitrage_ready=1`,
  `arbitrage_risk_paused=0` outside the deliberate closeout pauses.
- Durable pause reason `funded_canary_window_complete`;
  `SUMMARY.txt` → `post_window_state=paused_shadow_clean`.
- No `UNKNOWN` intent, no reconciliation drift, no false `LOW VENUE BALANCE`, no
  reconnect storm, no `ERROR`/`CRITICAL`/`Traceback` noise.

## Escape valve

If a route stays healthy but no natural opportunity appears for 60 minutes, mark
it `unexercised` and leave the goal open. Do not force a trade and do not close
the goal on synthetic evidence.
