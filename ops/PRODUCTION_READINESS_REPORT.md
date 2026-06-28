# Polymarket-Myriad production closeout report

Date: 2026-06-28

## Verdict

`GO` for repeatable Docker Compose shadow rollout on the live VM.

`NO-GO` for funded live trading. That still needs separate operator approval,
wallet authorization, and venue lifecycle smoke outside this closeout.

## Confirmed live deployment shape

- VM runtime is Docker Compose under `/home/tolik1992s/labyda_next`.
- The active service is `labyda_next-bot-1`.
- The standard rollout command is `./ops/deploy_compose.sh` from that checkout.
- The standard passive verification command is `./ops/shadow_smoke.sh`.

## Closeout evidence

- Local `master` was committed and pushed through the production fix set:
  - `92520e1` `Deduplicate duplicate Gamma market payloads`
  - `0e5c9a3` `Add compose deployment script`
  - `65c1868` `Mark compose deploy script executable`
- The live VM checkout now runs from a real git worktree instead of a file-sync overlay.
- `deploy_compose.sh` successfully fast-forwarded the live checkout, ran Alembic, rebuilt `bot`, and restored readiness.
- Final post-deploy checks on the VM returned:
  - `/health/live`: HTTP 200
  - `/health/ready`: HTTP 200 with `missing_routes=[]`
  - `arbitrage_ready=1.0`
  - `arbitrage_discovery_missing_routes=0`
  - low `arbitrage_market_data_age_seconds` for `Polymarket` and `Myriad`
  - `arbitrage_market_data_active_targets=46` for `Polymarket` and `Myriad`
- The final 10-minute shadow smoke passed with:
  - `40/40` successful `/health/live`
  - `40/40` successful `/health/ready`
  - no reconnect storm
  - no quiet-market false alerts
  - no snapshot-timeout churn
  - no error-class log lines in the audited window

## Why the rollout was previously noisy

- The live VM path was ambiguous between an old systemd assumption and the actual Docker Compose stack.
- The compose directory was not initially a git checkout, which made fast-forward deploys non-repeatable.
- Gamma bulk refresh could fail on duplicate market IDs even when a safe deduplicated snapshot was possible.

Those conditions have now been addressed in repo and on VM.

## Remaining follow-up goal

Keep driving the deployed `master` toward a durable production-closeout state where every repeat 10-minute shadow smoke remains clean without operator interpretation:

- Normalize runtime config so enabled routes and enabled venues stay aligned.
- Keep readiness free of disabled-route or disabled-venue pollution.
- Reduce or explicitly explain any future recurrent Myriad staleness or Polymarket snapshot-timeout noise.
- Preserve the Docker Compose deploy path as the authoritative VM workflow in docs and operations.
