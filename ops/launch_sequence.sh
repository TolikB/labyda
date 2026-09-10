#!/usr/bin/env bash
# Drive the funded-launch sequence: calibrate, land reserves, verify, arm.
#
# The four phases are the same ones the runbook describes; what this adds is
# that it remembers where it got to. Each shadow phase takes about two hours,
# and an SSH drop in the middle of one used to mean starting over -- so a phase
# whose evidence already exists for the current release is skipped rather than
# repeated, and a run already in flight is waited for rather than collided with.
#
# Phase 2 stops on purpose. Calibration produces the route reserves, and those
# go into the config through CI, not on the box: `production_closeout.sh`
# refuses to build on a dirty tracked worktree, and that refusal is the whole
# reason a config edit cannot be smuggled onto a running release. The phase
# prints the exact JSON to commit and stops there.
#
# Phase 4 starts real-money trading. It needs --confirm-live-trading, in the
# same spirit as CONTINUOUS_TRADING_CONFIRMED and every other flag in this
# system that costs money: nothing here spends anything by default.
set -Eeuo pipefail

REPO_DIR=${REPO_DIR:-/opt/labyda_next}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-${REPO_DIR}/closeout-artifacts}
CONTINUOUS_ENV_FILE=${CONTINUOUS_ENV_FILE:-/etc/labyda/continuous.env}
CONTINUOUS_SERVICE=${CONTINUOUS_SERVICE:-labyda-continuous.service}
FUNDED_TARGET=${FUNDED_TARGET:-quote_arb}
LOCK_WAIT_SECONDS=${LOCK_WAIT_SECONDS:-14400}
LOG_DIR=${LOG_DIR:-${REPO_DIR}/.runtime/launch-sequence}

PHASES=(calibrate reserves verify arm)
# A phase returns 0 to continue, HALT to stop the sequence cleanly because a
# person has to act, and anything else to fail.
HALT=90
start_phase=calibrate
only_phase=""
confirm_live_trading=NO
status_only=NO

usage() {
  cat <<'USAGE'
usage: ops/launch_sequence.sh [options]

  --status                 report where the sequence stands and exit
  --from PHASE             start at PHASE (default: calibrate)
  --only PHASE             run exactly one phase
  --confirm-live-trading   allow the `arm` phase to start real-money trading
  --help

phases:
  calibrate   shadow closeout: approve safe mappings, calibrate without the
              reserve check (~2h, spends nothing)
  reserves    read the calibration artifact and print the config change to
              land through CI, then stop
  verify      shadow closeout with the reserve check on, proving the committed
              reserves hold (~2h, spends nothing)
  arm         point the unit at the current release and start it
              -- THIS TRADES REAL MONEY, and needs --confirm-live-trading
USAGE
}

while (($#)); do
  case "$1" in
    --status) status_only=YES ;;
    --from) start_phase=${2:?--from needs a phase}; shift ;;
    --only) only_phase=${2:?--only needs a phase}; shift ;;
    --confirm-live-trading) confirm_live_trading=YES ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

for phase in "${start_phase}" ${only_phase:+"${only_phase}"}; do
  printf '%s\n' "${PHASES[@]}" | grep -qx "${phase}" || {
    echo "unknown phase: ${phase}" >&2
    exit 2
  }
done

cd "${REPO_DIR}"
test -f ops/production_closeout.sh || { echo "run this from the compose checkout" >&2; exit 1; }
mkdir -p "${LOG_DIR}"

release_sha=$(git rev-parse HEAD)
deployed_sha=$(cat .runtime/release-sha 2>/dev/null || echo "")

say() { printf '\n=== %s ===\n' "$*"; }

# `systemctl is-enabled` prints "disabled" *and* exits non-zero, so a plain
# `|| echo unknown` appends a second word to a perfectly good answer.
unit_state() {
  local value
  value=$(systemctl "$1" "${CONTINUOUS_SERVICE}" 2>/dev/null || true)
  printf '%s' "${value:-unknown}"
}

# The checkout, the running images and the unit must all name one release.
# Calibrating one release and arming another is the quiet way to end up trading
# code nobody verified.
require_consistent_release() {
  if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "tracked worktree is dirty; the closeout will refuse to run" >&2
    git status --porcelain --untracked-files=no >&2
    return 1
  fi
  if [[ "${deployed_sha}" != "${release_sha}" ]]; then
    echo "checkout is ${release_sha} but the deployed release is ${deployed_sha:-unknown}" >&2
    echo "deploy first: CI_VERIFIED_COMMIT_SHA=${release_sha} BRANCH=\$(git rev-parse --abbrev-ref HEAD) \\" >&2
    echo "  DEPLOY_HEALTH_POLICY=safe_paused_shadow_bootstrap ./ops/deploy_compose.sh" >&2
    return 1
  fi
  return 0
}

# A closeout already in flight is the normal case when this is re-run after an
# SSH drop. Waiting beats both colliding with it and starting over.
wait_for_closeout_to_finish() {
  local waited=0
  while pgrep -f '[p]roduction_closeout.sh' >/dev/null 2>&1; do
    if ((waited == 0)); then
      echo "a closeout run is already in flight; waiting for it to finish"
    fi
    ((waited += 30))
    if ((waited > LOCK_WAIT_SECONDS)); then
      echo "closeout still running after ${LOCK_WAIT_SECONDS}s; giving up" >&2
      return 1
    fi
    sleep 30
  done
  return 0
}

# Evidence for a completed shadow phase: this release, this reserve setting, and
# the wrapper's own success line. Anything less is not a phase that can be
# skipped.
completed_summary_for() {
  local reserve_required=$1
  local summary
  while IFS= read -r summary; do
    [[ -n "${summary}" ]] || continue
    grep -qx "result=shadow_calibration_and_preflight_complete" "${summary}" || continue
    grep -qx "ci_verified_commit_sha=${release_sha}" "${summary}" || continue
    grep -qx "calibration_require_configured_reserve=${reserve_required}" "${summary}" || continue
    printf '%s\n' "${summary}"
    return 0
  done < <(find "${ARTIFACT_ROOT}" -maxdepth 2 -name SUMMARY.txt -printf '%T@ %p\n' 2>/dev/null \
    | sort -rn | cut -d' ' -f2-)
  return 1
}

run_shadow_closeout() {
  local reserve_required=$1
  local log_file="${LOG_DIR}/$(date -u +%Y%m%dT%H%M%SZ)-reserve-${reserve_required}.log"
  echo "logging to ${log_file}"
  # Explicitly stripped: if this is ever run from a shell that sourced
  # /etc/labyda/continuous.env, ENABLE_FUNDED_CANARY=YES would turn a shadow
  # phase into a funded one without anything on screen saying so.
  env -u ENABLE_FUNDED_CANARY \
      -u CONTINUOUS_TRADING_CONFIRMED \
      -u FUNDED_CANARY_TARGET \
    CI_VERIFIED_COMMIT_SHA="${release_sha}" \
    AUTO_APPROVE_SAFE_MAPPINGS=YES \
    CALIBRATION_REQUIRE_CONFIGURED_RESERVE="${reserve_required}" \
    ./ops/production_closeout.sh 2>&1 | tee "${log_file}"
  return "${PIPESTATUS[0]}"
}

phase_calibrate() {
  say "phase 1/4: calibrate (reserve check off)"
  local summary
  if summary=$(completed_summary_for NO); then
    echo "already done for ${release_sha}: ${summary}"
    return 0
  fi
  require_consistent_release
  wait_for_closeout_to_finish
  if summary=$(completed_summary_for NO); then
    echo "the run that was in flight completed this phase: ${summary}"
    return 0
  fi
  run_shadow_closeout NO
}

phase_reserves() {
  say "phase 2/4: route reserves"
  local summary calibration
  summary=$(completed_summary_for NO) || {
    echo "no completed calibration for ${release_sha}; run the calibrate phase first" >&2
    return 1
  }
  calibration=$(dirname "${summary}")/${FUNDED_TARGET}/shadow-calibration-${FUNDED_TARGET}.json
  test -s "${calibration}" || { echo "calibration artifact missing: ${calibration}" >&2; return 1; }

  local status=0
  {
    python3 - "${calibration}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)

routes = report.get("routes") or {}
failed = {route: details for route, details in routes.items() if not details.get("passed")}

print(f"calibration artifact: {sys.argv[1]}")
print(f"window: {report.get('duration_seconds')}s, minimum valid evaluations: "
      f"{report.get('minimum_valid_evaluations')}")
print("")
for route, details in sorted(routes.items()):
    print(f"  {route:<24} p95={details.get('adverse_move_p95_pct')} "
          f"valid={details.get('valid_evaluation_count')} "
          f"{'ok' if details.get('passed') else 'BLOCKED: ' + ','.join(details.get('blockers') or [])}")

if failed:
    print("")
    print("Some routes did not calibrate. Committing a partial reserve set would leave")
    print("them funded without one, so fix these before landing anything:")
    for route, details in sorted(failed.items()):
        print(f"  {route}: {', '.join(details.get('blockers') or [])}")
    raise SystemExit(1)

block = {route: details["adverse_move_p95_pct"] for route, details in sorted(routes.items())}
print("")
print("Land this in config.production.quote_arb.json under \"spread_policy\":")
print("")
print('  "adverse_move_p95_pct_by_route": ' + json.dumps(block, indent=4).replace("\n", "\n  "))
PY
  } || status=$?
  ((status == 0)) || return "${status}"

  cat <<EOF

This is where the sequence stops on purpose.

The reserves have to reach the release through CI -- commit, review, merge, then
./ops/deploy_compose.sh. Editing the config on this box would leave the tracked
worktree dirty, and the closeout refuses to run on a dirty tree. That refusal is
the reason a config change cannot be smuggled onto a running release, so this
script will not work around it.

After the change is deployed, continue with:

  ./ops/launch_sequence.sh --from verify

EOF
  return "${HALT}"
}

phase_verify() {
  say "phase 3/4: verify the committed reserves"
  local summary
  if summary=$(completed_summary_for YES); then
    echo "already done for ${release_sha}: ${summary}"
    return 0
  fi
  require_consistent_release
  # A reserve check against an empty config passes vacuously, so make sure the
  # values actually landed before spending two hours proving nothing.
  local configured
  configured=$(python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    policy = json.load(handle).get("spread_policy") or {}
print(len(policy.get("adverse_move_p95_pct_by_route") or {}))
' "config.production.${FUNDED_TARGET}.json")
  if [[ "${configured}" == "0" ]]; then
    echo "config has no adverse_move_p95_pct_by_route; land the reserves phase first" >&2
    return 1
  fi
  echo "configured route reserves: ${configured}"
  wait_for_closeout_to_finish
  run_shadow_closeout YES
}

phase_arm() {
  say "phase 4/4: arm continuous funded trading"
  local summary
  summary=$(completed_summary_for YES) || {
    echo "no verified calibration for ${release_sha}; run the verify phase first" >&2
    return 1
  }
  require_consistent_release

  if [[ "${confirm_live_trading}" != "YES" ]]; then
    cat <<EOF
Everything is ready. The next step trades real money, so it is not taken by
default.

  release:        ${release_sha}
  evidence:       ${summary}
unit:                $(unit_state is-enabled) / $(unit_state is-active)
  window:         14400s, repeated until stopped
  daily loss cap: \$10, waits out the day and continues

Re-run with --confirm-live-trading to start it, or start it yourself:

  sudo systemctl enable --now ${CONTINUOUS_SERVICE}

EOF
    return "${HALT}"
  fi

  test -r "${CONTINUOUS_ENV_FILE}" || { echo "missing ${CONTINUOUS_ENV_FILE}" >&2; return 1; }
  # The unit trades whatever SHA this file names, so it is the last place a
  # stale release can hide.
  sed -i "s/^CI_VERIFIED_COMMIT_SHA=.*/CI_VERIFIED_COMMIT_SHA=${release_sha}/" "${CONTINUOUS_ENV_FILE}"
  grep -q "^CI_VERIFIED_COMMIT_SHA=${release_sha}$" "${CONTINUOUS_ENV_FILE}" || {
    echo "could not set the release SHA in ${CONTINUOUS_ENV_FILE}" >&2
    return 1
  }
  echo "armed ${CONTINUOUS_ENV_FILE} at ${release_sha}"
  systemctl enable --now "${CONTINUOUS_SERVICE}"
  sleep 5
  systemctl status "${CONTINUOUS_SERVICE}" --no-pager -l | head -n 15
  cat <<EOF

Started. Telegram should carry a "Continuous funded trading started" message --
that is the end-to-end proof the alert path works.

  follow:      journalctl -u ${CONTINUOUS_SERVICE} -f
  stop gently: touch ${REPO_DIR}/.runtime/canary-control/stop
  stop now:    sudo systemctl stop ${CONTINUOUS_SERVICE}
EOF
}

report_status() {
  local calibrated verified
  calibrated=$(completed_summary_for NO || echo "-")
  verified=$(completed_summary_for YES || echo "-")
  cat <<EOF
release (checkout):  ${release_sha}
release (deployed):  ${deployed_sha:-unknown}
tracked worktree:    $(git status --porcelain --untracked-files=no | wc -l) dirty file(s)
closeout in flight:  $(pgrep -f '[p]roduction_closeout.sh' >/dev/null 2>&1 && echo yes || echo no)
unit:                $(unit_state is-enabled) / $(unit_state is-active)

phase 1 calibrate:   ${calibrated}
phase 3 verify:      ${verified}
EOF
}

if [[ "${status_only}" == "YES" ]]; then
  report_status
  exit 0
fi

run_phase() {
  local status=0
  "phase_$1" || status=$?
  if ((status == HALT)); then
    exit 0
  fi
  return "${status}"
}

if [[ -n "${only_phase}" ]]; then
  run_phase "${only_phase}"
  exit $?
fi

started=NO
for phase in "${PHASES[@]}"; do
  [[ "${started}" == "YES" || "${phase}" == "${start_phase}" ]] && started=YES
  [[ "${started}" == "YES" ]] || continue
  run_phase "${phase}"
done
say "sequence complete"
