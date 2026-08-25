from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, is_dataclass
from typing import Any

from eth_account import Account
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId
from predict_sdk.errors import InvalidSignerError
from predict_sdk.order_builder import OrderBuilder
from predict_sdk.types import ApprovalCheck, ApprovalRunReport, ApprovalScope, OrderBuilderOptions

from arbitrage_engine.config import load_config, load_operator_env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preview or apply Predict.fun trading approvals for an EOA or Predict Account"
    )
    parser.add_argument("--config", default="config.production.quote_arb.json")
    parser.add_argument("--scope", choices=("trade", "all"), default="trade")
    parser.add_argument("--yield-bearing", choices=("both", "standard", "yield"), default="both")
    parser.add_argument("--apply", action="store_true")
    return parser


def _sdk_chain_id(chain_id: int) -> ChainId:
    if chain_id == 56:
        return ChainId.BNB_MAINNET
    if chain_id == 97:
        return ChainId.BNB_TESTNET
    raise SystemExit("Predict.fun approvals support only BNB mainnet (56) or BNB testnet (97)")


def _yield_bearing_selector(value: str) -> bool | None:
    if value == "both":
        return None
    if value == "yield":
        return True
    return False


def _dedupe_steps(steps: list[Any]) -> list[Any]:
    seen: set[str] = set()
    ordered: list[Any] = []
    for step in steps:
        step_id = str(getattr(step, "id", ""))
        if not step_id or step_id in seen:
            continue
        seen.add(step_id)
        ordered.append(step)
    return ordered


def _select_approval_steps(builder: Any, *, scope: str, yield_bearing: str) -> list[Any]:
    selector = _yield_bearing_selector(yield_bearing)
    if scope == "all":
        return _dedupe_steps(builder.get_all_approval_steps(is_yield_bearing=selector))
    tracks = [False, True] if selector is None else [selector]
    steps: list[Any] = []
    for is_yield_bearing in tracks:
        steps.extend(
            builder.get_approval_steps(
                ApprovalScope(operation="TRADE", is_neg_risk=False, is_yield_bearing=is_yield_bearing)
            )
        )
        steps.extend(
            builder.get_approval_steps(
                ApprovalScope(operation="TRADE", is_neg_risk=True, is_yield_bearing=is_yield_bearing)
            )
        )
    return _dedupe_steps(steps)


def _serialize_checks(checks: list[ApprovalCheck]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for check in checks:
        row = _to_plain_object(check.step)
        row["satisfied"] = bool(check.satisfied)
        rows.append(row)
    return rows


def _serialize_run_report(report: ApprovalRunReport | None) -> dict[str, Any] | None:
    if report is None:
        return None
    return {
        "success": bool(report.success),
        "steps": [
            {
                "step": _to_plain_object(item.step),
                "status": item.status,
                "transaction": _to_plain_object(item.transaction) if item.transaction is not None else None,
            }
            for item in report.steps
        ],
    }


def _to_plain_object(value: Any) -> dict[str, Any]:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if hasattr(value, "__dict__"):
        return {key: getattr(value, key) for key in vars(value)}
    raise TypeError(f"Unsupported object for serialization: {value!r}")


def _build_order_builder(app_config: Any) -> Any:
    private_key = app_config.predict_fun.private_key
    if not private_key:
        raise SystemExit("PREDICT_FUN_PRIVATE_KEY is required")
    try:
        return OrderBuilder.make(
            _sdk_chain_id(app_config.predict_fun.chain_id),
            signer=private_key,
            options=OrderBuilderOptions(
                precision=app_config.predict_fun.precision,
                predict_account=app_config.predict_fun.account_address,
                log_level="INFO",
            ),
        )
    except InvalidSignerError as exc:
        raise SystemExit(
            "Predict.fun signer private key does not own the configured predict_fun.account_address"
        ) from exc


def _report(
    *,
    app_config: Any,
    args: argparse.Namespace,
    builder: Any,
    checks: list[ApprovalCheck],
    run_report: ApprovalRunReport | None,
) -> dict[str, Any]:
    signer_address = Account.from_key(app_config.predict_fun.private_key).address
    approval_owner = app_config.predict_fun.account_address or signer_address
    chain = _sdk_chain_id(app_config.predict_fun.chain_id)
    addresses = asdict(ADDRESSES_BY_CHAIN_ID[chain])
    missing = [check for check in checks if not check.satisfied]
    apply_confirmed = bool(args.apply and os.getenv("PREDICT_FUN_APPROVE_CONFIRM") == "YES")
    return {
        "scope": args.scope,
        "yield_bearing": args.yield_bearing,
        "apply_requested": bool(args.apply),
        "apply_confirmed": apply_confirmed,
        "submitted": run_report is not None,
        "success": bool(run_report.success) if run_report is not None else not missing,
        "network": app_config.predict_fun.network,
        "chain_id": app_config.predict_fun.chain_id,
        "api_base_url": app_config.predict_fun.api_base_url,
        "signer_address": signer_address,
        "predict_account_address": app_config.predict_fun.account_address,
        "approval_owner_address": approval_owner,
        "mode": "predict_account" if app_config.predict_fun.account_address else "eoa",
        "contracts": addresses,
        "steps": _serialize_checks(checks),
        "step_count": len(checks),
        "missing_step_count": len(missing),
        "missing_step_ids": [check.step.id for check in missing],
        "run_report": _serialize_run_report(run_report),
        "confirm_hint": (
            "Set PREDICT_FUN_APPROVE_CONFIRM=YES and pass --apply to submit the missing approval transactions."
            if args.apply and run_report is None
            else None
        ),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    load_operator_env(args.config)
    app_config = load_config(args.config)
    builder = _build_order_builder(app_config)
    steps = _select_approval_steps(builder, scope=args.scope, yield_bearing=args.yield_bearing)
    checks = builder.check_approvals(steps)
    missing_steps = [check.step for check in checks if not check.satisfied]
    run_report: ApprovalRunReport | None = None
    if args.apply and os.getenv("PREDICT_FUN_APPROVE_CONFIRM") == "YES" and missing_steps:
        run_report = builder.run_approvals(missing_steps, skip_satisfied=False, stop_on_error=True)
        checks = builder.check_approvals(steps)
    report = _report(app_config=app_config, args=args, builder=builder, checks=checks, run_report=run_report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
