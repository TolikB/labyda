"""Send one operator message to Telegram from an ops script.

The engine already announces its own risk pauses. This is for the things it
cannot say because they happen outside it: the continuous wrapper starting a
run, waiting out a recoverable pause, or stopping for good. Without those, an
unattended run is silent in exactly the situations somebody would want to know
about.

Notification is never load-bearing. A Telegram outage must not take down a run
that is otherwise healthy, so this exits 0 even when delivery fails and says so
on stderr. Callers should not check its status.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.telegram import TelegramNotifier


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send one HTML Telegram message to the operator chat")
    parser.add_argument("--config", required=True)
    parser.add_argument("--text", required=True, help="Message body; Telegram HTML subset is allowed.")
    return parser


async def _send(config_path: str, text: str) -> None:
    load_operator_env(config_path)
    config = load_config(config_path)
    await TelegramNotifier(config.telegram).send_html(text)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        asyncio.run(_send(args.config, args.text))
    except Exception as exc:  # noqa: BLE001 - delivery is best effort by design
        print(f"operator notification failed: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
