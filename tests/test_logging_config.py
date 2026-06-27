import json
import logging
import os
import unittest

from arbitrage_engine.logging_config import JsonFormatter, configure_logging


class LoggingConfigTests(unittest.TestCase):
    def test_latency_metrics_are_emitted_as_json_fields(self) -> None:
        record = logging.LogRecord(
            name="arbitrage_engine.execution",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="execution_pipeline_latency",
            args=(),
            exc_info=None,
        )
        record._entry_submit_delta_us = 125.5
        record._first_exchange_ack_us = 900.0

        payload = json.loads(JsonFormatter().format(record))

        self.assertEqual(payload["message"], "execution_pipeline_latency")
        self.assertEqual(payload["entry_submit_delta_us"], 125.5)
        self.assertEqual(payload["first_exchange_ack_us"], 900.0)

    def test_database_password_and_bearer_token_are_redacted(self) -> None:
        record = logging.LogRecord(
            name="arbitrage_engine.database",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg=("postgresql+asyncpg://operator:super-secret@postgres/arbitrage Authorization: Bearer venue-token"),
            args=(),
            exc_info=None,
        )

        payload = json.loads(JsonFormatter().format(record))

        self.assertNotIn("super-secret", payload["message"])
        self.assertNotIn("venue-token", payload["message"])

    def test_public_condition_id_is_not_mislabeled_as_private_key(self) -> None:
        condition_id = "0x" + "ab" * 32
        record = logging.LogRecord(
            name="arbitrage_engine.market_discovery",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="condition discovered",
            args=(),
            exc_info=None,
        )
        record._condition_id = condition_id

        payload = json.loads(JsonFormatter().format(record))

        self.assertEqual(payload["condition_id"], condition_id)

    def test_labeled_private_key_and_env_secret_are_redacted(self) -> None:
        os.environ["TEST_PRIVATE_KEY"] = "0x" + "cd" * 32
        try:
            configure_logging()
            record = logging.LogRecord(
                name="arbitrage_engine.wallet",
                level=logging.ERROR,
                pathname=__file__,
                lineno=1,
                msg=f'private_key="{"0x" + "cd" * 32}"',
                args=(),
                exc_info=None,
            )

            payload = json.loads(JsonFormatter().format(record))
        finally:
            os.environ.pop("TEST_PRIVATE_KEY", None)

        self.assertNotIn("cd" * 32, payload["message"])


if __name__ == "__main__":
    unittest.main()
