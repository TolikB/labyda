import json
import logging
import unittest

from arbitrage_engine.logging_config import JsonFormatter


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


if __name__ == "__main__":
    unittest.main()
