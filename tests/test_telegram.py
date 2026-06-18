import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import TelegramConfig
from arbitrage_engine.telegram import TelegramNotifier


class TelegramLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_is_reused_and_closed(self) -> None:
        notifier = TelegramNotifier(TelegramConfig("token", "chat"))
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()

        with patch("arbitrage_engine.telegram.client_session", return_value=session) as factory:
            self.assertIs(notifier._get_rest_session(), session)
            self.assertIs(notifier._get_rest_session(), session)
            await notifier.close()

        factory.assert_called_once()
        session.close.assert_awaited_once()
        self.assertIsNone(notifier._rest_session)


if __name__ == "__main__":
    unittest.main()
