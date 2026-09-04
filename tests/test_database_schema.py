import logging
import runpy
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import Column, Numeric

from arbitrage_engine.database import Base

FINANCIAL_COLUMNS = {
    "order_intents": {"quantity", "limit_price"},
    "venue_orders": {"quantity", "cumulative_filled", "average_price"},
    "fills": {"quantity", "price", "fee"},
    "positions": {"first_quantity", "second_quantity", "first_entry_price", "second_entry_price"},
    "risk_state": {"daily_loss_usd"},
    "balance_snapshots": {"balance"},
    "redemption_intents": {"expected_contracts"},
    "external_account_baseline_items": {"quantity"},
}

INITIAL_FINANCIAL_COLUMNS = {
    key: value
    for key, value in FINANCIAL_COLUMNS.items()
    if key not in {"redemption_intents", "external_account_baseline_items"}
}


def _assert_money_type(test: unittest.TestCase, column: Column[Any]) -> None:
    test.assertIsInstance(column.type, Numeric)
    assert isinstance(column.type, Numeric)
    test.assertEqual(column.type.precision, 38)
    test.assertEqual(column.type.scale, 18)


class _MigrationOperations:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, Column[Any]]] = {}

    def create_table(self, name: str, *elements: object, **_kwargs: object) -> None:
        self.tables[name] = {
            element.name: element for element in elements if isinstance(element, Column) and element.name is not None
        }

    def create_index(self, *_args: object, **_kwargs: object) -> None:
        return None

    def add_column(self, table_name: str, column: Column[Any]) -> None:
        if column.name is not None:
            self.tables.setdefault(table_name, {})[column.name] = column


class DatabaseMoneySchemaTests(unittest.TestCase):
    def test_orm_financial_columns_use_numeric_38_18(self) -> None:
        for table_name, column_names in FINANCIAL_COLUMNS.items():
            table = Base.metadata.tables[table_name]
            for column_name in column_names:
                with self.subTest(table=table_name, column=column_name):
                    _assert_money_type(self, table.columns[column_name])

    def test_initial_migration_financial_columns_use_numeric_38_18(self) -> None:
        migration_path = Path(__file__).parents[1] / "migrations" / "versions" / "0001_production_state.py"
        namespace = runpy.run_path(str(migration_path))
        operations = _MigrationOperations()
        namespace["upgrade"].__globals__["op"] = operations
        namespace["upgrade"]()

        for table_name, column_names in INITIAL_FINANCIAL_COLUMNS.items():
            for column_name in column_names:
                with self.subTest(table=table_name, column=column_name):
                    _assert_money_type(self, operations.tables[table_name][column_name])

    def test_redemption_migration_uses_numeric_38_18(self) -> None:
        migration_path = Path(__file__).parents[1] / "migrations" / "versions" / "0002_redemption_intents.py"
        namespace = runpy.run_path(str(migration_path))
        operations = _MigrationOperations()
        namespace["upgrade"].__globals__["op"] = operations
        namespace["upgrade"]()

        _assert_money_type(self, operations.tables["redemption_intents"]["expected_contracts"])

    def test_external_baseline_migration_uses_numeric_38_18(self) -> None:
        migration_path = Path(__file__).parents[1] / "migrations" / "versions" / "0005_external_account_baseline.py"
        namespace = runpy.run_path(str(migration_path))
        operations = _MigrationOperations()
        namespace["upgrade"].__globals__["op"] = operations
        namespace["upgrade"]()

        _assert_money_type(self, operations.tables["external_account_baseline_items"]["quantity"])

    def test_external_baseline_migration_supports_up_down_up_with_legacy_rows(self) -> None:
        root = Path(__file__).parents[1]
        application_logger = logging.getLogger("arbitrage_engine.execution")
        application_logger.disabled = False
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            database_path = Path(tmp) / "migration-cycle.sqlite3"
            alembic_config = Config(str(root / "alembic.ini"))
            alembic_config.set_main_option("script_location", str(root / "migrations"))
            alembic_config.set_main_option(
                "sqlalchemy.url",
                f"sqlite+aiosqlite:///{database_path.as_posix()}",
            )
            command.upgrade(alembic_config, "0004_mapping_last_seen")
            with sqlite3.connect(database_path) as connection:
                connection.execute(
                    """
                    INSERT INTO reconciliation_runs (
                        venue, started_at, completed_at, orders_checked,
                        fills_recorded, drift_count, success, error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("Polymarket", "2026-09-04T00:00:00Z", "2026-09-04T00:00:01Z", 1, 0, 0, 1, None),
                )
                connection.commit()

            command.upgrade(alembic_config, "head")
            with sqlite3.connect(database_path) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(reconciliation_runs)")
                }
                self.assertTrue(
                    {
                        "runtime_instance_id",
                        "full",
                        "account_fingerprint",
                        "external_baseline_manifest_sha256",
                    }.issubset(columns)
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT runtime_instance_id, full FROM reconciliation_runs"
                    ).fetchone(),
                    ("legacy", 1),
                )
                self.assertIsNotNone(
                    connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='external_account_baselines'"
                    ).fetchone()
                )

            command.downgrade(alembic_config, "0004_mapping_last_seen")
            with sqlite3.connect(database_path) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(reconciliation_runs)")
                }
                self.assertNotIn("runtime_instance_id", columns)
                self.assertIsNone(
                    connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='external_account_baselines'"
                    ).fetchone()
                )

            command.upgrade(alembic_config, "head")
            with sqlite3.connect(database_path) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(reconciliation_runs)")
                }
                self.assertIn("external_baseline_manifest_sha256", columns)
        self.assertFalse(application_logger.disabled)
