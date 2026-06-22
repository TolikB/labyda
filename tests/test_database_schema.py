import runpy
import unittest
from pathlib import Path
from typing import Any

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
}

INITIAL_FINANCIAL_COLUMNS = {key: value for key, value in FINANCIAL_COLUMNS.items() if key != "redemption_intents"}


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
