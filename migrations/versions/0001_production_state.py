"""Create production trading state schema.

Revision ID: 0001_production_state
Revises:
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_production_state"
down_revision = None
branch_labels = None
depends_on = None

MONEY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.create_table(
        "canonical_markets",
        sa.Column("canonical_id", sa.String(128), primary_key=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("resolution_source", sa.Text(), nullable=False),
        sa.Column("cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timezone_name", sa.String(64), nullable=False),
        sa.Column("outcome_semantics", sa.Text(), nullable=False),
        sa.Column("rules_fingerprint", sa.String(64), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_canonical_markets_category", "canonical_markets", ["category"])

    op.create_table(
        "venue_instruments",
        sa.Column("instrument_id", sa.String(128), primary_key=True),
        sa.Column("canonical_id", sa.String(128), sa.ForeignKey("canonical_markets.canonical_id")),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("market_id", sa.String(256), nullable=False),
        sa.Column("yes_token_id", sa.Text(), nullable=False),
        sa.Column("no_token_id", sa.Text(), nullable=False),
        sa.Column("closes_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolution_source", sa.Text()),
        sa.Column("rules_fingerprint", sa.String(64)),
        sa.Column("category", sa.String(64)),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_venue_instruments_venue", "venue_instruments", ["venue"])
    op.create_index("uq_venue_market", "venue_instruments", ["venue", "market_id"], unique=True)

    op.create_table(
        "market_mappings",
        sa.Column("mapping_id", sa.String(64), primary_key=True),
        sa.Column(
            "canonical_market_id",
            sa.String(128),
            sa.ForeignKey("canonical_markets.canonical_id"),
            nullable=False,
        ),
        sa.Column("left_venue", sa.String(32), nullable=False),
        sa.Column("left_market_id", sa.String(256), nullable=False),
        sa.Column("right_venue", sa.String(32), nullable=False),
        sa.Column("right_market_id", sa.String(256), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("rules_fingerprint", sa.String(64), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("verified_by", sa.String(128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_market_mappings_status", "market_mappings", ["status"])
    op.create_index(
        "uq_market_mapping_pair",
        "market_mappings",
        ["left_venue", "left_market_id", "right_venue", "right_market_id"],
        unique=True,
    )

    op.create_table(
        "order_intents",
        sa.Column("client_order_id", sa.String(36), primary_key=True),
        sa.Column("route", sa.String(64), nullable=False),
        sa.Column("market_key", sa.String(512), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.Column("binary_side", sa.String(8), nullable=False),
        sa.Column("action", sa.String(8), nullable=False),
        sa.Column("quantity", MONEY, nullable=False),
        sa.Column("limit_price", MONEY, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("venue_order_id", sa.String(256)),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    for column in ("route", "market_key", "venue", "status", "venue_order_id"):
        op.create_index(f"ix_order_intents_{column}", "order_intents", [column])

    op.create_table(
        "venue_orders",
        sa.Column("venue_order_id", sa.String(256), primary_key=True),
        sa.Column(
            "client_order_id",
            sa.String(36),
            sa.ForeignKey("order_intents.client_order_id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("quantity", MONEY, nullable=False),
        sa.Column("cumulative_filled", MONEY, nullable=False),
        sa.Column("average_price", MONEY, nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_venue_orders_venue", "venue_orders", ["venue"])
    op.create_index("ix_venue_orders_status", "venue_orders", ["status"])

    op.create_table(
        "fills",
        sa.Column("fill_id", sa.String(256), primary_key=True),
        sa.Column(
            "client_order_id",
            sa.String(36),
            sa.ForeignKey("order_intents.client_order_id"),
            nullable=False,
        ),
        sa.Column("venue_order_id", sa.String(256), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("quantity", MONEY, nullable=False),
        sa.Column("price", MONEY, nullable=False),
        sa.Column("fee", MONEY, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    )
    for column in ("client_order_id", "venue_order_id", "venue", "occurred_at"):
        op.create_index(f"ix_fills_{column}", "fills", [column])

    op.create_table(
        "positions",
        sa.Column("position_key", sa.String(768), primary_key=True),
        sa.Column("symbol", sa.String(512), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("first_venue", sa.String(32), nullable=False),
        sa.Column("second_venue", sa.String(32), nullable=False),
        sa.Column("first_quantity", MONEY, nullable=False),
        sa.Column("second_quantity", MONEY, nullable=False),
        sa.Column("first_entry_price", MONEY, nullable=False),
        sa.Column("second_entry_price", MONEY, nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_positions_symbol", "positions", ["symbol"])
    op.create_index("ix_positions_status", "positions", ["status"])

    op.create_table(
        "risk_state",
        sa.Column("state_id", sa.String(32), primary_key=True),
        sa.Column("loss_day", sa.String(10), nullable=False),
        sa.Column("daily_loss_usd", MONEY, nullable=False),
        sa.Column("consecutive_api_errors", sa.Integer(), nullable=False),
        sa.Column("paused", sa.Boolean(), nullable=False),
        sa.Column("pause_reason", sa.Text()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "balance_snapshots",
        sa.Column("snapshot_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(64), nullable=False),
        sa.Column("balance", MONEY, nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_balance_snapshots_venue", "balance_snapshots", ["venue"])
    op.create_index("ix_balance_snapshots_captured_at", "balance_snapshots", ["captured_at"])

    op.create_table(
        "reconciliation_runs",
        sa.Column("run_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("orders_checked", sa.Integer(), nullable=False),
        sa.Column("fills_recorded", sa.Integer(), nullable=False),
        sa.Column("drift_count", sa.Integer(), nullable=False),
        sa.Column("success", sa.Boolean(), nullable=False),
        sa.Column("error", sa.Text()),
    )
    op.create_index("ix_reconciliation_runs_venue", "reconciliation_runs", ["venue"])

    op.create_table(
        "audit_events",
        sa.Column("event_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("correlation_id", sa.String(64)),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])
    op.create_index("ix_audit_events_correlation_id", "audit_events", ["correlation_id"])


def downgrade() -> None:
    for table in (
        "audit_events",
        "reconciliation_runs",
        "balance_snapshots",
        "risk_state",
        "positions",
        "fills",
        "venue_orders",
        "order_intents",
        "market_mappings",
        "venue_instruments",
        "canonical_markets",
    ):
        op.drop_table(table)
