"""Add durable Conditional Tokens redemption intents.

Revision ID: 0002_redemption_intents
Revises: 0001_production_state
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_redemption_intents"
down_revision = "0001_production_state"
branch_labels = None
depends_on = None

MONEY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.create_table(
        "redemption_intents",
        sa.Column("redemption_id", sa.String(36), primary_key=True),
        sa.Column("position_key", sa.String(768), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("market_id", sa.String(256), nullable=False),
        sa.Column("condition_id", sa.String(256), nullable=False),
        sa.Column("collateral_token", sa.String(128), nullable=False),
        sa.Column("expected_contracts", MONEY, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("tx_hash", sa.String(256)),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    for column in ("position_key", "venue", "status", "tx_hash"):
        op.create_index(f"ix_redemption_intents_{column}", "redemption_intents", [column])
    op.create_index(
        "uq_redemption_position_venue_condition",
        "redemption_intents",
        ["position_key", "venue", "condition_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("redemption_intents")
