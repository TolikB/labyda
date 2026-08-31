"""Persist explicit discovery observation evidence for mapping approval.

Revision ID: 0004_mapping_last_seen
Revises: 0003_mapping_match_strategy
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_mapping_last_seen"
down_revision = "0003_mapping_match_strategy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "market_mappings",
        sa.Column("last_discovered_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("market_mappings", "last_discovered_at")
