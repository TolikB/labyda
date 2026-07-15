"""Persist discovery match provenance for mapping approval.

Revision ID: 0003_mapping_match_strategy
Revises: 0002_redemption_intents
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_mapping_match_strategy"
down_revision = "0002_redemption_intents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("market_mappings", sa.Column("match_strategy", sa.String(24), nullable=True))


def downgrade() -> None:
    op.drop_column("market_mappings", "match_strategy")
