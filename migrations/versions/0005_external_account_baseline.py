"""Persist exact operator-approved external account baselines.

Revision ID: 0005_external_account_baseline
Revises: 0004_mapping_last_seen
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_external_account_baseline"
down_revision = "0004_mapping_last_seen"
branch_labels = None
depends_on = None

MONEY = sa.Numeric(38, 18)


def upgrade() -> None:
    op.add_column(
        "reconciliation_runs",
        sa.Column(
            "runtime_instance_id",
            sa.String(128),
            nullable=False,
            server_default="legacy",
        ),
    )
    op.add_column(
        "reconciliation_runs",
        sa.Column("full", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "reconciliation_runs",
        sa.Column("account_fingerprint", sa.String(64)),
    )
    op.add_column(
        "reconciliation_runs",
        sa.Column("external_baseline_manifest_sha256", sa.String(64)),
    )
    op.create_index(
        "ix_reconciliation_runs_runtime_instance_id",
        "reconciliation_runs",
        ["runtime_instance_id"],
    )
    op.create_table(
        "external_account_baselines",
        sa.Column("manifest_sha256", sa.String(64), primary_key=True),
        sa.Column("runtime_instance_id", sa.String(128), nullable=False),
        sa.Column("venue", sa.String(32), nullable=False),
        sa.Column("account_fingerprint", sa.String(64), nullable=False),
        sa.Column("operator", sa.String(128), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_by", sa.String(128)),
    )
    op.create_index(
        "ix_external_account_baselines_runtime_instance_id",
        "external_account_baselines",
        ["runtime_instance_id"],
    )
    op.create_index(
        "ix_external_account_baselines_venue",
        "external_account_baselines",
        ["venue"],
    )
    op.create_index(
        "uq_external_account_baselines_active_scope",
        "external_account_baselines",
        ["runtime_instance_id", "venue"],
        unique=True,
        postgresql_where=sa.text("revoked_at IS NULL"),
        sqlite_where=sa.text("revoked_at IS NULL"),
    )
    op.create_table(
        "external_account_baseline_items",
        sa.Column(
            "manifest_sha256",
            sa.String(64),
            sa.ForeignKey("external_account_baselines.manifest_sha256", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("item_type", sa.String(16), primary_key=True),
        sa.Column("item_key", sa.String(256), primary_key=True),
        sa.Column("quantity", MONEY),
    )


def downgrade() -> None:
    op.drop_table("external_account_baseline_items")
    op.drop_index(
        "uq_external_account_baselines_active_scope",
        table_name="external_account_baselines",
    )
    op.drop_index(
        "ix_external_account_baselines_venue",
        table_name="external_account_baselines",
    )
    op.drop_index(
        "ix_external_account_baselines_runtime_instance_id",
        table_name="external_account_baselines",
    )
    op.drop_table("external_account_baselines")
    op.drop_index(
        "ix_reconciliation_runs_runtime_instance_id",
        table_name="reconciliation_runs",
    )
    op.drop_column("reconciliation_runs", "external_baseline_manifest_sha256")
    op.drop_column("reconciliation_runs", "account_fingerprint")
    op.drop_column("reconciliation_runs", "full")
    op.drop_column("reconciliation_runs", "runtime_instance_id")
