"""observability logs and explainable matcher fields

Revision ID: 20260514_0002
Revises: 20260514_0001
Create Date: 2026-05-14
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260514_0002"
down_revision = "20260514_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("office_impacts", sa.Column("match_explanation", sa.JSON(), nullable=True))

    op.create_table(
        "event_logs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column("trace_id", sa.Uuid(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_event_logs_created_at", "event_logs", ["created_at"])
    op.create_index("ix_event_logs_event_type", "event_logs", ["event_type"])
    op.create_index("ix_event_logs_trace_id", "event_logs", ["trace_id"])

    op.create_table(
        "queue_depth_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pending", sa.Integer(), nullable=False),
        sa.Column("running", sa.Integer(), nullable=False),
        sa.Column("failed", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_queue_depth_snapshots_created_at",
        "queue_depth_snapshots",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_queue_depth_snapshots_created_at", table_name="queue_depth_snapshots")
    op.drop_table("queue_depth_snapshots")

    op.drop_index("ix_event_logs_trace_id", table_name="event_logs")
    op.drop_index("ix_event_logs_event_type", table_name="event_logs")
    op.drop_index("ix_event_logs_created_at", table_name="event_logs")
    op.drop_table("event_logs")

    op.drop_column("office_impacts", "match_explanation")
