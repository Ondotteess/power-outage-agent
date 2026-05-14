"""initial durable pipeline schema

Revision ID: 20260514_0001
Revises:
Create Date: 2026-05-14
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260514_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(length=50), nullable=False),
        sa.Column("poll_interval_seconds", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("parser_profile", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_type", "url", name="uq_sources_type_url"),
    )

    op.create_table(
        "raw_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(length=50), nullable=False),
        sa.Column("raw_content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_raw_records_content_hash"), "raw_records", ["content_hash"], unique=True)

    op.create_table(
        "parsed_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("raw_record_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=True),
        sa.Column("external_id", sa.String(length=64), nullable=True),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("location_city", sa.String(length=255), nullable=True),
        sa.Column("location_district", sa.String(length=255), nullable=True),
        sa.Column("location_street", sa.Text(), nullable=True),
        sa.Column("location_region_code", sa.String(length=128), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("extracted_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["raw_record_id"], ["raw_records.id"]),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("fingerprint", name="uq_parsed_records_fingerprint"),
    )
    op.create_index("ix_parsed_records_raw_record_id", "parsed_records", ["raw_record_id"])
    op.create_index("ix_parsed_records_source_external", "parsed_records", ["source_id", "external_id"])
    op.create_index(op.f("ix_parsed_records_fingerprint"), "parsed_records", ["fingerprint"])

    op.create_table(
        "normalized_events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("parsed_record_id", sa.Uuid(), nullable=True),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("location_raw", sa.Text(), nullable=False),
        sa.Column("location_normalized", sa.Text(), nullable=True),
        sa.Column("location_city", sa.String(length=255), nullable=True),
        sa.Column("location_street", sa.Text(), nullable=True),
        sa.Column("location_building", sa.String(length=128), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("sources", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("normalized_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["parsed_record_id"], ["parsed_records.id"]),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint("parsed_record_id", name="uq_normalized_events_parsed_record_id"),
        sa.UniqueConstraint(
            "event_type",
            "location_normalized",
            "start_time",
            "end_time",
            name="uq_normalized_events_exact_window",
        ),
    )
    op.create_index("ix_normalized_events_address_time", "normalized_events", ["location_normalized", "start_time", "end_time"])
    op.create_index("ix_normalized_events_parsed_record_id", "normalized_events", ["parsed_record_id"])

    op.create_table(
        "offices",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("city", sa.String(length=255), nullable=False),
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column("region", sa.String(length=64), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "city", "address", name="uq_offices_name_city_address"),
    )
    op.create_index("ix_offices_city_address", "offices", ["city", "address"])

    op.create_table(
        "dedup_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("incoming_event_id", sa.Uuid(), nullable=False),
        sa.Column("existing_event_id", sa.Uuid(), nullable=False),
        sa.Column("strategy", sa.String(length=64), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["existing_event_id"], ["normalized_events.event_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_dedup_events_created_at", "dedup_events", ["created_at"])
    op.create_index("ix_dedup_events_existing_event_id", "dedup_events", ["existing_event_id"])

    op.create_table(
        "office_impacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("office_id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("impact_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("impact_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("impact_level", sa.String(length=20), nullable=False),
        sa.Column("match_strategy", sa.String(length=64), nullable=False),
        sa.Column("match_score", sa.Float(), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["normalized_events.event_id"]),
        sa.ForeignKeyConstraint(["office_id"], ["offices.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("office_id", "event_id", name="uq_office_impacts_office_event"),
    )
    op.create_index("ix_office_impacts_event_id", "office_impacts", ["event_id"])
    op.create_index("ix_office_impacts_office_start", "office_impacts", ["office_id", "impact_start"])

    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("office_id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("emitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["normalized_events.event_id"]),
        sa.ForeignKeyConstraint(["office_id"], ["offices.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("office_id", "event_id", "channel", name="uq_notifications_delivery"),
    )
    op.create_index("ix_notifications_emitted_at", "notifications", ["emitted_at"])
    op.create_index("ix_notifications_event_id", "notifications", ["event_id"])

    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_type", sa.String(length=50), nullable=False),
        sa.Column("input_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("normalizer_path", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_tasks_created_at", "tasks", ["created_at"])
    op.create_index(op.f("ix_tasks_input_hash"), "tasks", ["input_hash"])
    op.create_index("ix_tasks_task_type_completed_at", "tasks", ["task_type", "completed_at"])

    op.create_table(
        "llm_calls",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column("model", sa.String(length=64), nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), nullable=False),
        sa.Column("completion_tokens", sa.Integer(), nullable=False),
        sa.Column("total_tokens", sa.Integer(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("trace_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_llm_calls_created_at", "llm_calls", ["created_at"])

    op.create_table(
        "poll_requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_poll_requests_source_id", "poll_requests", ["source_id"])
    op.create_index("ix_poll_requests_status_created", "poll_requests", ["status", "created_at"])

    op.create_table(
        "retry_requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("new_task_id", sa.Uuid(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_retry_requests_status_created", "retry_requests", ["status", "created_at"])
    op.create_index("ix_retry_requests_task_id", "retry_requests", ["task_id"])


def downgrade() -> None:
    op.drop_table("retry_requests")
    op.drop_table("poll_requests")
    op.drop_table("llm_calls")
    op.drop_table("tasks")
    op.drop_table("notifications")
    op.drop_table("office_impacts")
    op.drop_table("dedup_events")
    op.drop_table("offices")
    op.drop_table("normalized_events")
    op.drop_table("parsed_records")
    op.drop_table("raw_records")
    op.drop_table("sources")
