from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.engine import Base


def _now() -> datetime:
    return datetime.now(UTC)


class Source(Base):
    __tablename__ = "sources"
    __table_args__ = (UniqueConstraint("source_type", "url", name="uq_sources_type_url"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(String(50))
    poll_interval_seconds: Mapped[int]
    is_active: Mapped[bool] = mapped_column(default=True)
    parser_profile: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class RawRecord(Base):
    __tablename__ = "raw_records"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source_id: Mapped[UUID | None] = mapped_column(Uuid, ForeignKey("sources.id"), nullable=True)
    source_url: Mapped[str] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(String(50))
    raw_content: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64), index=True, unique=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    trace_id: Mapped[UUID] = mapped_column(Uuid)


class ParsedRecord(Base):
    __tablename__ = "parsed_records"
    __table_args__ = (
        # fast lookup by raw source; also used to skip re-parsing same raw
        Index("ix_parsed_records_raw_record_id", "raw_record_id"),
        # dedup within a source by external ID
        Index("ix_parsed_records_source_external", "source_id", "external_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    raw_record_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("raw_records.id"))
    source_id: Mapped[UUID | None] = mapped_column(Uuid, ForeignKey("sources.id"), nullable=True)
    external_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    location_city: Mapped[str | None] = mapped_column(String(255), nullable=True)
    location_district: Mapped[str | None] = mapped_column(String(255), nullable=True)
    location_street: Mapped[str | None] = mapped_column(Text, nullable=True)
    location_region_code: Mapped[str | None] = mapped_column(String(128), nullable=True)

    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    extra: Mapped[dict] = mapped_column(JSON, default=dict)

    trace_id: Mapped[UUID] = mapped_column(Uuid)
    extracted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class NormalizedEvent(Base):
    __tablename__ = "normalized_events"
    __table_args__ = (
        UniqueConstraint("parsed_record_id", name="uq_normalized_events_parsed_record_id"),
        UniqueConstraint(
            "event_type",
            "location_normalized",
            "start_time",
            "end_time",
            name="uq_normalized_events_exact_window",
        ),
        Index("ix_normalized_events_parsed_record_id", "parsed_record_id"),
        Index("ix_normalized_events_address_time", "location_normalized", "start_time", "end_time"),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    parsed_record_id: Mapped[UUID | None] = mapped_column(
        Uuid, ForeignKey("parsed_records.id"), nullable=True
    )
    event_type: Mapped[str] = mapped_column(String(50))
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    location_raw: Mapped[str] = mapped_column(Text)
    location_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    location_city: Mapped[str | None] = mapped_column(String(255), nullable=True)
    location_street: Mapped[str | None] = mapped_column(Text, nullable=True)
    location_building: Mapped[str | None] = mapped_column(String(128), nullable=True)

    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    sources: Mapped[list] = mapped_column(JSON, default=list)
    confidence: Mapped[float] = mapped_column(default=0.0)
    trace_id: Mapped[UUID] = mapped_column(Uuid)
    normalized_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class DedupEvent(Base):
    __tablename__ = "dedup_events"
    __table_args__ = (
        Index("ix_dedup_events_created_at", "created_at"),
        Index("ix_dedup_events_existing_event_id", "existing_event_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    incoming_event_id: Mapped[UUID] = mapped_column(Uuid)
    existing_event_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("normalized_events.event_id"))
    strategy: Mapped[str] = mapped_column(String(64))
    trace_id: Mapped[UUID] = mapped_column(Uuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Office(Base):
    __tablename__ = "offices"
    __table_args__ = (
        UniqueConstraint("name", "city", "address", name="uq_offices_name_city_address"),
        Index("ix_offices_city_address", "city", "address"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255))
    city: Mapped[str] = mapped_column(String(255))
    address: Mapped[str] = mapped_column(Text)
    region: Mapped[str] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(default=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    extra: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class OfficeImpact(Base):
    __tablename__ = "office_impacts"
    __table_args__ = (
        UniqueConstraint("office_id", "event_id", name="uq_office_impacts_office_event"),
        Index("ix_office_impacts_event_id", "event_id"),
        Index("ix_office_impacts_office_start", "office_id", "impact_start"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    office_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("offices.id"))
    event_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("normalized_events.event_id"))
    impact_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    impact_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    impact_level: Mapped[str] = mapped_column(String(20))
    match_strategy: Mapped[str] = mapped_column(String(64))
    match_score: Mapped[float] = mapped_column(Float, default=0.0)
    trace_id: Mapped[UUID] = mapped_column(Uuid)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        UniqueConstraint("office_id", "event_id", "channel", name="uq_notifications_delivery"),
        Index("ix_notifications_emitted_at", "emitted_at"),
        Index("ix_notifications_event_id", "event_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    office_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("offices.id"))
    event_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("normalized_events.event_id"))
    channel: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32))
    severity: Mapped[str] = mapped_column(String(20))
    summary: Mapped[str] = mapped_column(Text)
    trace_id: Mapped[UUID] = mapped_column(Uuid)
    emitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class TaskRecord(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("ix_tasks_task_type_completed_at", "task_type", "completed_at"),
        Index("ix_tasks_created_at", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    task_type: Mapped[str] = mapped_column(String(50))
    input_hash: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(20))  # pending | running | done | failed
    attempt: Mapped[int] = mapped_column(default=0)
    payload: Mapped[dict] = mapped_column(JSON)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    trace_id: Mapped[UUID] = mapped_column(Uuid)
    # Per-stage timing. `started_at` is set when the dispatcher pulls the task,
    # `completed_at` when the handler returns (or fails). Duration = diff;
    # nullable so historic rows from before the metrics feature stay valid.
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # For NORMALIZE_EVENT tasks: which path produced the event. Lets the
    # Metrics page show automaton-vs-LLM ratio without scanning every call.
    normalizer_path: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )


class LLMCall(Base):
    """One row per chat-completion request to GigaChat.

    The pipeline writes this from `LLMNormalizer.normalize` after a successful
    call so the Metrics page can show real token/cost numbers. We keep this in
    its own table instead of stuffing into TaskRecord because a single task
    may issue zero or more LLM calls (today it's at most one, but the shape
    survives future caching/retry semantics).
    """

    __tablename__ = "llm_calls"
    __table_args__ = (Index("ix_llm_calls_created_at", "created_at"),)

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    task_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    model: Mapped[str] = mapped_column(String(64))
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(20))  # ok | error
    trace_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class PollRequest(Base):
    __tablename__ = "poll_requests"
    __table_args__ = (
        Index("ix_poll_requests_status_created", "status", "created_at"),
        Index("ix_poll_requests_source_id", "source_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    source_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("sources.id"))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    task_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    trace_id: Mapped[UUID] = mapped_column(Uuid, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RetryRequest(Base):
    __tablename__ = "retry_requests"
    __table_args__ = (
        Index("ix_retry_requests_status_created", "status", "created_at"),
        Index("ix_retry_requests_task_id", "task_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    task_id: Mapped[UUID] = mapped_column(Uuid, ForeignKey("tasks.id"))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    new_task_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    trace_id: Mapped[UUID] = mapped_column(Uuid, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
