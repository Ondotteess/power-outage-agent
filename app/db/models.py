from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, Uuid
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


class TaskRecord(Base):
    __tablename__ = "tasks"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    task_type: Mapped[str] = mapped_column(String(50))
    input_hash: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(20))  # pending | running | done | failed
    attempt: Mapped[int] = mapped_column(default=0)
    payload: Mapped[dict] = mapped_column(JSON)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    trace_id: Mapped[UUID] = mapped_column(Uuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )
