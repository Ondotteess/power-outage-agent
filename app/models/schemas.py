from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class SourceType(StrEnum):
    HTML = "html"
    RSS = "rss"
    TELEGRAM = "telegram"
    JSON = "json"
    OTHER = "other"


class EventType(StrEnum):
    POWER_OUTAGE = "power_outage"
    MAINTENANCE = "maintenance"
    INFRASTRUCTURE_FAILURE = "infrastructure_failure"
    OTHER = "other"


class ImpactLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class SourceSchema(BaseModel):
    id: UUID
    name: str
    url: str
    source_type: SourceType
    poll_interval_seconds: int
    is_active: bool = True
    parser_profile: dict = Field(default_factory=dict)


class RawRecordSchema(BaseModel):
    id: UUID
    source_id: UUID | None = None
    source_url: str
    source_type: SourceType
    raw_content: str
    content_hash: str
    fetched_at: datetime
    trace_id: UUID


class LocationSchema(BaseModel):
    raw: str
    normalized: str | None = None
    city: str | None = None
    street: str | None = None
    building: str | None = None


class NormalizedEventSchema(BaseModel):
    event_id: UUID
    parsed_record_id: UUID | None = None
    event_type: EventType
    start_time: datetime
    end_time: datetime | None = None
    location: LocationSchema
    reason: str | None = None
    sources: list[UUID] = Field(default_factory=list)
    confidence: float = 0.0


class OfficeImpactSchema(BaseModel):
    office_id: UUID
    event_id: UUID
    impact_start: datetime
    impact_end: datetime | None = None
    impact_level: ImpactLevel
    match_strategy: str  # exact_address | geo_radius | feeder
    detected_at: datetime


class NotificationSchema(BaseModel):
    notification_id: UUID
    office_id: UUID
    event_id: UUID
    type: EventType
    severity: ImpactLevel
    start_time: datetime
    end_time: datetime | None = None
    source_summary: str
    channels: list[str] = Field(default_factory=list)
    emitted_at: datetime


class ParsedRecordSchema(BaseModel):
    """Structured record extracted from a raw source, before LLM normalization."""

    id: UUID
    raw_record_id: UUID
    source_id: UUID | None = None
    external_id: str | None = None  # source's own ID, used for dedup

    start_time: datetime | None = None
    end_time: datetime | None = None

    location_city: str | None = None
    location_district: str | None = None
    location_street: str | None = None
    location_region_code: str | None = None

    reason: str | None = None
    extra: dict = Field(default_factory=dict)

    trace_id: UUID
    extracted_at: datetime
