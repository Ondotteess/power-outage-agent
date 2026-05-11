from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from app.models.schemas import ParsedRecordSchema

logger = logging.getLogger(__name__)

# Naive datetimes from the API are local Novosibirsk time (UTC+7).
_DEFAULT_TZ = timezone(timedelta(hours=7))


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        naive = datetime.fromisoformat(value)
    except ValueError:
        return None
    if naive.tzinfo is None:
        naive = naive.replace(tzinfo=_DEFAULT_TZ)
    return naive.astimezone(UTC)


class EsetiParser:
    """Parses the eseti.ru DotNetNuke WebApi `/API/Shutdown` JSON response.

    parser_profile keys:
        date_filter_days (int, default 4): keep records with shutdownDate in [today, today+N].
    """

    def parse(
        self,
        raw_content: str,
        raw_record_id: UUID,
        source_id: UUID | None,
        trace_id: UUID,
        parser_profile: dict,
    ) -> list[ParsedRecordSchema]:
        try:
            items: list[dict] = json.loads(raw_content)
        except json.JSONDecodeError:
            logger.error(
                "EsetiParser  invalid JSON  raw_record_id=%s  trace=%s", raw_record_id, trace_id
            )
            return []

        if not isinstance(items, list):
            logger.error(
                "EsetiParser  expected list, got %s  raw_record_id=%s",
                type(items).__name__,
                raw_record_id,
            )
            return []

        date_filter_days = int(parser_profile.get("date_filter_days", 4))
        today = date.today()
        cutoff = today + timedelta(days=date_filter_days)
        now_utc = datetime.now(UTC)

        results: list[ParsedRecordSchema] = []
        for item in items:
            try:
                record = self._parse_one(
                    item, raw_record_id, source_id, trace_id, today, cutoff, now_utc
                )
                if record is not None:
                    results.append(record)
            except Exception:
                logger.warning(
                    "EsetiParser  failed to parse item  trace=%s", trace_id, exc_info=True
                )

        logger.info(
            "EsetiParser  total=%d  in_window=%d  raw_record_id=%s  trace=%s",
            len(items),
            len(results),
            raw_record_id,
            trace_id,
        )
        return results

    def _parse_one(
        self,
        item: dict,
        raw_record_id: UUID,
        source_id: UUID | None,
        trace_id: UUID,
        today: date,
        cutoff: date,
        now_utc: datetime,
    ) -> ParsedRecordSchema | None:
        start_time = _parse_iso(item.get("shutdownDate", ""))
        if start_time is None:
            return None

        # Compare in local time so the date filter matches what users see on the site.
        event_date = start_time.astimezone(_DEFAULT_TZ).date()
        if not (today <= event_date <= cutoff):
            return None

        end_time = _parse_iso(item.get("restoreDate", ""))
        street = (item.get("street") or "").strip() or None
        houses = (item.get("commaSeparatedHouses") or "").strip()

        return ParsedRecordSchema(
            id=uuid4(),
            raw_record_id=raw_record_id,
            source_id=source_id,
            external_id=None,  # API doesn't expose an ID
            start_time=start_time,
            end_time=end_time,
            location_city=(item.get("city") or "").strip() or None,
            location_district=None,
            location_street=street,
            location_region_code=(item.get("region") or "").strip() or None,
            reason=(item.get("type") or "").strip() or None,
            extra={"houses": houses} if houses else {},
            trace_id=trace_id,
            extracted_at=now_utc,
        )
