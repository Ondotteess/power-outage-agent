from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from app.models.schemas import ParsedRecordSchema

logger = logging.getLogger(__name__)

_DATE_FMT = "%d.%m.%Y"
_TIME_FMT = "%H:%M"

# Rosseti Siberia operates across UTC+6..+9; UTC+7 is the dominant zone.
# Timestamps are stored in UTC. The normalization layer will refine if needed.
_DEFAULT_TZ = timezone(timedelta(hours=7))


def _parse_dt(date_str: str, time_str: str, tz: timezone) -> datetime | None:
    try:
        naive = datetime.strptime(f"{date_str} {time_str}", f"{_DATE_FMT} {_TIME_FMT}")
        return naive.replace(tzinfo=tz).astimezone(UTC)
    except ValueError:
        return None


class RossetiSibParser:
    """Parses the rosseti-sib.ru data.php JSON response.

    parser_profile keys:
        date_filter_days (int, default 4): keep records with date_start in [today, today+N].
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
                "RossetiSibParser  invalid JSON  raw_record_id=%s  trace=%s",
                raw_record_id,
                trace_id,
            )
            return []

        if not isinstance(items, list):
            logger.error(
                "RossetiSibParser  expected list, got %s  raw_record_id=%s",
                type(items).__name__,
                raw_record_id,
            )
            return []

        date_filter_days: int = parser_profile.get("date_filter_days", 4)
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
                    "RossetiSibParser  failed to parse item id=%s  trace=%s",
                    item.get("id"),
                    trace_id,
                    exc_info=True,
                )

        logger.info(
            "RossetiSibParser  total=%d  in_window=%d  raw_record_id=%s  trace=%s",
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
        date_start_str = item.get("date_start", "").strip()
        if not date_start_str:
            return None

        try:
            event_date = datetime.strptime(date_start_str, _DATE_FMT).date()
        except ValueError:
            logger.debug("RossetiSibParser  bad date_start=%r  skipping", date_start_str)
            return None

        if not (today <= event_date <= cutoff):
            return None

        date_finish_str = item.get("date_finish", date_start_str).strip() or date_start_str
        time_start_str = item.get("time_start", "00:00").strip() or "00:00"
        time_finish_str = item.get("time_finish", "").strip()

        start_time = _parse_dt(date_start_str, time_start_str, _DEFAULT_TZ)
        end_time = _parse_dt(date_finish_str, time_finish_str, _DEFAULT_TZ) if time_finish_str else None

        return ParsedRecordSchema(
            id=uuid4(),
            raw_record_id=raw_record_id,
            source_id=source_id,
            external_id=str(item.get("id", "")).strip() or None,
            start_time=start_time,
            end_time=end_time,
            location_city=item.get("gorod", "").strip() or None,
            location_district=item.get("raion", "").strip() or None,
            location_street=item.get("street", "").strip() or None,
            location_region_code=item.get("region", "").strip() or None,
            reason=item.get("res", "").strip() or None,
            extra={"f_otkl": item.get("f_otkl")},
            trace_id=trace_id,
            extracted_at=now_utc,
        )
