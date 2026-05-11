from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime, timedelta, timezone
from uuid import UUID, uuid4

from bs4 import BeautifulSoup, Tag

from app.models.schemas import ParsedRecordSchema

logger = logging.getLogger(__name__)

_DATE_FMT = "%d.%m.%Y"
_TIME_RE = re.compile(r"с\s*(\d{1,2}):(\d{2})\s*до\s*(\d{1,2}):(\d{2})")
_DEFAULT_TZ = timezone(timedelta(hours=7))  # Tomsk = UTC+7


class RossetiTomskParser:
    """Parses the rosseti-tomsk.ru planovie_otklucheniya.php HTML page.

    Table: `table.shuthown_table`. Each `<tr>` contains one `<td>` with five
    `<p class="t1..t5">` paragraphs, each having a `<label>` followed by the value.

    parser_profile keys:
        date_filter_days (int, default 4): keep records with date in [today, today+N].
    """

    def parse(
        self,
        raw_content: str,
        raw_record_id: UUID,
        source_id: UUID | None,
        trace_id: UUID,
        parser_profile: dict,
    ) -> list[ParsedRecordSchema]:
        soup = BeautifulSoup(raw_content, "html.parser")
        table = soup.find("table", class_="shuthown_table")
        if table is None:
            logger.warning(
                "RossetiTomskParser  table.shuthown_table not found  raw_record_id=%s  trace=%s",
                raw_record_id,
                trace_id,
            )
            return []

        rows = table.find_all("tr")
        date_filter_days = int(parser_profile.get("date_filter_days", 4))
        today = date.today()
        cutoff = today + timedelta(days=date_filter_days)
        now_utc = datetime.now(UTC)

        results: list[ParsedRecordSchema] = []
        for tr in rows:
            td = tr.find("td")
            if td is None:
                continue
            try:
                rec = self._parse_row(
                    td, raw_record_id, source_id, trace_id, today, cutoff, now_utc
                )
                if rec is not None:
                    results.append(rec)
            except Exception:
                logger.warning(
                    "RossetiTomskParser  failed to parse row  trace=%s",
                    trace_id,
                    exc_info=True,
                )

        logger.info(
            "RossetiTomskParser  rows=%d  in_window=%d  raw_record_id=%s  trace=%s",
            len(rows),
            len(results),
            raw_record_id,
            trace_id,
        )
        return results

    def _parse_row(
        self,
        td: Tag,
        raw_record_id: UUID,
        source_id: UUID | None,
        trace_id: UUID,
        today: date,
        cutoff: date,
        now_utc: datetime,
    ) -> ParsedRecordSchema | None:
        fields: dict[str, str] = {}
        for p in td.find_all("p"):
            cls = " ".join(p.get("class", []))
            value = _value_after_label(p)
            if "t1" in cls:
                fields["locality"] = value
            elif "t2" in cls:
                fields["address"] = value
            elif "t3" in cls:
                # t3 occurs twice: first time is date, second is time
                key = "date" if "date" not in fields else "time"
                fields[key] = value
            elif "t4" in cls:
                fields["reason"] = value
            elif "t5" in cls:
                fields["equipment"] = value

        date_str = fields.get("date", "").strip()
        if not date_str:
            return None
        try:
            event_date = datetime.strptime(date_str, _DATE_FMT).date()
        except ValueError:
            return None

        if not (today <= event_date <= cutoff):
            return None

        start_time, end_time = _parse_time_range(event_date, fields.get("time", ""))

        region, district, city = _split_locality(fields.get("locality", ""))
        external_id = (td.get("id") or "").strip() or None

        return ParsedRecordSchema(
            id=uuid4(),
            raw_record_id=raw_record_id,
            source_id=source_id,
            external_id=external_id,
            start_time=start_time,
            end_time=end_time,
            location_city=city,
            location_district=district,
            location_street=fields.get("address", "").strip() or None,
            location_region_code=region,
            reason=fields.get("reason", "").strip() or None,
            extra={"equipment": fields.get("equipment", "").strip()},
            trace_id=trace_id,
            extracted_at=now_utc,
        )


def _value_after_label(p_tag: Tag) -> str:
    """Return the text inside a <p> excluding the leading <label>."""
    label = p_tag.find("label")
    if label is None:
        return p_tag.get_text(strip=True)
    full = p_tag.get_text()
    label_text = label.get_text()
    return full.replace(label_text, "", 1).strip()


def _parse_time_range(event_date: date, time_str: str) -> tuple[datetime | None, datetime | None]:
    match = _TIME_RE.search(time_str)
    if not match:
        return None, None
    sh, sm, eh, em = (int(x) for x in match.groups())
    base = datetime.combine(event_date, datetime.min.time()).replace(tzinfo=_DEFAULT_TZ)
    start = base.replace(hour=sh, minute=sm).astimezone(UTC)
    end = base.replace(hour=eh, minute=em).astimezone(UTC)
    return start, end


def _split_locality(text: str) -> tuple[str | None, str | None, str | None]:
    """Split "Томская обл, Томский р-н, деревня Нелюбино" into (region, district, city).

    Handles 1, 2 and 3-part cases. Region detected by suffixes 'обл', 'край', 'респ', etc.;
    district by 'р-н' or 'район'.
    """
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        return None, None, None

    region: str | None = None
    district: str | None = None
    city: str | None = None

    region_markers = ("обл", "край", "респ", "ао")
    district_markers = ("р-н", "район")

    remaining = list(parts)
    if remaining and any(m in remaining[0].lower() for m in region_markers):
        region = remaining.pop(0)

    if remaining and any(m in remaining[0].lower() for m in district_markers):
        district = remaining.pop(0)

    if remaining:
        # last meaningful component is the city/village
        city = remaining[-1]

    return region, district, city
