import hashlib
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PowerOutageAgent/0.1; +https://github.com/your-org/power-outage-agent)"
    )
}


class HtmlCollector(BaseCollector):
    async def fetch(self, url: str, trace_id: UUID) -> RawRecordSchema:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=30, headers=_HEADERS
        ) as client:
            response = await client.get(url)
            response.raise_for_status()

        content = response.text
        return RawRecordSchema(
            id=uuid4(),
            source_url=url,
            source_type=SourceType.HTML,
            raw_content=content,
            content_hash=hashlib.sha256(content.encode()).hexdigest(),
            fetched_at=datetime.now(timezone.utc),
            trace_id=trace_id,
        )
