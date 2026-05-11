from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PowerOutageAgent/0.1; +https://github.com/your-org/power-outage-agent)"
    )
}


class HtmlCollector(BaseCollector):
    async def fetch(self, url: str, trace_id: UUID) -> RawRecordSchema:
        logger.debug("HtmlCollector  GET  url=%s  trace=%s", url, trace_id)

        async with httpx.AsyncClient(follow_redirects=True, timeout=30, headers=_HEADERS) as client:
            response = await client.get(url)

        logger.debug(
            "HtmlCollector  response  status=%d  content_type=%s  size=%d bytes  url=%s  trace=%s",
            response.status_code,
            response.headers.get("content-type", "—"),
            len(response.content),
            url,
            trace_id,
        )
        response.raise_for_status()

        content = response.text
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        logger.info(
            "HtmlCollector  fetched  %d chars  hash=%s  url=%s  trace=%s",
            len(content),
            content_hash,
            url,
            trace_id,
        )

        return RawRecordSchema(
            id=uuid4(),
            source_url=url,
            source_type=SourceType.HTML,
            raw_content=content,
            content_hash=content_hash,
            fetched_at=datetime.now(UTC),
            trace_id=trace_id,
        )
