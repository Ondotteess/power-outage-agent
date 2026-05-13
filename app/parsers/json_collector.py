from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx

from app.models.schemas import RawRecordSchema, SourceType
from app.parsers.base import BaseCollector

logger = logging.getLogger(__name__)

_MAX_RESPONSE_BYTES = 10 * 1024 * 1024

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PowerOutageAgent/0.1)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.rosseti-sib.ru/otkluchenie-energii/",
}


class JsonCollector(BaseCollector):
    """Fetches a JSON API endpoint and stores the raw response body."""

    async def fetch(self, url: str, trace_id: UUID, verify_ssl: bool = True) -> RawRecordSchema:
        logger.debug(
            "JsonCollector  GET  url=%s  verify_ssl=%s  trace=%s", url, verify_ssl, trace_id
        )

        async with httpx.AsyncClient(
            follow_redirects=True, timeout=30, headers=_HEADERS, verify=verify_ssl
        ) as client:
            response = await client.get(url)

        logger.debug(
            "JsonCollector  response  status=%d  content_type=%s  size=%d bytes  url=%s  trace=%s",
            response.status_code,
            response.headers.get("content-type", "—"),
            len(response.content),
            url,
            trace_id,
        )
        response.raise_for_status()
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise ValueError(f"JSON response too large: {len(response.content)} bytes")

        content = response.text
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        logger.info(
            "JsonCollector  fetched  %d chars  hash=%s  url=%s  trace=%s",
            len(content),
            content_hash,
            url,
            trace_id,
        )

        return RawRecordSchema(
            id=uuid4(),
            source_url=url,
            source_type=SourceType.JSON,
            raw_content=content,
            content_hash=content_hash,
            fetched_at=datetime.now(UTC),
            trace_id=trace_id,
        )
