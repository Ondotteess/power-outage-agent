import logging

from app.models.schemas import NormalizedEventSchema, RawRecordSchema

logger = logging.getLogger(__name__)


class StubNormalizer:
    """Placeholder — returns None until LLM normalizer is wired in Week 2."""

    async def normalize(self, raw: RawRecordSchema) -> NormalizedEventSchema | None:
        logger.debug("StubNormalizer: skipping %s", raw.source_url)
        return None
