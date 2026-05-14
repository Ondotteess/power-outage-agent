from __future__ import annotations

import asyncio
import logging
from typing import Protocol

logger = logging.getLogger(__name__)


class QueueSnapshotStoreProtocol(Protocol):
    async def record_current(self) -> None: ...


class QueueSnapshotter:
    def __init__(self, store: QueueSnapshotStoreProtocol, *, interval_seconds: float = 60.0) -> None:
        self._store = store
        self._interval_seconds = interval_seconds

    async def run(self) -> None:
        logger.info("QueueSnapshotter started  interval=%.1fs", self._interval_seconds)
        while True:
            try:
                await self._store.record_current()
            except Exception:  # noqa: BLE001 - observability must not break pipeline
                logger.exception("QueueSnapshotter failed to record queue depth")
            await asyncio.sleep(self._interval_seconds)
