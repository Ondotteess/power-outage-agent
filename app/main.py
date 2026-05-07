import asyncio
import logging

from app.config import settings
from app.db.engine import init_db
from app.workers.collector_worker import CollectorWorker
from app.workers.queue import TaskQueue
from app.workers.scheduler import Scheduler, SourceConfig

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Initializing database")
    await init_db()

    queue = TaskQueue()

    scheduler = Scheduler(queue)
    # TODO week-1: load sources from DB instead of hardcoding
    scheduler.add_source(
        SourceConfig(
            source_id="00000000-0000-0000-0000-000000000001",
            url="https://example.com/outages",
            source_type="html",
            poll_interval_seconds=21600,  # 6 h
        )
    )

    worker = CollectorWorker(queue)

    logger.info("Starting pipeline")
    await asyncio.gather(
        scheduler.run(),
        worker.run(),
    )


if __name__ == "__main__":
    asyncio.run(main())
