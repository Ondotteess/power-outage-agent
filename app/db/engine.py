import asyncio
import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


async def get_session() -> AsyncSession:
    async with async_session_factory() as session:
        yield session


async def init_db() -> None:
    """Run alembic migrations up to head. Replaces the previous
    `Base.metadata.create_all` shortcut: schema is now a versioned artifact,
    and `init_db` is the same code path both the demo runner and the admin
    API use to ensure they're talking to a current DB.
    """
    from alembic.config import Config

    # Imported lazily so the lib is not required for non-DB workflows
    # (e.g. running a unit test that mocks the session factory).
    from alembic import command
    from app.db import models as _models  # noqa: F401 — make sure they're registered

    if not _ALEMBIC_INI.exists():
        raise RuntimeError(f"alembic.ini not found at {_ALEMBIC_INI}")

    cfg = Config(str(_ALEMBIC_INI))
    # `command.upgrade` is sync and calls `asyncio.run()` inside env.py;
    # offload it so the running event loop is not the one alembic uses.
    logger.info("init_db  running alembic upgrade head")
    await asyncio.to_thread(command.upgrade, cfg, "head")
    logger.info("init_db  alembic upgrade complete")
