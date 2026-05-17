import asyncio
import logging
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_LEGACY_CREATE_ALL_REVISION = "20260514_0002"
_LEGACY_CREATE_ALL_TABLES = {
    "dedup_events",
    "normalized_events",
    "notifications",
    "office_impacts",
    "offices",
    "parsed_records",
    "poll_requests",
    "raw_records",
    "retry_requests",
    "sources",
    "tasks",
}


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
    await _adopt_legacy_create_all_schema_if_needed(cfg)
    # `command.upgrade` is sync and calls `asyncio.run()` inside env.py;
    # offload it so the running event loop is not the one alembic uses.
    logger.info("init_db  running alembic upgrade head")
    await asyncio.to_thread(command.upgrade, cfg, "head")
    logger.info("init_db  alembic upgrade complete")


async def _adopt_legacy_create_all_schema_if_needed(cfg) -> None:
    """Bring old Docker/demo volumes under Alembic without dropping data.

    Earlier MVP builds used SQLAlchemy `create_all`, so existing local Postgres
    volumes can have application tables but no `alembic_version`. A plain
    `upgrade head` then tries to create `sources` again and the API exits on
    startup. This adapter is intentionally narrow: it only runs for Postgres
    databases that look like that legacy schema.
    """

    async with engine.begin() as conn:
        if conn.dialect.name != "postgresql":
            return

        tables = await conn.run_sync(
            lambda sync_conn: set(inspect(sync_conn).get_table_names(schema="public"))
        )
        if "alembic_version" in tables or "sources" not in tables:
            return
        if not _LEGACY_CREATE_ALL_TABLES.issubset(tables):
            logger.warning(
                "init_db  found existing tables without alembic_version, but schema "
                "does not match the known legacy create_all layout; leaving it for "
                "alembic to report the exact problem"
            )
            return

        logger.warning(
            "init_db  adopting legacy create_all schema into alembic revision %s",
            _LEGACY_CREATE_ALL_REVISION,
        )
        await _upgrade_legacy_create_all_schema(conn)

    from alembic import command

    await asyncio.to_thread(command.stamp, cfg, _LEGACY_CREATE_ALL_REVISION)


async def _upgrade_legacy_create_all_schema(conn) -> None:
    statements = [
        """
        ALTER TABLE parsed_records
        ADD COLUMN IF NOT EXISTS fingerprint VARCHAR(64)
        """,
        """
        UPDATE parsed_records
        SET fingerprint = md5(id::text)
        WHERE fingerprint IS NULL
        """,
        """
        ALTER TABLE parsed_records
        ALTER COLUMN fingerprint SET NOT NULL
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_parsed_records_fingerprint'
            ) THEN
                ALTER TABLE parsed_records
                ADD CONSTRAINT uq_parsed_records_fingerprint UNIQUE (fingerprint);
            END IF;
        END $$;
        """,
        """
        ALTER TABLE tasks
        ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 5
        """,
        """
        ALTER TABLE tasks
        ADD COLUMN IF NOT EXISTS started_at TIMESTAMP WITH TIME ZONE
        """,
        """
        ALTER TABLE tasks
        ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP WITH TIME ZONE
        """,
        """
        ALTER TABLE tasks
        ADD COLUMN IF NOT EXISTS duration_ms INTEGER
        """,
        """
        ALTER TABLE tasks
        ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMP WITH TIME ZONE
        """,
        """
        ALTER TABLE tasks
        ADD COLUMN IF NOT EXISTS normalizer_path VARCHAR(32)
        """,
        """
        ALTER TABLE office_impacts
        ADD COLUMN IF NOT EXISTS match_explanation JSON
        """,
        """
        CREATE TABLE IF NOT EXISTS llm_calls (
            id UUID PRIMARY KEY,
            task_id UUID,
            model VARCHAR(64) NOT NULL,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            duration_ms INTEGER NOT NULL,
            status VARCHAR(20) NOT NULL,
            trace_id UUID,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS event_logs (
            id UUID PRIMARY KEY,
            event_type VARCHAR(64) NOT NULL,
            severity VARCHAR(16) NOT NULL,
            message TEXT NOT NULL,
            source VARCHAR(128),
            task_id UUID,
            trace_id UUID,
            payload JSON NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS queue_depth_snapshots (
            id UUID PRIMARY KEY,
            pending INTEGER NOT NULL,
            running INTEGER NOT NULL,
            failed INTEGER NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_normalized_events_parsed_record_id'
            ) THEN
                ALTER TABLE normalized_events
                ADD CONSTRAINT uq_normalized_events_parsed_record_id UNIQUE (parsed_record_id);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_normalized_events_exact_window'
            ) THEN
                ALTER TABLE normalized_events
                ADD CONSTRAINT uq_normalized_events_exact_window
                UNIQUE (event_type, location_normalized, start_time, end_time);
            END IF;
        END $$;
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_parsed_records_fingerprint
        ON parsed_records (fingerprint)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_parsed_records_raw_record_id
        ON parsed_records (raw_record_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_parsed_records_source_external
        ON parsed_records (source_id, external_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_normalized_events_parsed_record_id
        ON normalized_events (parsed_record_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_normalized_events_address_time
        ON normalized_events (location_normalized, start_time, end_time)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_dedup_events_created_at
        ON dedup_events (created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_dedup_events_existing_event_id
        ON dedup_events (existing_event_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_office_impacts_event_id
        ON office_impacts (event_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_office_impacts_office_start
        ON office_impacts (office_id, impact_start)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_notifications_emitted_at
        ON notifications (emitted_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_notifications_event_id
        ON notifications (event_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_tasks_created_at
        ON tasks (created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_tasks_task_type_completed_at
        ON tasks (task_type, completed_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_llm_calls_created_at
        ON llm_calls (created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_event_logs_created_at
        ON event_logs (created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_event_logs_event_type
        ON event_logs (event_type)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_event_logs_trace_id
        ON event_logs (trace_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_queue_depth_snapshots_created_at
        ON queue_depth_snapshots (created_at)
        """,
    ]
    for statement in statements:
        await conn.execute(text(statement))
