"""FastAPI admin panel application.

Runs as a separate process from the pipeline worker:

    uvicorn app.api.app:app --reload --port 8000

CORS origins are explicit and configured via CORS_ALLOW_ORIGINS.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routers import (
    dashboard,
    logs,
    map,
    metrics,
    notifications,
    offices,
    pipeline,
    records,
    sources,
    tasks,
)
from app.config import settings
from app.db.engine import async_session_factory, init_db
from app.db.repositories import OfficeStore
from app.matching.defaults import DEFAULT_OFFICES

logger = logging.getLogger(__name__)


def _cors_origins() -> list[str]:
    return [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Idempotent — same create_all the pipeline runs. Lets the admin API
    # come up even before the pipeline has been started for the first time.
    try:
        await init_db()
        await OfficeStore(async_session_factory).seed_if_empty(DEFAULT_OFFICES)
        logger.info("Admin API: database schema ready")
    except OSError as exc:
        logger.error("Admin API: cannot reach database: %s", exc)
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Power Outage Agent — Admin API",
        version="0.1.0",
        description="Read-only admin/observability API for the outage pipeline.",
        lifespan=_lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins(),
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(dashboard.router)
    app.include_router(logs.router)
    app.include_router(map.router)
    app.include_router(metrics.router)
    app.include_router(offices.router)
    app.include_router(notifications.router)
    app.include_router(pipeline.router)
    app.include_router(sources.router)
    app.include_router(records.router)
    app.include_router(tasks.router)

    @app.get("/api/health", tags=["health"])
    async def health() -> dict:
        return {"ok": True}

    return app


app = create_app()
