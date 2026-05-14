"""Integration tests for /api/offices/import (JSON + CSV).

Uses an in-memory SQLite DB via a separate async engine to keep the suite
self-contained — production uses Postgres but the import logic is plain ORM.
"""

from __future__ import annotations

import io
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import deps
from app.api.routers import offices as offices_router
from app.config import settings
from app.db.engine import Base
from app.db.models import Office


@asynccontextmanager
async def _ephemeral_db() -> AsyncIterator[async_sessionmaker]:
    """Create a fresh in-memory SQLite for each test, tear it down at the end."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        # SQLite doesn't honour JSON default=list — coerce via metadata.create_all
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def client():
    """FastAPI TestClient with the offices router wired to a fresh DB."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(offices_router.router)

    async with _ephemeral_db() as sf:
        # Override the session_factory used by the route handler.
        offices_router.async_session_factory = sf
        # Override SessionDep so GET /offices works in the test DB too.

        async def _override_get_session():
            async with sf() as session:
                yield session

        app.dependency_overrides[deps.get_session] = _override_get_session
        # Force open mode for the tests.
        original_token = settings.office_import_token
        settings.office_import_token = ""
        try:
            with TestClient(app) as c:
                yield c, sf
        finally:
            settings.office_import_token = original_token


async def _count_offices(sf: async_sessionmaker) -> int:
    from sqlalchemy import func, select

    async with sf() as session:
        result = await session.execute(select(func.count(Office.id)))
        return int(result.scalar() or 0)


async def test_import_json_inserts_new_offices(client):
    c, sf = client
    payload = {
        "offices": [
            {"name": "Test 1", "city": "Кемерово", "address": "пр. Ленина, 90", "region": "RU-KEM"},
            {"name": "Test 2", "city": "Томск", "address": "пр. Ленина, 120", "region": "RU-TOM"},
        ]
    }
    response = c.post("/api/offices/import", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body == {"received": 2, "inserted": 2, "updated": 0, "skipped": 0}
    assert await _count_offices(sf) == 2


async def test_import_json_is_idempotent(client):
    c, sf = client
    payload = {
        "offices": [
            {"name": "Test 1", "city": "Кемерово", "address": "пр. Ленина, 90", "region": "RU-KEM"},
        ]
    }
    c.post("/api/offices/import", json=payload)
    response = c.post("/api/offices/import", json=payload)

    body = response.json()
    assert body == {"received": 1, "inserted": 0, "updated": 1, "skipped": 0}
    assert await _count_offices(sf) == 1


async def test_import_json_updates_coordinates_on_existing_row(client):
    c, sf = client
    c.post(
        "/api/offices/import",
        json={
            "offices": [
                {
                    "name": "Test 1",
                    "city": "Кемерово",
                    "address": "пр. Ленина, 90",
                    "region": "RU-KEM",
                }
            ]
        },
    )
    c.post(
        "/api/offices/import",
        json={
            "offices": [
                {
                    "name": "Test 1",
                    "city": "Кемерово",
                    "address": "пр. Ленина, 90",
                    "region": "RU-KEM",
                    "latitude": 55.354,
                    "longitude": 86.087,
                }
            ]
        },
    )

    from sqlalchemy import select

    async with sf() as session:
        result = await session.execute(select(Office))
        office = result.scalars().one()
    assert office.latitude == pytest.approx(55.354)
    assert office.longitude == pytest.approx(86.087)


async def test_import_csv_round_trip(client):
    c, sf = client
    csv_body = (
        "name,city,address,region,latitude,longitude,is_active\n"
        'Test 1,Кемерово,"пр. Ленина, 90",RU-KEM,55.354,86.087,true\n'
        'Test 2,Томск,"пр. Ленина, 120",RU-TOM,,,true\n'
    )
    response = c.post(
        "/api/offices/import/csv",
        files={"file": ("offices.csv", io.BytesIO(csv_body.encode("utf-8")), "text/csv")},
    )
    body = response.json()
    assert response.status_code == 200, body
    assert body == {"received": 2, "inserted": 2, "updated": 0, "skipped": 0}
    assert await _count_offices(sf) == 2


async def test_import_csv_missing_columns_returns_400(client):
    c, _sf = client
    csv_body = "name,city\nfoo,bar\n"
    response = c.post(
        "/api/offices/import/csv",
        files={"file": ("bad.csv", io.BytesIO(csv_body.encode("utf-8")), "text/csv")},
    )
    assert response.status_code == 400
    assert "missing required columns" in response.json()["detail"]


async def test_import_rejects_bad_token_when_configured(client):
    c, _sf = client
    settings.office_import_token = "secret-token"
    try:
        response = c.post(
            "/api/offices/import",
            json={"offices": []},
            headers={"X-Import-Token": "wrong"},
        )
        assert response.status_code == 401
        # Right token works.
        response_ok = c.post(
            "/api/offices/import",
            json={"offices": []},
            headers={"X-Import-Token": "secret-token"},
        )
        assert response_ok.status_code == 200
    finally:
        settings.office_import_token = ""
