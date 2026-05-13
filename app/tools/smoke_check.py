from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

import httpx
from sqlalchemy import distinct, func, or_, select

from app.db.engine import async_session_factory, init_db
from app.db.models import (
    DedupEvent,
    NormalizedEvent,
    Notification,
    Office,
    OfficeImpact,
    ParsedRecord,
    RawRecord,
    TaskRecord,
)

SYSTEM_ABANDONED_TASK_ERROR = "abandoned by previous pipeline process"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a demo E2E pipeline run.")
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--min-raw", type=int, default=1)
    parser.add_argument("--min-parsed", type=int, default=1)
    parser.add_argument("--min-normalized", type=int, default=1)
    parser.add_argument("--min-offices", type=int, default=1)
    parser.add_argument("--expected-offices", type=int, default=None)
    parser.add_argument("--min-impacts", type=int, default=1)
    parser.add_argument("--min-active-risk-offices", type=int, default=0)
    parser.add_argument("--min-notifications", type=int, default=1)
    return parser.parse_args()


async def _count(model) -> int:
    async with async_session_factory() as session:
        result = await session.execute(select(func.count(model.id)))
        return result.scalar() or 0


async def _count_normalized_events() -> int:
    async with async_session_factory() as session:
        result = await session.execute(select(func.count(NormalizedEvent.event_id)))
        return result.scalar() or 0


async def _count_active_risk_offices() -> int:
    now = datetime.now(UTC)
    async with async_session_factory() as session:
        result = await session.execute(
            select(func.count(distinct(OfficeImpact.office_id))).where(
                OfficeImpact.impact_start <= now,
                or_(OfficeImpact.impact_end.is_(None), OfficeImpact.impact_end >= now),
            )
        )
        return result.scalar() or 0


async def _task_status_counts() -> dict[str, int]:
    async with async_session_factory() as session:
        result = await session.execute(
            select(TaskRecord.status, func.count(TaskRecord.id))
            .where(
                or_(
                    TaskRecord.status != "failed",
                    TaskRecord.error.is_(None),
                    TaskRecord.error != SYSTEM_ABANDONED_TASK_ERROR,
                )
            )
            .group_by(TaskRecord.status)
        )
        return {str(row[0]): int(row[1]) for row in result.all()}


async def _api_get(client: httpx.AsyncClient, path: str) -> dict:
    response = await client.get(path)
    response.raise_for_status()
    return response.json()


def _require(ok: bool, message: str, failures: list[str]) -> None:
    if not ok:
        failures.append(message)


async def main() -> int:
    args = _parse_args()
    await init_db()

    counts = {
        "raw_records": await _count(RawRecord),
        "parsed_records": await _count(ParsedRecord),
        "normalized_events": await _count_normalized_events(),
        "offices": await _count(Office),
        "office_impacts": await _count(OfficeImpact),
        "active_risk_offices": await _count_active_risk_offices(),
        "notifications": await _count(Notification),
        "dedup_events": await _count(DedupEvent),
    }
    task_counts = await _task_status_counts()

    failures: list[str] = []
    _require(counts["raw_records"] >= args.min_raw, "raw_records below threshold", failures)
    _require(
        counts["parsed_records"] >= args.min_parsed, "parsed_records below threshold", failures
    )
    _require(
        counts["normalized_events"] >= args.min_normalized,
        "normalized_events below threshold",
        failures,
    )
    if args.expected_offices is not None:
        _require(
            counts["offices"] == args.expected_offices,
            "offices do not match expected count",
            failures,
        )
    _require(counts["offices"] >= args.min_offices, "offices below threshold", failures)
    _require(
        counts["office_impacts"] >= args.min_impacts, "office_impacts below threshold", failures
    )
    _require(
        counts["active_risk_offices"] >= args.min_active_risk_offices,
        "active risk offices below threshold",
        failures,
    )
    _require(
        counts["notifications"] >= args.min_notifications,
        "notifications below threshold",
        failures,
    )
    _require(task_counts.get("failed", 0) == 0, "failed tasks present", failures)

    async with httpx.AsyncClient(base_url=args.api_base_url, timeout=10.0) as client:
        health = await _api_get(client, "/api/health")
        pipeline = await _api_get(client, "/api/pipeline/status")
        summary = await _api_get(client, "/api/dashboard/summary")
        notifications = await client.get("/api/notifications")
        notifications.raise_for_status()

    _require(health.get("ok") is True, "health endpoint is not ok", failures)
    _require(pipeline.get("overall") == "healthy", "pipeline is not healthy", failures)
    _require(
        summary.get("failed_tasks", {}).get("value") == 0,
        "dashboard reports failed tasks",
        failures,
    )

    print("Smoke check counts")
    for key, value in counts.items():
        print(f"  {key}: {value}")
    print("Task statuses")
    for key, value in sorted(task_counts.items()):
        print(f"  {key}: {value}")

    if failures:
        print("Smoke check FAILED")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("Smoke check OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
