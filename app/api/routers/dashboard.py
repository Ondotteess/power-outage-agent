from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Query
from sqlalchemy import desc, select

from app.api import queries
from app.api.deps import SessionDep
from app.api.schemas import (
    ActivityEvent,
    DashboardSummary,
    KpiDelta,
    NormalizationQuality,
    QueueBacklogPoint,
)
from app.db.models import (
    NormalizedEvent,
    Notification,
    Office,
    OfficeImpact,
    ParsedRecord,
    RawRecord,
)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _delta(current: int, previous: int) -> tuple[float | None, str | None, str]:
    if previous == 0:
        return None, None, "neutral"
    pct = (current - previous) / previous * 100.0
    label = f"{pct:+.1f}% vs prev"
    status = "success" if pct >= 0 else "warning"
    return round(pct, 1), label, status


@router.get("/summary", response_model=DashboardSummary)
async def summary(session: SessionDep) -> DashboardSummary:
    now_24h = queries.utc_window(24)
    prev_24h = queries.utc_window(48)

    active_sources = await queries.count_sources(session, active_only=True)
    raw_today = await queries.count_raw_since(session, now_24h)
    raw_prev = await queries.count_raw_since(session, prev_24h) - raw_today
    parsed_today = await queries.count_parsed_since(session, now_24h)
    parsed_prev = await queries.count_parsed_since(session, prev_24h) - parsed_today

    task_counts = await queries.count_tasks_by_status(session)
    failed = task_counts.get("failed", 0)
    now = datetime.now(UTC)
    offices_at_risk = await queries.count_active_office_impacts(
        session,
        now,
        horizon_until=now + timedelta(days=7),
    )
    duplicates_skipped = await queries.count_dedup_events_since(session, now_24h)

    pct_raw, label_raw, status_raw = _delta(raw_today, raw_prev)
    pct_parsed, label_parsed, status_parsed = _delta(parsed_today, parsed_prev)

    return DashboardSummary(
        active_sources=KpiDelta(
            value=active_sources, status="success" if active_sources else "warning"
        ),
        raw_records_today=KpiDelta(
            value=raw_today, delta_pct=pct_raw, delta_label=label_raw, status=status_raw
        ),
        parsed_outages=KpiDelta(
            value=parsed_today,
            delta_pct=pct_parsed,
            delta_label=label_parsed,
            status=status_parsed,
        ),
        duplicates_skipped=KpiDelta(
            value=duplicates_skipped,
            status="neutral" if duplicates_skipped == 0 else "success",
        ),
        failed_tasks=KpiDelta(value=failed, status="error" if failed else "success"),
        offices_at_risk=KpiDelta(
            value=offices_at_risk,
            delta_pct=None,
            delta_label="active now",
            status="warning" if offices_at_risk else "success",
        ),
    )


@router.get("/activity", response_model=list[ActivityEvent])
async def activity(
    session: SessionDep,
    limit: int = Query(30, ge=1, le=200),
) -> list[ActivityEvent]:
    """Synthesize an activity feed by interleaving recent raw / parsed / normalized
    / task rows. Cheap and good enough for a dashboard; a real event log can
    replace this later without changing the API shape."""
    items: list[ActivityEvent] = []

    raw_rows = (
        (
            await session.execute(
                select(RawRecord).order_by(desc(RawRecord.fetched_at)).limit(limit // 3 + 5)
            )
        )
        .scalars()
        .all()
    )
    for r in raw_rows:
        items.append(
            ActivityEvent(
                id=f"raw-{r.id}",
                type="RawFetched",
                severity="info",
                source=str(r.source_id) if r.source_id else r.source_url,
                message=f"Fetched {len(r.raw_content or '')} bytes from {r.source_type}",
                at=r.fetched_at,
            )
        )

    parsed_rows = (
        (
            await session.execute(
                select(ParsedRecord).order_by(desc(ParsedRecord.extracted_at)).limit(limit // 3 + 5)
            )
        )
        .scalars()
        .all()
    )
    for p in parsed_rows:
        items.append(
            ActivityEvent(
                id=f"parsed-{p.id}",
                type="RawParsed",
                severity="success",
                source=str(p.source_id) if p.source_id else None,
                message=f"Parsed outage @ {p.location_city or 'unknown'} {p.location_street or ''}".strip(),
                at=p.extracted_at,
            )
        )

    impact_rows = (
        await session.execute(
            select(OfficeImpact, Office)
            .join(Office, Office.id == OfficeImpact.office_id)
            .order_by(desc(OfficeImpact.detected_at))
            .limit(limit // 3 + 5)
        )
    ).all()
    for impact, office in impact_rows:
        severity = "error" if impact.impact_level == "high" else "warning"
        items.append(
            ActivityEvent(
                id=f"impact-{impact.id}",
                type="OfficeImpactDetected",
                severity=severity,
                source=office.name,
                message=f"{office.name}: {impact.match_strategy} match",
                at=impact.detected_at,
            )
        )

    notification_rows = (
        await session.execute(
            select(Notification, Office)
            .join(Office, Office.id == Notification.office_id)
            .order_by(desc(Notification.emitted_at))
            .limit(limit // 3 + 5)
        )
    ).all()
    for notification, office in notification_rows:
        severity = "success" if notification.status == "sent" else "warning"
        items.append(
            ActivityEvent(
                id=f"notification-{notification.id}",
                type="NotificationEmitted",
                severity=severity,
                source=office.name,
                message=notification.summary,
                at=notification.emitted_at,
            )
        )

    task_rows = await queries.list_tasks(
        session,
        status="failed",
        limit=limit // 3 + 5,
    )
    for t in task_rows:
        items.append(
            ActivityEvent(
                id=f"task-{t.id}",
                type="TaskFailed",
                severity="error",
                source=t.task_type,
                message=(t.error or "task failed")[:200],
                at=t.updated_at,
            )
        )

    items.sort(key=lambda i: i.at, reverse=True)
    return items[:limit]


@router.get("/normalization-quality", response_model=NormalizationQuality)
async def normalization_quality(
    session: SessionDep,
) -> NormalizationQuality:
    dist = await queries.confidence_distribution(session)
    parsed_total = await queries.count_parsed_since(session, datetime.now(UTC) - timedelta(days=30))

    # Average confidence — done client-side from rows we already pulled would
    # double-scan; do a small explicit query.
    rows = (
        (
            await session.execute(
                select(NormalizedEvent.confidence)
                .order_by(desc(NormalizedEvent.normalized_at))
                .limit(1000)
            )
        )
        .scalars()
        .all()
    )
    values = [v or 0.0 for v in rows]
    avg = sum(values) / len(values) if values else 0.0

    return NormalizationQuality(
        average_confidence=round(avg, 3),
        normalized_count=dist["total"],
        parsed_total=parsed_total,
        high=dist["high"],
        medium=dist["medium"],
        low=dist["low"],
        estimated_tokens=None,
        estimated_cost_usd=None,
    )


@router.get("/queue-backlog", response_model=list[QueueBacklogPoint])
async def queue_backlog(session: SessionDep) -> list[QueueBacklogPoint]:
    """24h queue backlog samples from persisted snapshots."""
    now = datetime.now(UTC)
    snapshots = await queries.list_queue_depth_snapshots(session, since=now - timedelta(hours=24))
    if snapshots:
        return [
            QueueBacklogPoint(
                at=snapshot.created_at,
                pending=snapshot.pending,
                running=snapshot.running,
                failed=snapshot.failed,
            )
            for snapshot in snapshots
        ]

    counts = await queries.count_tasks_by_status(session)
    return [
        QueueBacklogPoint(
            at=now,
            pending=counts.get("pending", 0),
            running=counts.get("running", 0),
            failed=counts.get("failed", 0),
        )
    ]
