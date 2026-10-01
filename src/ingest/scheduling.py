from datetime import datetime, timedelta, UTC

BACKFILL_TOLERANCE = timedelta(hours=1)


def resolve_snapshot_at(scheduled_at: str | None, now: datetime | None = None) -> datetime:
    """Scheduled run time floored to the hour (ADR-019); falls back to now() when Airflow passed nothing."""
    if scheduled_at is None or scheduled_at.strip() in ("", "None"):
        moment = now or datetime.now(UTC)
    else:
        moment = datetime.fromisoformat(scheduled_at.strip())
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def is_backfill(snapshot_at: datetime, now: datetime | None = None) -> bool:
    """A live-only API cannot be backfilled: a snapshot older than the tolerance would mislabel today's market."""
    return (now or datetime.now(UTC)) - snapshot_at > BACKFILL_TOLERANCE
