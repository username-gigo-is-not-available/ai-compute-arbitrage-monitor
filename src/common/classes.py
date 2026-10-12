from datetime import datetime, timedelta, UTC
from typing import ClassVar

from pydantic import BaseModel, ConfigDict

from common.enums import DatasetName, DatasetType


class Dataset(BaseModel):
    dataset_name: DatasetName
    dataset_type: DatasetType


class Snapshot(BaseModel):
    """The scheduled day an ingest run's data represents, not the fetch moment (ADR-019)."""
    model_config = ConfigDict(frozen=True)

    # A day: a failed daily run cleared in Airflow keeps its 00:00 label and must still write (ADR-019)
    BACKFILL_TOLERANCE: ClassVar[timedelta] = timedelta(hours=24)

    at: datetime

    @classmethod
    def from_scheduled_at(cls, scheduled_at: str | None, now: datetime | None = None) -> "Snapshot":
        # Airflow's data_interval_end floored to the UTC day; None, "" and "None" mean no Airflow, so now() is used
        if scheduled_at is None or scheduled_at.strip() in ("", "None"):
            moment = now or datetime.now(UTC)
        else:
            moment = datetime.fromisoformat(scheduled_at.strip())
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=UTC)
        return cls(at=moment.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))

    def is_backfill(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(UTC)) - self.at > self.BACKFILL_TOLERANCE
