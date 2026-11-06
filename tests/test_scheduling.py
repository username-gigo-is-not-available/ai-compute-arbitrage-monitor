"""common.classes.Snapshot (ADR-019)."""

from datetime import UTC, datetime, timedelta

import pytest

from common.classes import Snapshot

NOW = datetime(2026, 10, 1, 14, 37, 12, 345678, tzinfo=UTC)
OCT_1 = datetime(2026, 10, 1, tzinfo=UTC)


class TestFromScheduledAt:
    def test_scheduled_at_midnight_is_kept(self):
        assert Snapshot.from_scheduled_at("2026-10-01T00:00:00+00:00", now=NOW).at == OCT_1

    def test_scheduled_later_in_the_day_is_floored_to_the_day(self):
        assert Snapshot.from_scheduled_at("2026-10-01T14:37:12.5+00:00", now=NOW).at == OCT_1

    def test_non_utc_offset_is_floored_to_the_utc_day(self):
        # 01:00 in UTC+02:00 is 23:00 the previous UTC day
        assert Snapshot.from_scheduled_at("2026-10-02T01:00:00+02:00", now=NOW).at == OCT_1

    def test_naive_value_is_treated_as_utc(self):
        assert Snapshot.from_scheduled_at("2026-10-01T14:00:00", now=NOW).at == OCT_1

    @pytest.mark.parametrize("absent", [None, "", "None", "  "])
    def test_absent_values_fall_back_to_now_floored(self, absent):
        assert Snapshot.from_scheduled_at(absent, now=NOW).at == OCT_1

    def test_runs_on_the_same_day_share_one_snapshot(self):
        # A failed 00:00 run cleared later, or a new manual run that day, lands on the same snapshot (ADR-019)
        assert Snapshot.from_scheduled_at("2026-10-01T00:00:00+00:00", now=NOW) == Snapshot.from_scheduled_at(
            "2026-10-01T09:12:00+00:00", now=NOW
        )

    def test_result_is_utc(self):
        assert Snapshot.from_scheduled_at("2026-10-01T16:00:00+02:00", now=NOW).at.tzinfo == UTC


class TestIsBackfill:
    def test_rerun_later_the_same_day_is_not_backfill(self):
        assert not Snapshot(at=OCT_1).is_backfill(now=NOW)

    def test_exactly_one_day_old_is_not_backfill(self):
        assert not Snapshot(at=OCT_1).is_backfill(now=OCT_1 + timedelta(hours=24))

    def test_previous_day_rerun_the_next_day_is_backfill(self):
        assert Snapshot(at=datetime(2026, 9, 30, tzinfo=UTC)).is_backfill(now=NOW)
