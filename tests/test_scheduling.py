"""Unit tests for ingest.scheduling (ADR-019).

Run directly:   uv run python tests/test_scheduling.py
Under pytest:   uv run --extra dev pytest tests/test_scheduling.py
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone, UTC

from ingest.scheduling import is_backfill, resolve_snapshot_at

NOW = datetime(2026, 10, 1, 14, 37, 12, 345678, tzinfo=UTC)


class TestResolveSnapshotAt(unittest.TestCase):

    def test_scheduled_on_the_hour_is_kept(self):
        self.assertEqual(resolve_snapshot_at("2026-10-01T14:00:00+00:00", now=NOW),
                         datetime(2026, 10, 1, 14, tzinfo=UTC))

    def test_scheduled_off_the_hour_is_floored(self):
        self.assertEqual(resolve_snapshot_at("2026-10-01T14:37:12.5+00:00", now=NOW),
                         datetime(2026, 10, 1, 14, tzinfo=UTC))

    def test_non_utc_offset_is_normalised_to_utc(self):
        self.assertEqual(resolve_snapshot_at("2026-10-01T16:00:00+02:00", now=NOW),
                         datetime(2026, 10, 1, 14, tzinfo=UTC))

    def test_naive_value_is_treated_as_utc(self):
        self.assertEqual(resolve_snapshot_at("2026-10-01T14:00:00", now=NOW),
                         datetime(2026, 10, 1, 14, tzinfo=UTC))

    def test_absent_values_fall_back_to_now_floored(self):
        for absent in (None, "", "None", "  "):
            with self.subTest(absent=absent):
                self.assertEqual(resolve_snapshot_at(absent, now=NOW),
                                 datetime(2026, 10, 1, 14, tzinfo=UTC))

    def test_result_is_utc(self):
        self.assertEqual(resolve_snapshot_at("2026-10-01T16:00:00+02:00", now=NOW).tzinfo, timezone.utc)


class TestIsBackfill(unittest.TestCase):

    def test_current_hour_is_not_backfill(self):
        self.assertFalse(is_backfill(datetime(2026, 10, 1, 14, tzinfo=UTC), now=NOW))

    def test_exactly_one_hour_old_is_not_backfill(self):
        snapshot_at = datetime(2026, 10, 1, 14, tzinfo=UTC)
        self.assertFalse(is_backfill(snapshot_at, now=snapshot_at + timedelta(hours=1)))

    def test_older_than_one_hour_is_backfill(self):
        self.assertTrue(is_backfill(datetime(2026, 10, 1, 13, tzinfo=UTC), now=NOW))


if __name__ == "__main__":
    unittest.main()
