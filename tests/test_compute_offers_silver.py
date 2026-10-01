"""Silver idempotency for compute_offers on snapshot_at (ADR-019).

Run directly:   uv run python tests/test_compute_offers_silver.py
Under pytest:   uv run --extra dev pytest tests/test_compute_offers_silver.py
"""

from __future__ import annotations

import os
import sys
import unittest

from pyspark.sql import SparkSession

from refine.sources.compute_offers import deduplicate_compute_offers
from refine.write_strategy import IncrementalAppend


def _offers(session: SparkSession, rows: str):
    # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
    return session.sql(
        "SELECT offer_id, offer_type, CAST(price AS DOUBLE) AS price, CAST(snap AS TIMESTAMP) AS snapshot_at, "
        "CAST(fetched AS TIMESTAMP) AS ingested_at "
        f"FROM VALUES {rows} AS t(offer_id, offer_type, price, snap, fetched)"
    )


class TestComputeOffersSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        _offers(cls.session, "(1, 'bid', 0.10, '2026-10-01 14:00:00', '2026-10-01 14:00:05')") \
            .createOrReplaceTempView("silver_offers")

    def test_watermark_ignores_rerun_of_already_refined_hour(self):
        bronze = _offers(self.session,
                         "(1, 'bid', 0.11, '2026-10-01 14:00:00', '2026-10-01 14:40:00'), "
                         "(1, 'bid', 0.12, '2026-10-01 15:00:00', '2026-10-01 15:00:04')")
        result = IncrementalAppend(column="snapshot_at").read_filter(bronze, self.session, "silver_offers")
        self.assertEqual([r["price"] for r in result.collect()], [0.12])

    def test_retry_replaces_whole_earlier_attempt_not_merges(self):
        # Attempt 1 saw offers 1 and 2; the retry saw offers 1 and 3. The snapshot is the retry's view only.
        batch = _offers(self.session,
                        "(1, 'bid', 0.11, '2026-10-01 15:00:00', '2026-10-01 15:00:04'), "
                        "(2, 'bid', 0.20, '2026-10-01 15:00:00', '2026-10-01 15:00:04'), "
                        "(1, 'bid', 0.12, '2026-10-01 15:00:00', '2026-10-01 15:05:09'), "
                        "(3, 'bid', 0.30, '2026-10-01 15:00:00', '2026-10-01 15:05:09'), "
                        "(4, 'bid', 0.40, '2026-10-01 16:00:00', '2026-10-01 16:00:03')")
        rows = {(r["offer_id"], r["price"]) for r in deduplicate_compute_offers(batch).collect()}
        self.assertEqual(rows, {(1, 0.12), (3, 0.30), (4, 0.40)})


if __name__ == "__main__":
    unittest.main()
