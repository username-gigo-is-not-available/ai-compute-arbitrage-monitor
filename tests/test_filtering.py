from refine.assets.filtering import deduplicate_keep_latest


def test_keeps_row_with_latest_order_column_per_key(spark):
    # SQL VALUES instead of createDataFrame(list) so no Python worker is spawned (Windows).
    df = spark.sql(
        "SELECT k, v, CAST(at AS TIMESTAMP) AS ingested_at FROM VALUES "
        "('a', 1.0, '2026-09-30 10:00:00'), "
        "('a', 2.0, '2026-09-30 12:00:00'), "
        "('a', 3.0, '2026-09-30 11:00:00'), "
        "('b', 9.0, '2026-09-30 10:00:00') "
        "AS t(k, v, at)"
    )
    result = deduplicate_keep_latest(df, columns=["k"], order_by="ingested_at")
    assert {r["k"]: r["v"] for r in result.collect()} == {"a": 2.0, "b": 9.0}
    assert result.columns == df.columns
