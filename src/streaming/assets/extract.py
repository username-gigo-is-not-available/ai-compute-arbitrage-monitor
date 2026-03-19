from pyspark.sql import functions as F, Column, DataFrame


def infer_manufacturer(df: DataFrame, model_name_col: str, manufacturer_name_col) -> DataFrame:
    """Resolve missing manufacturer_name by extracting it from the name column.
      Known manufacturers are derived dynamically from existing non-null values.
      """
    manufacturers: list[str] = [
        row[manufacturer_name_col]
        for row in df.select(manufacturer_name_col)
        .filter(F.col(manufacturer_name_col).isNotNull() & (F.col(manufacturer_name_col) != ""))
        .distinct()
        .collect()
    ]

    conditions: Column = F.lit(None).cast("string")
    for manufacturer_name in manufacturers:
        conditions: Column = F.when(
            F.col(model_name_col).ilike(f"%{manufacturer_name}%"), manufacturer_name
        ).otherwise(conditions)

    return (
        df
        .withColumn(
            manufacturer_name_col,
            F.when(
                F.col(manufacturer_name_col).isNull() | (F.col(manufacturer_name_col) == ""),
                conditions
            ).otherwise(F.col(manufacturer_name_col))
        )
    )


def add_processed_at_column(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", F.to_utc_timestamp(F.current_timestamp(), 'UTC'))
