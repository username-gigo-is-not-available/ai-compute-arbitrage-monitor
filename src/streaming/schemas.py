from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, \
    BooleanType, DateType

META_COLUMNS_SCHEMA = [
    StructField("ingested_at", TimestampType(), nullable=True),
    StructField("processed_at", TimestampType(), nullable=True),

]

ELECTRICITY_TARIFF_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("tariff_description", StringType(), nullable=False),
        StructField("price_per_kwh_mkd", FloatType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
    ])

ELECTRICITY_TARIFF_SCHEDULE_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("tariff_type", StringType(), nullable=False),
        StructField("day_of_week", IntegerType(), nullable=False),
        StructField("start_hour", IntegerType(), nullable=False),
        StructField("end_hour", IntegerType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False)
    ]
)

COMPUTE_OFFER_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        # IDs
        StructField("instance_id", IntegerType(), nullable=False),
        # PRICES
        StructField("total_price_usd_per_hour", FloatType(), nullable=False),
        StructField("gpu_price_usd_per_hour", FloatType(), nullable=False),
        StructField("deep_learning_score_per_dollar", FloatType(), nullable=True),
        # GPU
        StructField("gpu_architecture", StringType(), nullable=False),
        StructField("gpu_model_name", StringType(), nullable=False),
        StructField("gpu_memory_mb", FloatType(), nullable=False),
        StructField("gpu_tdp_watts", FloatType(), nullable=False),
        StructField("number_of_gpus", IntegerType(), nullable=False),
        StructField("gpu_max_cuda_version_supported", FloatType(), nullable=True),
        StructField("gpu_tflops", FloatType(), nullable=True),
        # CPU
        StructField("cpu_architecture", StringType(), nullable=False),
        StructField("cpu_model_name", StringType(), nullable=True),
        StructField("number_of_cpu_cores", FloatType(), nullable=True),
        # RAM
        StructField("ram_mb", FloatType(), nullable=False),
        # DISK
        StructField("disk_model_name", StringType(), nullable=True),
        StructField("disk_space_gb", FloatType(), nullable=False),
        # INTERNET
        StructField("network_download_mbps", FloatType(), nullable=False),
        StructField("network_upload_mbps", FloatType(), nullable=False),
        # OTHER METRICS
        StructField("reliability_score", FloatType(), nullable=True),
        StructField("deep_learning_score", FloatType(), nullable=True),
        # LOCATION
        StructField("geolocation", StringType(), nullable=True),
        # FLAGS
        StructField("verification_flag", StringType(), nullable=False),
        StructField("rentable_flag", BooleanType(), nullable=False),
        StructField("rented_flag", BooleanType(), nullable=False),
    ])

EXCHANGE_RATE_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("from_currency", StringType(), nullable=False),
        StructField("to_currency", StringType(), nullable=False),
        StructField("rate", FloatType(), nullable=False),
        StructField("rate_timestamp", TimestampType(), nullable=False),
    ])
