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
        StructField("price_mkd_per_kwh", FloatType(), nullable=False),
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
        StructField("offer_id", IntegerType(), nullable=False),
        StructField("machine_id", IntegerType(), nullable=False),
        StructField("host_id", IntegerType(), nullable=False),
        # TYPES
        StructField("offer_type", StringType(), nullable=False),
        # PRICES
        StructField("total_price_usd_per_hr", FloatType(), nullable=False),
        StructField("gpu_price_usd_per_hr", FloatType(), nullable=False),
        StructField("minimum_bid_price_usd", FloatType(), nullable=False),
        StructField("storage_cost_usd_per_hr", FloatType(), nullable=False),
        StructField("network_upload_cost_usd_per_gbit", FloatType(), nullable=False),
        StructField("network_download_cost_usd_per_gbit", FloatType(), nullable=False),
        StructField("deep_learning_score_per_usd", FloatType(), nullable=True),
        # GPU
        StructField("gpu_architecture", StringType(), nullable=False),
        StructField("gpu_model_name", StringType(), nullable=False),
        StructField("gpu_memory_mb", FloatType(), nullable=False),
        StructField("gpu_tdp_watts", FloatType(), nullable=False),
        StructField("number_of_gpus", IntegerType(), nullable=False),
        StructField("gpu_max_cuda_version_supported", FloatType(), nullable=True),
        StructField("gpu_tflops", FloatType(), nullable=True),
        StructField("gpu_bandwidth_gbytes_per_sec", FloatType(), nullable=True),
        # CPU
        StructField("cpu_architecture", StringType(), nullable=False),
        StructField("cpu_model_name", StringType(), nullable=True),
        StructField("number_of_cpu_cores", FloatType(), nullable=True),
        StructField("cpu_clock_speed_ghz", FloatType(), nullable=True),
        # RAM
        StructField("ram_mb", FloatType(), nullable=False),
        # DISK
        StructField("disk_model_name", StringType(), nullable=True),
        StructField("disk_space_gb", FloatType(), nullable=False),
        StructField("disk_bandwidth_mbytes_per_sec", FloatType(), nullable=False),
        # PCIe
        StructField("pcie_generation", FloatType(), nullable=True),
        StructField("pcie_bandwidth_gbytes_per_sec", FloatType(), nullable=True),
        # INTERNET
        StructField("network_download_mbits_per_sec", FloatType(), nullable=False),
        StructField("network_upload_mbits_per_sec", FloatType(), nullable=False),
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
        StructField("value", FloatType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
    ])
