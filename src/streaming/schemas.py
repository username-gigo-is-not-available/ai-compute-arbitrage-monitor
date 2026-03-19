from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, \
    BooleanType, DateType

META_COLUMNS_SCHEMA = [
    StructField("ingested_at", TimestampType(), nullable=True),
    StructField("processed_at", TimestampType(), nullable=True),

]
BASE_HARDWARE_SPECIFICATION_SCHEMA = META_COLUMNS_SCHEMA + [

    StructField("open_db_id", StringType(), nullable=False),
    StructField("model_name", StringType(), nullable=False),
    StructField("manufacturer_name", StringType(), nullable=True),
    StructField("tdp_watts", IntegerType(), nullable=False),
]

GPU_SPECIFICATION_SCHEMA = StructType(
    BASE_HARDWARE_SPECIFICATION_SCHEMA + [
    StructField("chipset_manufacturer", StringType(), nullable=False),
    StructField("chipset_name", StringType(), nullable=False),
    StructField("memory_gb", IntegerType(), nullable=False),
    StructField("memory_type", StringType(), nullable=False),
    StructField("memory_bus_bits", IntegerType(), nullable=True),
    StructField("core_base_clock_mhz", IntegerType(), nullable=True),
    StructField("core_boost_clock_mhz", IntegerType(), nullable=True),
    StructField("interface_type", StringType(), nullable=True),
])

CPU_SPECIFICATION_SCHEMA = StructType(BASE_HARDWARE_SPECIFICATION_SCHEMA + [
    StructField("socket_type", StringType(), nullable=True),
    StructField("number_of_cores", IntegerType(), nullable=False),
    StructField("number_of_threads", IntegerType(), nullable=True),
    StructField("base_clock_ghz", DoubleType(), nullable=True),
    StructField("boost_clock_ghz", DoubleType(), nullable=True),
    StructField("microarchitecture_type", StringType(), nullable=True),
])

ELECTRICITY_TARIFF_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
    StructField("tariff_type", StringType(), nullable=False),
    StructField("price_per_kwh_mkd", FloatType(), nullable=False),
    StructField("valid_from", DateType(), nullable=True),
])

ELECTRICITY_TARIFF_SCHEDULE_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
        StructField("tariff_type", StringType(), nullable=False),
        StructField("day_of_week", IntegerType(), nullable=False),
        StructField("start_hour", IntegerType(), nullable=False),
        StructField("end_hour", IntegerType(), nullable=False),
    ]
)


COMPUTE_OFFER_SCHEMA = StructType(
    META_COLUMNS_SCHEMA +
    [
    # IDs
    StructField("instance_id", IntegerType(), nullable=False),
    # TIME
    StructField("timestamp", TimestampType(), nullable=False),
    # PRICES
    StructField("price_usd_per_hour", FloatType(), nullable=False),
    # GPU
    StructField("gpu_architecture", StringType(), nullable=False),
    StructField("gpu_model_name", StringType(), nullable=False),
    StructField("gpu_memory_mb", FloatType(), nullable=False),
    StructField("gpu_max_power_watts", FloatType(), nullable=False),
    StructField("number_of_gpus", IntegerType(), nullable=False),
    StructField("max_cuda_version_supported", FloatType(), nullable=True),
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
    StructField("timestamp", TimestampType(), nullable=False),
])