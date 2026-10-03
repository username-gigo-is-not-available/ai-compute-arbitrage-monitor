from pyspark.sql.types import (
    ArrayType, StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
)

COMPUTE_OFFERS_SILVER_SCHEMA = StructType(
    [
        # SNAPSHOT (ADR-019)
        StructField("snapshot_at", TimestampType(), nullable=False),
        # IDs
        StructField("offer_id", IntegerType(), nullable=False),
        StructField("machine_id", IntegerType(), nullable=False),
        StructField("host_id", IntegerType(), nullable=False),
        # TYPES
        StructField("offer_type", StringType(), nullable=False),
        # PRICES
        StructField("total_price_usd_per_hr", DoubleType(), nullable=False),
        StructField("gpu_price_usd_per_hr", DoubleType(), nullable=False),
        StructField("minimum_bid_price_usd", DoubleType(), nullable=False),
        StructField("storage_cost_usd_per_hr", DoubleType(), nullable=False),
        StructField("network_upload_cost_usd_per_gbit", DoubleType(), nullable=False),
        StructField("network_download_cost_usd_per_gbit", DoubleType(), nullable=False),
        StructField("deep_learning_score_per_usd", DoubleType(), nullable=True),
        # GPU
        StructField("gpu_architecture", StringType(), nullable=False),
        StructField("gpu_model_name", StringType(), nullable=False),
        StructField("gpu_memory_mb", DoubleType(), nullable=False),
        StructField("gpu_tdp_watts", DoubleType(), nullable=False),
        StructField("number_of_gpus", IntegerType(), nullable=False),
        StructField("gpu_fraction_of_machine", DoubleType(), nullable=True),
        StructField("gpu_ids", ArrayType(LongType()), nullable=True),
        StructField("gpu_max_cuda_version_supported", DoubleType(), nullable=True),
        StructField("gpu_tflops", DoubleType(), nullable=True),
        StructField("gpu_bandwidth_gbytes_per_sec", DoubleType(), nullable=True),
        # CPU
        StructField("cpu_architecture", StringType(), nullable=False),
        StructField("cpu_model_name", StringType(), nullable=True),
        StructField("number_of_cpu_cores", DoubleType(), nullable=True),
        StructField("cpu_clock_speed_ghz", DoubleType(), nullable=True),
        # RAM
        StructField("ram_mb", DoubleType(), nullable=False),
        # DISK
        StructField("disk_model_name", StringType(), nullable=True),
        StructField("disk_space_gb", DoubleType(), nullable=False),
        StructField("disk_bandwidth_mbytes_per_sec", DoubleType(), nullable=False),
        # PCIe
        StructField("pcie_generation", DoubleType(), nullable=True),
        StructField("pcie_bandwidth_gbytes_per_sec", DoubleType(), nullable=True),
        # INTERNET
        StructField("network_download_mbits_per_sec", DoubleType(), nullable=False),
        StructField("network_upload_mbits_per_sec", DoubleType(), nullable=False),
        # OTHER METRICS
        StructField("reliability_score", DoubleType(), nullable=True),
        StructField("deep_learning_score", DoubleType(), nullable=True),
        # LOCATION
        StructField("geolocation", StringType(), nullable=True),
        # FLAGS
        StructField("verification_flag", StringType(), nullable=False),
        StructField("rentable_flag", BooleanType(), nullable=False),
        StructField("rented_flag", BooleanType(), nullable=False),
    ])
