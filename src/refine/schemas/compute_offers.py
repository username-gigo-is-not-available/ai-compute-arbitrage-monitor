from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

from refine.schemas.base import META_COLUMNS_SCHEMA

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

