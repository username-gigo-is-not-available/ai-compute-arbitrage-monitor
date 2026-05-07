import json
import os

from pyspark.sql import SparkSession


def initialize_spark() -> SparkSession:
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    with open(creds_path) as f:
        creds = json.load(f)
    return (
        SparkSession.builder
        .appName("ai-compute-arbitrage-monitor")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.type", "USER_CREDENTIALS")
        .config("spark.hadoop.fs.gs.auth.client.id", creds["client_id"])
        .config("spark.hadoop.fs.gs.auth.client.secret", creds["client_secret"])
        .config("spark.hadoop.fs.gs.auth.refresh.token", creds["refresh_token"])
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )