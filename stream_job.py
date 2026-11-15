#!/usr/bin/env python3
"""
PySpark Structured Streaming Pipeline
Topic 11 — Kafka & PySpark Structured Streaming Ingestion Pipeline

Reads clickstream JSON events from Kafka topic 'klik', parses schema,
applies a 2-minute event-time watermark, and aggregates event counts per
1-minute tumbling window grouped by category.

Supports:
  - console sink (outputMode: update)
  - parquet sink (outputMode: append, checkpoint: chk/klik, output: hasil/klik_per_menit)
"""

import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def create_spark_session():
    """Configure and build the SparkSession."""
    spark = (
        SparkSession.builder
        .appName("klik-stream-pipeline")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_streaming_pipeline(spark, starting_offsets=None):
    """Define the ingestion, schema parsing, watermarking, and window aggregation."""
    if starting_offsets is None:
        starting_offsets = os.getenv("STARTING_OFFSETS", "earliest")

    # 1. Define JSON schema: user_id (int), kategori (string), waktu (string)
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("kategori", StringType(), True),
        StructField("waktu", StringType(), True),
    ])

    # 2. Ingest from Kafka topic 'klik'
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "klik")
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 3. Parse JSON payload and cast 'waktu' to TimestampType
    parsed_stream = (
        raw_stream
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("waktu", to_timestamp(col("waktu")))
    )

    # 4. Apply 2-minute watermark and 1-minute tumbling window grouped by kategori
    windowed_counts = (
        parsed_stream
        .withWatermark("waktu", "2 minutes")
        .groupBy(
            window(col("waktu"), "1 minute"),
            col("kategori")
        )
        .count()
    )

    return windowed_counts


def run_stream_job():
    # Parse CLI arguments
    mode = "console"
    if len(sys.argv) > 1:
        arg_mode = sys.argv[1].lower()
        if arg_mode in ("console", "parquet"):
            mode = arg_mode
        elif arg_mode in ("-h", "--help", "help"):
            print("Usage: python stream_job.py [console|parquet] [timeout_seconds]")
            sys.exit(0)
        else:
            print(f"[ERROR] Unknown mode '{sys.argv[1]}'. Must be 'console' or 'parquet'.")
            print("Usage: python stream_job.py [console|parquet] [timeout_seconds]")
            sys.exit(1)

    # Optional timeout for automated verification/testing
    timeout = None
    if len(sys.argv) > 2 and sys.argv[2].isdigit():
        timeout = int(sys.argv[2])

    print(f"[Spark] Starting Structured Streaming Pipeline in '{mode}' mode...")
    if timeout:
        print(f"[Spark] Timeout set to {timeout} seconds.")

    spark = create_spark_session()
    windowed_counts = build_streaming_pipeline(spark)

    query = None
    if mode == "console":
        query = (
            windowed_counts.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .start()
        )
        print("[Spark] Streaming to console (update mode). Waiting for batches...")

    elif mode == "parquet":
        output_path = os.getenv("OUTPUT_PATH", "hasil/klik_per_menit")
        checkpoint_path = os.getenv("CHECKPOINT_PATH", "chk/klik")
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(checkpoint_path, exist_ok=True)

        query = (
            windowed_counts.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .start()
        )
        print(f"[Spark] Streaming to Parquet at '{output_path}' with checkpoint '{checkpoint_path}' (append mode)...")

    try:
        if timeout:
            query.awaitTermination(timeout=timeout)
            print(f"[Spark] Timeout of {timeout}s reached. Stopping query.")
            query.stop()
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[Spark] Interrupted by user. Terminating streaming query...")
        if query:
            query.stop()
    finally:
        spark.stop()
        print("[Spark] SparkSession stopped successfully.")


if __name__ == "__main__":
    run_stream_job()
