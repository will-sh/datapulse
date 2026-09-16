from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import UTC, datetime
from pathlib import Path

from app.lakehouse_settings import discover_hive_site, get_lakehouse_settings
from app.spark_stream.kafka_stream import _kafka_options


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _print_json(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def discover_environment() -> int:
    settings = get_lakehouse_settings()
    hive_site = discover_hive_site(settings.hadoop_conf_dir)
    summary = {
        "lakehouse": {
            "catalog": settings.catalog,
            "database": settings.database,
            "table": settings.table,
            "qualified_table": settings.qualified_table,
            "hive_metastore_uri": settings.hive_metastore_uri,
            "warehouse": settings.warehouse,
            "ozone_filesystems": settings.ozone_filesystems,
            "spark_packages": settings.spark_packages,
        },
        "kafka": {
            "bootstrap_servers": _env(
                "KAFKA_BOOTSTRAP_SERVERS",
                "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
            ),
            "topic": _env("KAFKA_TOPIC", "datapulse-events"),
            "client_id_set": bool(_env("KAFKA_CLIENT_ID")),
        },
        "hive_site_source": hive_site.get("source"),
        "hive_site_keys": sorted(k for k in hive_site if k != "source"),
    }
    _print_json("discover", summary)
    return 0


def _apply_iceberg_catalog_builder(builder, settings):
    catalog = settings.catalog
    builder = (
        builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hive")
        .config(f"spark.sql.catalog.{catalog}.uri", settings.hive_metastore_uri)
    )
    if settings.warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog}.warehouse", settings.warehouse)
    if settings.ozone_filesystems:
        builder = builder.config("spark.yarn.access.hadoopFileSystems", settings.ozone_filesystems)
    return builder


def _apply_ozone_s3a_builder(builder, settings):
    ozone_host = _env("OZONE_HOST", "lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work")
    if not ozone_host or not settings.warehouse.startswith("s3a://"):
        return builder
    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", ozone_host)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )


def _configure_iceberg_catalog(spark, settings) -> None:
    # Dynamic post-create tweaks only; static catalog configs belong on the builder.
    _ = settings


def _spark_packages(include_kafka: bool = True) -> str:
    settings = get_lakehouse_settings()
    if include_kafka:
        return settings.spark_packages
    return settings.iceberg_package


def _build_lakehouse_spark_session(*, include_kafka: bool = True):
    settings = get_lakehouse_settings()
    allowed_urls = ""
    dist_files = ""
    if include_kafka:
        _, allowed_urls, dist_files = _kafka_options()

    local_master = _env("LAKEHOUSE_SPARK_MASTER") or _env("SPARK_MASTER")
    connect_url = _env("SPARK_REMOTE") or _env("SPARK_CONNECT_URL")
    if not connect_url and not local_master:
        from app.spark_stream.kafka_stream import _spark_connect_url

        connect_url = _spark_connect_url()
    if not connect_url and not local_master:
        raise RuntimeError(
            "Spark session config missing (set LAKEHOUSE_SPARK_MASTER=local[*] or Spark Connect URL)"
        )

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("datapulse-kafka-to-iceberg")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", _spark_packages(include_kafka=include_kafka))
    )
    if local_master:
        builder = builder.master(local_master)
        print(f"Using local Spark master: {local_master}")
    else:
        builder = builder.remote(connect_url)
        print(f"Using Spark Connect: {connect_url}")
        if allowed_urls:
            builder = builder.config("spark.driver.extraJavaOptions", allowed_urls).config(
                "spark.executor.extraJavaOptions", allowed_urls
            )
    if dist_files:
        builder = builder.config("spark.files", dist_files)

    builder = _apply_iceberg_catalog_builder(builder, settings)
    builder = _apply_ozone_s3a_builder(builder, settings)

    spark = builder.getOrCreate()
    _configure_iceberg_catalog(spark, settings)
    return spark, settings


def _create_lakehouse_spark_session(*, include_kafka: bool = True):
    timeout = int(_env("SPARK_SESSION_TIMEOUT_SEC", "180"))
    target = _env("LAKEHOUSE_SPARK_MASTER") or _env("SPARK_REMOTE") or _env("SPARK_CONNECT_URL") or "spark"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_build_lakehouse_spark_session, include_kafka=include_kafka)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError as exc:
            raise RuntimeError(f"Spark session timed out after {timeout}s (target={target})") from exc


def _ensure_table(spark, settings) -> None:
    retries = int(_env("HMS_RETRY_COUNT", "3"))
    delay_sec = int(_env("HMS_RETRY_DELAY_SEC", "5"))
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.catalog}.{settings.database}")
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {settings.qualified_table} (
                  event_id STRING,
                  project_id STRING,
                  event_name STRING,
                  properties_json STRING,
                  event_timestamp BIGINT,
                  source STRING,
                  user_id STRING,
                  anonymous_id STRING,
                  session_id STRING,
                  page_path STRING,
                  kafka_partition INT,
                  kafka_offset BIGINT,
                  kafka_timestamp TIMESTAMP,
                  ingested_at TIMESTAMP,
                  raw_payload STRING
                )
                USING iceberg
                """
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"HMS DDL attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                import time

                time.sleep(delay_sec)
    raise RuntimeError(f"failed to create Iceberg table via HMS after {retries} attempts: {last_error}")


def _parse_events_df(spark, batch_df):
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

    event_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("project_id", StringType()),
            StructField("name", StringType()),
            StructField("properties", MapType(StringType(), StringType())),
            StructField("timestamp", LongType()),
            StructField("source", StringType()),
            StructField("user_id", StringType()),
            StructField("anonymous_id", StringType()),
            StructField("session_id", StringType()),
            StructField("page_path", StringType()),
        ]
    )

    parsed = batch_df.select(
        F.col("message").alias("raw_payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("event_timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("message"), event_schema).alias("event"),
    )

    return parsed.select(
        F.col("event.event_id").alias("event_id"),
        F.col("event.project_id").alias("project_id"),
        F.col("event.name").alias("event_name"),
        F.to_json(F.col("event.properties")).alias("properties_json"),
        F.col("event.timestamp").alias("event_timestamp"),
        F.col("event.source").alias("source"),
        F.col("event.user_id").alias("user_id"),
        F.col("event.anonymous_id").alias("anonymous_id"),
        F.col("event.session_id").alias("session_id"),
        F.col("event.page_path").alias("page_path"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.col("raw_payload"),
    )


def _write_batch_to_iceberg(batch_df, batch_id: int, settings) -> None:
    if batch_df.isEmpty():
        print(f"batch {batch_id}: empty")
        return

    rows = _parse_events_df(batch_df.sparkSession, batch_df)
    count = rows.count()
    rows.writeTo(settings.qualified_table).append()
    print(f"batch {batch_id}: appended {count} rows to {settings.qualified_table}")


def bootstrap_table() -> int:
    spark, settings = _create_lakehouse_spark_session(include_kafka=False)
    print(f"Spark session ready: version={spark.version} url={_env('SPARK_CONNECT_URL')}")
    _ensure_table(spark, settings)
    _print_json(
        "bootstrap",
        {
            "spark_version": spark.version,
            "table": settings.qualified_table,
            "warehouse": settings.warehouse,
            "hive_metastore_uri": settings.hive_metastore_uri,
        },
    )
    return 0


def _kafka_source(spark):
    kafka_options, _, _ = _kafka_options()
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .selectExpr(
            "CAST(value AS STRING) AS message",
            "partition",
            "offset",
            "CAST(timestamp AS TIMESTAMP) AS event_timestamp",
        )
    )


def run_batch_ingest(max_batches: int = 1, timeout_sec: int = 600) -> int:
    spark, settings = _create_lakehouse_spark_session()
    _ensure_table(spark, settings)

    source = _kafka_source(spark)
    checkpoint = str(settings.checkpoint_dir / "batch")
    Path(checkpoint).mkdir(parents=True, exist_ok=True)

    for batch_index in range(max_batches):
        query = (
            source.writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint)
            .foreachBatch(lambda batch_df, batch_id: _write_batch_to_iceberg(batch_df, batch_id, settings))
            .trigger(once=True)
            .start()
        )
        query.awaitTermination(timeout=timeout_sec)
        print(f"completed batch {batch_index + 1}/{max_batches}")

    verify_ingest(spark, settings)
    return 0


def run_stream_ingest(processing_interval: str = "30 seconds") -> int:
    spark, settings = _create_lakehouse_spark_session()
    _ensure_table(spark, settings)

    source = _kafka_source(spark)
    checkpoint = str(settings.checkpoint_dir / "stream")
    Path(checkpoint).mkdir(parents=True, exist_ok=True)

    query = (
        source.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(lambda batch_df, batch_id: _write_batch_to_iceberg(batch_df, batch_id, settings))
        .trigger(processingTime=processing_interval)
        .start()
    )
    print(f"streaming to {settings.qualified_table}; checkpoint={checkpoint}")
    query.awaitTermination()
    return 0


def verify_ingest(spark=None, settings=None) -> int:
    owns_spark = spark is None
    if spark is None:
        spark, settings = _create_lakehouse_spark_session()
    assert settings is not None

    count = spark.table(settings.qualified_table).count()
    sample = spark.sql(
        f"""
        SELECT event_name, user_id, anonymous_id, event_timestamp, ingested_at
        FROM {settings.qualified_table}
        ORDER BY ingested_at DESC
        LIMIT 10
        """
    ).collect()
    _print_json(
        "verify",
        {
            "table": settings.qualified_table,
            "row_count": count,
            "sample": [row.asDict(recursive=True) for row in sample],
            "verified_at": datetime.now(UTC).isoformat(),
        },
    )
    if owns_spark:
        spark.stop()
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka -> Iceberg lakehouse ingest job")
    parser.add_argument(
        "mode",
        choices=("discover", "bootstrap", "batch", "stream", "verify", "spark-probe"),
        help="discover env, create table, one-shot batch ingest, continuous stream, verify table, or spark connect probe",
    )
    parser.add_argument("--max-batches", type=int, default=1)
    parser.add_argument("--timeout-sec", type=int, default=600)
    parser.add_argument("--processing-interval", default="30 seconds")
    return parser.parse_args(argv)


def spark_probe() -> int:
    spark, settings = _create_lakehouse_spark_session(include_kafka=False)
    _print_json(
        "spark-probe",
        {
            "spark_version": spark.version,
            "spark_connect_url": _env("SPARK_CONNECT_URL"),
            "qualified_table": settings.qualified_table,
        },
    )
    spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == "discover":
        return discover_environment()
    if args.mode == "spark-probe":
        return spark_probe()
    if args.mode == "bootstrap":
        return bootstrap_table()
    if args.mode == "batch":
        return run_batch_ingest(max_batches=args.max_batches, timeout_sec=args.timeout_sec)
    if args.mode == "stream":
        return run_stream_ingest(processing_interval=args.processing_interval)
    if args.mode == "verify":
        return verify_ingest()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
