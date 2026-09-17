from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime

from app.cdp_base_settings import get_cdp_base_settings


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _print_json(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def discover_environment() -> int:
    settings = get_cdp_base_settings()
    _print_json(
        "cdp-base-discover",
        {
            "cluster_host": settings.cluster_host,
            "hdfs_namenode": settings.hdfs_namenode,
            "yarn_rm": settings.yarn_rm,
            "hdfs_user": settings.hdfs_user,
            "catalog": settings.catalog,
            "catalog_type": "hadoop",
            "warehouse": settings.warehouse,
            "qualified_table": settings.qualified_table,
            "iceberg_jar": settings.iceberg_jar,
            "spark_master": settings.spark_master,
            "deploy_mode": settings.deploy_mode,
            "hadoop_conf_dir": _env("HADOOP_CONF_DIR"),
            "spark_conf_dir": _env("SPARK_CONF_DIR"),
            "note": "No Hive Metastore — use hadoop catalog only",
        },
    )
    return 0


def _build_spark_session():
    settings = get_cdp_base_settings()
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("datapulse-cdp-base-iceberg-smoke")
    for key, value in settings.spark_catalog_configs().items():
        builder = builder.config(key, value)
    return builder.getOrCreate(), settings


def smoke_test() -> int:
    spark, settings = _build_spark_session()
    db = settings.database
    table = settings.qualified_table
    catalog = settings.catalog

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          id BIGINT,
          event_type STRING,
          ts TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table} VALUES
          (1, 'login', current_timestamp()),
          (2, 'logout', current_timestamp())
        """
    )
    rows = spark.sql(f"SELECT id, event_type, ts FROM {table} ORDER BY id").collect()
    count = spark.table(table).count()
    payload = {
        "spark_version": spark.version,
        "qualified_table": table,
        "warehouse": settings.warehouse,
        "row_count": count,
        "rows": [row.asDict(recursive=True) for row in rows],
        "verified_at": datetime.now(UTC).isoformat(),
    }
    spark.stop()
    _print_json("cdp-base-smoke", payload)
    if count < 2:
        raise RuntimeError(f"expected at least 2 rows, got {count}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CDP Base Spark + Iceberg smoke (Hadoop catalog)")
    parser.add_argument(
        "mode",
        choices=("discover", "smoke"),
        help="discover env or run CREATE/INSERT/SELECT smoke test",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == "discover":
        return discover_environment()
    if args.mode == "smoke":
        return smoke_test()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
