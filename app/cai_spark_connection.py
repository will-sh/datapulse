from __future__ import annotations

import os
from pathlib import Path

from app.lakehouse_settings import LakehouseSettings, get_lakehouse_settings


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def resolve_data_connection_name() -> str:
    """Return the CAI project data connection name, if configured."""
    return _env("CAI_SPARK_DATA_CONNECTION") or _env("CDSW_DATA_CONNECTION")


def _hive_site_status() -> dict[str, object]:
    hadoop_conf_dir = _env("HADOOP_CONF_DIR") or "/home/cdsw/hadoop_config_dir"
    hive_site = Path(hadoop_conf_dir) / "hive-site.xml"
    return {
        "hadoop_conf_dir": hadoop_conf_dir,
        "hive_site_path": str(hive_site),
        "hive_site_exists": hive_site.is_file(),
    }


def _configure_iceberg_catalog(spark, settings: LakehouseSettings) -> None:
    catalog = settings.catalog
    if spark.conf.get(f"spark.sql.catalog.{catalog}"):
        return

    spark.conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalog}.type", "hive")
    if settings.warehouse:
        spark.conf.set(f"spark.sql.catalog.{catalog}.warehouse", settings.warehouse)


def create_spark_session_from_data_connection(
    connection_name: str,
    *,
    settings: LakehouseSettings | None = None,
) -> tuple[object, LakehouseSettings]:
    """Build a Spark session using a synced CAI Spark Data Lake connection."""
    import cml.data_v1 as cmldata

    settings = settings or get_lakehouse_settings()
    print(f"Using CAI Spark data connection: {connection_name!r}")
    conn = cmldata.get_connection(connection_name)
    spark = conn.get_spark_session()
    _configure_iceberg_catalog(spark, settings)
    status = _hive_site_status()
    print(
        "Data connection Spark session ready: "
        f"version={spark.version} "
        f"hadoop_conf_dir={status['hadoop_conf_dir']!r} "
        f"hive_site_exists={status['hive_site_exists']}"
    )
    return spark, settings
