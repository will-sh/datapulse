"""CDP Base cluster settings for Spark + Iceberg (Hadoop catalog, no Hive)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


DEFAULT_CLUSTER_HOST = "ccycloud-1.wxiao-732.root.comops.site"
DEFAULT_ICEBERG_JAR = (
    "/opt/cloudera/parcels/CDH-7.3.2-1.cdh7.3.2.p30000.83076434/lib/iceberg/"
    "iceberg-spark-runtime-3.5_2.12-1.10.1.7.3.2.30000-26.jar"
)
DEFAULT_WAREHOUSE = "hdfs:///user/systest/iceberg-warehouse"
DEFAULT_CATALOG = "local"
DEFAULT_DATABASE = "datapulse"
DEFAULT_TABLE = "events"
DEFAULT_HDFS_USER = "systest"


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class CdpBaseSettings:
    cluster_host: str
    hdfs_namenode: str
    yarn_rm: str
    hdfs_user: str
    catalog: str
    database: str
    table: str
    warehouse: str
    iceberg_jar: str
    spark_master: str
    deploy_mode: str

    @property
    def qualified_table(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"

    def spark_catalog_configs(self) -> dict[str, str]:
        catalog = self.catalog
        return {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog}.type": "hadoop",
            f"spark.sql.catalog.{catalog}.warehouse": self.warehouse,
        }


@lru_cache
def get_cdp_base_settings() -> CdpBaseSettings:
    host = _env("CDP_BASE_HOST", DEFAULT_CLUSTER_HOST)
    return CdpBaseSettings(
        cluster_host=host,
        hdfs_namenode=_env("CDP_BASE_NN", f"hdfs://{host}:8020"),
        yarn_rm=_env("CDP_BASE_YARN_RM", f"{host}:8032"),
        hdfs_user=_env("CDP_BASE_HDFS_USER", DEFAULT_HDFS_USER),
        catalog=_env("CDP_BASE_ICEBERG_CATALOG", DEFAULT_CATALOG),
        database=_env("CDP_BASE_ICEBERG_DATABASE", DEFAULT_DATABASE),
        table=_env("CDP_BASE_ICEBERG_TABLE", DEFAULT_TABLE),
        warehouse=_env("CDP_BASE_ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE),
        iceberg_jar=_env("CDP_BASE_ICEBERG_JAR", DEFAULT_ICEBERG_JAR),
        spark_master=_env("CDP_BASE_SPARK_MASTER", "yarn"),
        deploy_mode=_env("CDP_BASE_SPARK_DEPLOY_MODE", "client"),
    )
