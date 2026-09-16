from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


DEFAULT_HMS_HOST = "hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work"
DEFAULT_HMS_PORT = "9083"
DEFAULT_ICEBERG_CATALOG = "iceberg_catalog"
DEFAULT_ICEBERG_DATABASE = "datapulse"
DEFAULT_ICEBERG_TABLE = "events"
DEFAULT_SPARK_VERSION = "3.5.4"
DEFAULT_SCALA_VERSION = "2.12"
DEFAULT_ICEBERG_VERSION = "1.5.2"


@dataclass(frozen=True)
class LakehouseSettings:
    catalog: str
    database: str
    table: str
    hive_metastore_uri: str
    warehouse: str
    spark_version: str
    scala_version: str
    iceberg_version: str
    hadoop_conf_dir: Path | None
    ozone_filesystems: str
    checkpoint_dir: Path

    @property
    def qualified_table(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"

    @property
    def iceberg_package(self) -> str:
        major_minor = ".".join(self.spark_version.split(".")[:2])
        return (
            f"org.apache.iceberg:iceberg-spark-runtime-{major_minor}_"
            f"{self.scala_version}:{self.iceberg_version}"
        )

    @property
    def kafka_package(self) -> str:
        return (
            f"org.apache.spark:spark-sql-kafka-0-10_{self.scala_version}:"
            f"{self.spark_version}"
        )

    @property
    def spark_packages(self) -> str:
        return f"{self.kafka_package},{self.iceberg_package}"


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _parse_hive_site(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return {}

    props: dict[str, str] = {}
    for prop in root.findall(".//property"):
        name_el = prop.find("name")
        value_el = prop.find("value")
        if name_el is not None and value_el is not None and name_el.text:
            props[name_el.text.strip()] = (value_el.text or "").strip()
    return props


def discover_hive_site(conf_dir: Path | None = None) -> dict[str, str]:
    candidates: list[Path] = []
    if conf_dir is not None:
        candidates.append(conf_dir / "hive-site.xml")
    for env_name in ("HADOOP_CONF_DIR", "HIVE_CONF_DIR", "SPARK_CONF_DIR"):
        raw = _env(env_name)
        if raw:
            candidates.append(Path(raw).expanduser() / "hive-site.xml")
    candidates.extend(
        [
            Path("/etc/hadoop/conf/hive-site.xml"),
            Path("/etc/spark/conf/hive-site.xml"),
            Path("/opt/cloudera/parcels/CDH/etc/hive/conf.dist/hive-site.xml"),
        ]
    )

    for path in candidates:
        props = _parse_hive_site(path)
        if props:
            return {"source": str(path), **props}
    return {}


def resolve_hive_metastore_uri(hive_site: dict[str, str]) -> str:
    explicit = _env("HIVE_METASTORE_URI") or _env("ICEBERG_HMS_URI")
    if explicit:
        return explicit

    discovered = hive_site.get("hive.metastore.uris", "")
    if discovered:
        return discovered

    host = _env("HIVE_METASTORE_HOST", DEFAULT_HMS_HOST)
    port = _env("HIVE_METASTORE_PORT", DEFAULT_HMS_PORT)
    return f"thrift://{host}:{port}"


def resolve_warehouse(hive_site: dict[str, str]) -> str:
    explicit = _env("ICEBERG_WAREHOUSE") or _env("LAKEHOUSE_WAREHOUSE")
    if explicit:
        return explicit

    for key in ("spark.sql.warehouse.dir", "hive.metastore.warehouse.dir"):
        value = hive_site.get(key, "")
        if value:
            return value

    ozone_host = _env("OZONE_HOST", "lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work")
    volume = _env("OZONE_VOLUME", "s3v")
    bucket = _env("OZONE_BUCKET", "warehouse")
    prefix = _env("OZONE_WAREHOUSE_PREFIX", "datapulse")
    return f"s3a://{volume}/{bucket}/{prefix}" if ozone_host else ""


def resolve_ozone_filesystems(warehouse: str) -> str:
    explicit = _env("SPARK_YARN_ACCESS_HADOOP_FILESYSTEMS") or _env("OZONE_FILESYSTEMS")
    if explicit:
        return explicit

    if warehouse.startswith("ofs://"):
        return warehouse.split("/", 3)[2] if warehouse.count("/") >= 2 else warehouse
    if warehouse.startswith("s3a://"):
        return _env("OZONE_HOST", "lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work")
    return _env("OZONE_HOST", DEFAULT_HMS_HOST)


@lru_cache
def get_lakehouse_settings() -> LakehouseSettings:
    conf_raw = _env("HADOOP_CONF_DIR")
    conf_dir = Path(conf_raw).expanduser() if conf_raw else None
    hive_site = discover_hive_site(conf_dir)
    warehouse = resolve_warehouse(hive_site)
    checkpoint_raw = _env("LAKEHOUSE_CHECKPOINT_DIR", "config/lakehouse/.checkpoints/kafka-to-iceberg")

    return LakehouseSettings(
        catalog=_env("ICEBERG_CATALOG", DEFAULT_ICEBERG_CATALOG),
        database=_env("ICEBERG_DATABASE", DEFAULT_ICEBERG_DATABASE),
        table=_env("ICEBERG_TABLE", DEFAULT_ICEBERG_TABLE),
        hive_metastore_uri=resolve_hive_metastore_uri(hive_site),
        warehouse=warehouse,
        spark_version=_env("SPARK_VERSION", DEFAULT_SPARK_VERSION),
        scala_version=_env("SPARK_SCALA_VERSION", DEFAULT_SCALA_VERSION),
        iceberg_version=_env("ICEBERG_VERSION", DEFAULT_ICEBERG_VERSION),
        hadoop_conf_dir=conf_dir,
        ozone_filesystems=resolve_ozone_filesystems(warehouse),
        checkpoint_dir=Path(checkpoint_raw).expanduser(),
    )
