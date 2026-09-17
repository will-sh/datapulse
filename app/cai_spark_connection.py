from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from app.lakehouse_settings import LakehouseSettings, get_lakehouse_settings
from app.spark_connect_env import prepare_spark_connect


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def resolve_data_connection_name() -> str:
    """Return the CAI project data connection name, if configured."""
    return _env("CAI_SPARK_DATA_CONNECTION") or _env("CDSW_DATA_CONNECTION")


def _ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _fetch_project_data_connection(connection_name: str) -> dict[str, Any]:
    """Resolve project data-connection metadata without Knox WebSSO."""
    inline = _env("CAI_SPARK_DATA_CONNECTION_INFO")
    if inline:
        connection = json.loads(inline)
        if connection.get("name") == connection_name:
            return connection
        raise RuntimeError(
            f"CAI_SPARK_DATA_CONNECTION_INFO name {connection.get('name')!r} "
            f"does not match {connection_name!r}"
        )

    api_key = _env("CDSW_APIV2_KEY") or _env("CAI_KEY")
    if not api_key:
        raise RuntimeError(
            "CDSW_APIV2_KEY is required to resolve project data connections inside CAI jobs"
        )

    project_id = _env("CAI_PROJECT_NUMERIC_ID") or _env("CDSW_PROJECT_ID") or "3"
    base = _env("CAI_BASE") or _env("CDSW_DOMAIN") or "https://ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work"
    url = f"{base.rstrip('/')}/api/v1/projects/{project_id}/data-connections"
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=60, context=_ssl_context()) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(
            f"failed to list project data connections: HTTP {exc.code} {body[:300]}"
        ) from exc

    for item in payload.get("projectDataConnectionList", []):
        if item.get("name") == connection_name:
            return item
    raise RuntimeError(
        f"project data connection {connection_name!r} not found in project {project_id}"
    )


def _normalize_connection_details(connection: dict[str, Any]) -> dict[str, Any]:
    """Return a detail payload compatible with cml.data_v1 lookup helpers."""
    details = dict(connection)
    workspace_id = details.get("workspaceConnectionId")
    if details.get("id") is None and workspace_id is not None:
        details["id"] = int(workspace_id)
    return details


def _patch_requests_ssl() -> None:
    """spark_connect.zip bundles requests without CAI pod trust roots."""
    try:
        import requests
    except ImportError:
        return

    if getattr(requests.Session.request, "_datapulse_no_verify", False):
        return

    original = requests.Session.request

    def request(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("verify", False)
        return original(self, method, url, **kwargs)

    request._datapulse_no_verify = True  # type: ignore[attr-defined]
    requests.Session.request = request  # type: ignore[method-assign]


def _patch_cml_connection_lookup(connection: dict[str, Any]) -> None:
    """Avoid Knox WebSSO inside cml.data_v1 by serving cached project metadata."""
    import cml.data_v1.data as cml_data

    details = _normalize_connection_details(connection)
    connection_id = details.get("id") or details.get("workspaceConnectionId")
    connection_name = details.get("name")

    def get_project_dataconnections() -> list[dict[str, Any]]:
        return [details]

    def _get_project_dataconnection_by_id(dataconnection_id, *args, **kwargs):  # type: ignore[no-untyped-def]
        if str(dataconnection_id) == str(connection_id):
            return details
        raise RuntimeError(f"unknown project data connection id {dataconnection_id!r}")

    def _get_project_dataconnection(dataconnection_name, *args, **kwargs):  # type: ignore[no-untyped-def]
        if dataconnection_name == connection_name:
            return details
        raise RuntimeError(f"unknown project data connection {dataconnection_name!r}")

    cml_data.get_project_dataconnections = get_project_dataconnections
    cml_data._get_project_dataconnection_by_id = _get_project_dataconnection_by_id
    cml_data._get_project_dataconnection = _get_project_dataconnection


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


def _platform_iceberg_jars() -> str:
    jars: list[str] = []
    for root in (Path("/opt/spark/optional-11b"), Path("/opt/spark/optional")):
        if root.is_dir():
            jars.extend(str(path) for path in sorted(root.glob("*.jar")))
    return ",".join(jars)


def _build_spark_connect_session(
    settings: LakehouseSettings,
    *,
    external_dir: str,
) -> object:
    """Build a remote Spark session using CAI data-connection metadata."""
    connect_url = prepare_spark_connect()
    hadoop_conf_dir = _env("HADOOP_CONF_DIR") or "/home/cdsw/hadoop_config_dir"
    warehouse = external_dir or settings.warehouse
    catalog = settings.catalog

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("datapulse-kafka-to-iceberg")
        .remote(connect_url)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hive")
        .config(f"spark.sql.catalog.{catalog}.uri", settings.hive_metastore_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.executorEnv.HADOOP_CONF_DIR", hadoop_conf_dir)
        .config("spark.hadoop.iceberg.engine.hive.enabled", "true")
    )

    platform_jars = _platform_iceberg_jars()
    if platform_jars:
        builder = builder.config("spark.jars", platform_jars)

    ozone_host = _env("OZONE_HOST", "lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work")
    if ozone_host and warehouse.startswith("s3a://"):
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", ozone_host)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    if settings.ozone_filesystems:
        builder = builder.config("spark.kerberos.access.hadoopFileSystems", settings.ozone_filesystems)

    print(f"Opening Spark Connect session via data connection: {connect_url}")
    return builder.getOrCreate()


def create_spark_session_from_data_connection(
    connection_name: str,
    *,
    settings: LakehouseSettings | None = None,
) -> tuple[object, LakehouseSettings]:
    """Build a Spark session using a synced CAI Spark Data Lake connection."""
    os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
    ssl._create_default_https_context = ssl._create_unverified_context
    _patch_requests_ssl()

    settings = settings or get_lakehouse_settings()
    connection = _fetch_project_data_connection(connection_name)
    external_dir = str(connection.get("connectionInfo", {}).get("dataLakeExternalDir", "")).strip()
    if external_dir:
        os.environ.setdefault("DATALAKE_DIRECTORY", external_dir)
        os.environ.setdefault("ICEBERG_WAREHOUSE", external_dir)

    print(
        f"Using CAI Spark data connection: {connection_name!r} "
        f"dataLakeExternalDir={external_dir or '(missing)'}"
    )

    try:
        spark = _build_spark_connect_session(settings, external_dir=external_dir)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Failed to open CAI data connection {connection_name!r} via Spark Connect: {exc}"
        ) from exc

    _configure_iceberg_catalog(spark, settings)
    status = _hive_site_status()
    print(
        "Data connection Spark session ready: "
        f"version={spark.version} "
        f"hadoop_conf_dir={status['hadoop_conf_dir']!r} "
        f"hive_site_exists={status['hive_site_exists']}"
    )
    return spark, settings
