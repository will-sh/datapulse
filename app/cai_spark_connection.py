from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from app.lakehouse_settings import LakehouseSettings, get_lakehouse_settings


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


def _patch_cml_connection_lookup(connection: dict[str, Any]) -> None:
    """Avoid Knox WebSSO inside cml.data_v1 by serving cached project metadata."""
    import cml.data_v1.data as cml_data

    cached = {"projectDataConnectionList": [connection]}

    def get_project_dataconnections() -> dict[str, Any]:
        return cached

    cml_data.get_project_dataconnections = get_project_dataconnections


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
    os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
    ssl._create_default_https_context = ssl._create_unverified_context

    try:
        import cml.data_v1 as cmldata
    except ImportError as exc:
        raise RuntimeError(
            "cml.data_v1 is unavailable in this runtime. Attach the Spark Connect runtime "
            "addon (sparkconnect354-731-26) and ensure the project data connection is synced."
        ) from exc

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

    _patch_cml_connection_lookup(connection)
    try:
        conn = cmldata.get_connection(connection_name)
        spark = conn.get_spark_session()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Failed to open CAI data connection {connection_name!r}: {exc}"
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
