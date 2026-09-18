#!/usr/bin/env python3
"""Set up a DataPulse analytics dashboard in CDV using the official CDV MCP Server tools.

Uses Knox WebSSO session cookies (required for this AWC CDV deployment) instead of
CDV_API_KEY, which is not accepted under SSO auth on this instance.

Workflow (per https://github.com/cloudera/CDV-MCP-Server):
  1. Discover connections / workspaces
  2. Sync aggregates from Trino iceberg.datapulse.events
  3. Stage CSV in CDV (datapulse-local sqlite connection)
  4. Create dataset + chart visuals via create_smart_visual
  5. Wrap charts in a dashboard via create_dashboard

Usage:
  python3 scripts/cdv_datapulse_dashboard.py setup
  python3 scripts/cdv_datapulse_dashboard.py status
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
from pathlib import Path

import requests

# Repo root on path for Trino export
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CDV_MCP_ROOT = Path(os.getenv("CDV_MCP_ROOT", "/tmp/CDV-MCP-Server"))
if str(CDV_MCP_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(CDV_MCP_ROOT / "src"))

DEFAULT_CDV_URL = "https://cdv-bp-1-dataviz.cldr-csk-cdv-2.a70735.test.cldr.work"
DEFAULT_KNOX = "https://knox.readygo.a70735.test.cldr.work"
COOKIE_JAR = Path(os.getenv("CDV_COOKIE_JAR", "/tmp/cdv-cookies.txt"))


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def knox_login(cdv_url: str, knox_url: str, user: str, password: str) -> None:
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    target = f"{cdv_url.rstrip('/')}/arc/apps/home"
    subprocess.run(
        [
            "curl",
            "-sk",
            "-c",
            str(COOKIE_JAR),
            "-L",
            "-X",
            "POST",
            "-H",
            f"Authorization: Basic {auth}",
            f"{knox_url.rstrip('/')}/gateway/knox-cdpsso/api/v1/websso?originalUrl={target}",
        ],
        check=True,
        capture_output=True,
    )


def _load_session(cdv_url: str) -> requests.Session:
    session = requests.Session()
    session.verify = False  # AWC test env uses private CA; curl uses -sk
    if not COOKIE_JAR.exists():
        raise RuntimeError(f"Missing cookie jar {COOKIE_JAR}; run knox_login first")
    # Parse Netscape cookie jar manually (simple format from curl)
    for line in COOKIE_JAR.read_text().splitlines():
        if not line or line.startswith("#"):
            if line.startswith("#HttpOnly_"):
                line = line[len("#HttpOnly_") :]
            else:
                continue
        parts = line.split("\t")
        if len(parts) >= 7:
            domain, _, path, secure, _, name, value = parts[:7]
            session.cookies.set(name, value, domain=domain.lstrip("."), path=path)
    session.headers.update({"Accept": "application/json"})
    return session


def patch_cdv_mcp_session(cdv_url: str) -> requests.Session:
    """Patch CDV MCP api_client to use Knox session cookies (SSO deployments)."""
    os.environ.setdefault("CDV_BASE_URL", cdv_url)
    os.environ.setdefault("CDV_API_KEY", "knox-session-placeholder")

    import cdv_mcp_server.tools.api_client as api_client

    session = _load_session(cdv_url)

    def _session_headers() -> dict:
        csrf = session.cookies.get("arccsrftoken", "")
        return {
            "accept": "application/json",
            "Content-Type": "application/json",
            "X-CSRFToken": csrf,
        }

    def _form_headers() -> dict:
        csrf = session.cookies.get("arccsrftoken", "")
        return {
            "accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-CSRFToken": csrf,
        }

    def _url(path: str) -> str:
        return f"{cdv_url.rstrip('/')}/{path.lstrip('/')}"

    def cdv_get(path: str, params: dict | None = None) -> str:
        r = session.get(_url(path), headers=_session_headers(), params=params, timeout=60)
        r.raise_for_status()
        return r.text

    def cdv_post_admin(path: str, body: dict) -> str:
        form = {"data": json.dumps([body])}
        r = session.post(_url(path), headers=_form_headers(), data=form, timeout=60)
        r.raise_for_status()
        return r.text

    def cdv_post_form(path: str, data: dict) -> str:
        try:
            r = session.post(_url(path), headers=_form_headers(), data=data, timeout=60)
            r.raise_for_status()
            return r.text
        except requests.HTTPError as e:
            return json.dumps(
                {
                    "error": str(e),
                    "status_code": e.response.status_code,
                    "detail": e.response.text,
                }
            )

    def cdv_delete(path: str) -> str:
        r = session.delete(_url(path), headers=_session_headers(), timeout=60)
        r.raise_for_status()
        return r.text or json.dumps({"result": "deleted"})

    def get_cdv_session() -> requests.Session | None:
        return session

    api_client.cdv_get = cdv_get
    api_client.cdv_post_admin = cdv_post_admin
    api_client.cdv_post_form = cdv_post_form
    api_client.cdv_delete = cdv_delete
    api_client.get_cdv_session = get_cdv_session
    return session


def export_trino_aggregates_csv() -> tuple[Path, dict]:
    from app.trino_lakehouse import TrinoOAuthClient, get_trino_settings

    # Ensure lakehouse datapulse table (not stale env defaults)
    os.environ.setdefault("TRINO_CATALOG", "iceberg")
    os.environ.setdefault("TRINO_SCHEMA", "datapulse")
    os.environ.setdefault("TRINO_TABLE", "events")
    os.environ.setdefault(
        "TRINO_HOST",
        "lakehouse-bp-556b64.cldr-csk-lakehouse.a70735.test.cldr.work",
    )

    settings = get_trino_settings()
    client = TrinoOAuthClient(settings)
    sql = f"""
    SELECT
      event_name,
      COALESCE(page_path, 'unknown') AS page_path,
      COALESCE(source, 'unknown') AS source,
      COUNT(*) AS event_count
    FROM {settings.qualified_table}
    GROUP BY 1, 2, 3
    ORDER BY event_count DESC
    """
    result = client.execute(sql, catalog=settings.catalog, schema=settings.database)
    columns = [str(c) for c in result.get("columns") or []]
    rows = result.get("rows") or []

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)

    path = Path(tempfile.gettempdir()) / "datapulse_events_agg.csv"
    path.write_text(buf.getvalue())
    meta = {"rows": len(rows), "qualified_table": settings.qualified_table}
    return path, meta


def ensure_sqlite_connection(session: requests.Session, cdv_url: str) -> int:
    from cdv_mcp_server.tools import connections_tools

    conns = json.loads(connections_tools.list_connections())
    for conn in conns:
        if conn.get("name") == "datapulse-local":
            return int(conn["id"])

    body = {
        "name": "datapulse-local",
        "type": "sqlite",
        "info": {"PARAMS": {"FILE": "datapulse_events.sqlite"}},
    }
    created = json.loads(connections_tools.create_connection(body))
    if isinstance(created, list):
        created = created[0]
    return int(created["id"])


def stage_csv_import(session: requests.Session, cdv_url: str, dc_id: int, csv_path: Path) -> dict:
    csrf = session.cookies.get("arccsrftoken", "")
    with csv_path.open("rb") as fh:
        r = session.post(
            f"{cdv_url.rstrip('/')}/arc/datasources/dataimport",
            headers={"X-CSRFToken": csrf},
            files={"datasource_file": (csv_path.name, fh, "text/csv")},
            data={
                "datasource_active_dataconnid": str(dc_id),
                "data-type": "csv",
                "csrfmiddlewaretoken": csrf,
            },
            timeout=120,
        )
    r.raise_for_status()
    m = re.search(r"datasource_id\\u0022: (\d+)", r.text)
    if not m:
        raise RuntimeError("CSV staging failed: could not parse datasource_id from import preview")
    ds_id = m.group(1)
    headers = [
        {"colname": "event_name", "coltype": "String"},
        {"colname": "page_path", "coltype": "String"},
        {"colname": "source", "coltype": "String"},
        {"colname": "event_count", "coltype": "Integer"},
    ]
    payload = {
        "datasource_headers": json.dumps(headers),
        "datasource_tablename": "datapulse_events",
        "datasource_database": "main",
        "datasource_importtype": "csv",
        "datasource_col_delimiter": "comma",
        "datasource_locale": "C",
        "datasource_external_table": "0",
        "datasource_has_header": "1",
        "datasource_skip_bad_rows": "0",
        "datasource_fill_missing_columns": "0",
        "datasource_escapechar": "0",
        "datasource_import_confirmed": "1",
        "csrfmiddlewaretoken": csrf,
    }
    r2 = session.put(
        f"{cdv_url.rstrip('/')}/arc/datasources/dataimport.json/{ds_id}",
        headers={"X-CSRFToken": csrf, "Content-Type": "application/x-www-form-urlencoded"},
        data=payload,
        timeout=120,
    )
    r2.raise_for_status()
    return json.loads(r2.text)


def table_exists(session: requests.Session, cdv_url: str, dc_id: int, table: str) -> bool:
    q = f"SELECT COUNT(*) AS cnt FROM {table}"
    r = session.get(
        f"{cdv_url.rstrip('/')}/arc/apps/dataapi",
        params={"dataconnection_id": dc_id, "query": q, "limit": 1},
        timeout=60,
    )
    if r.status_code != 200:
        return False
    return "no such table" not in r.text.lower()


def ensure_dataset(dc_id: int, table: str, name: str) -> int:
    from cdv_mcp_server.tools import datasets_tools

    datasets = json.loads(datasets_tools.list_datasets())
    for ds in datasets:
        if ds.get("name") == name:
            return int(ds["id"])

    body = {
        "name": name,
        "type": "singletable",
        "detail": table,
        "dc_id": dc_id,
        "description": "Aggregated events synced from Trino iceberg.datapulse.events",
    }
    created = json.loads(datasets_tools.create_dataset(body))
    if isinstance(created, list):
        created = created[0]
    if "error" in created:
        raise RuntimeError(f"create_dataset failed: {created}")
    return int(created["id"])


def build_dashboard(dataset_id: int, workspace_id: int = 1) -> dict:
    from cdv_mcp_server.tools import visuals_tools

    visuals: list[int] = []

    specs = [
        (
            "Top Event Types",
            "trellis-bars",
            [
                {"column_name": "event_name"},
                {"column_name": "event_count", "aggregate_function": "sum"},
            ],
        ),
        (
            "Events by Page Path",
            "trellis-bars",
            [
                {"column_name": "page_path"},
                {"column_name": "event_count", "aggregate_function": "sum"},
            ],
        ),
        (
            "Events by Source",
            "pie",
            [
                {"column_name": "source"},
                {"column_name": "event_count", "aggregate_function": "sum"},
            ],
        ),
        (
            "Events by Source and Type",
            "trellis-groupedbars",
            [
                {"column_name": "event_name"},
                {"column_name": "event_count", "aggregate_function": "sum"},
                {"column_name": "source", "shelf": "color"},
            ],
        ),
    ]

    for title, vtype, columns in specs:
        raw = visuals_tools.create_smart_visual(
            dataset_id=str(dataset_id),
            visual_type=vtype,
            title=title,
            columns=columns,
            workspace_id=workspace_id,
        )
        result = json.loads(raw)
        if "error" in result:
            raise RuntimeError(f"create_smart_visual({title}) failed: {result}")
        visuals.append(int(result["id"]))

    dash_raw = visuals_tools.create_dashboard(
        title="DataPulse Iceberg Analytics",
        workspace_id=workspace_id,
        visual_ids=visuals,
        dataset_id=dataset_id,
        description="Dashboard built from Trino/Iceberg datapulse.events via CDV MCP Server",
    )
    return json.loads(dash_raw)


def cmd_setup(args: argparse.Namespace) -> int:
    cdv_url = _env("CDV_BASE_URL", DEFAULT_CDV_URL)
    knox = _env("KNOX_URL", DEFAULT_KNOX)
    user = _env("CDV_USERNAME", _env("CONSOLE_USER", "admin"))
    password = _env("CDV_PASSWORD", _env("CONSOLE_PASSWORD", "awc-admin-password"))

    print("1/6 Knox login → CDV session")
    knox_login(cdv_url, knox, user, password)
    session = patch_cdv_mcp_session(cdv_url)

    from cdv_mcp_server.tools import connections_tools, workspaces_tools

    print("2/6 Export Trino aggregates")
    csv_path, meta = export_trino_aggregates_csv()
    print(f"    exported {meta['rows']} rows from {meta['qualified_table']} → {csv_path}")

    print("3/6 Ensure datapulse-local sqlite connection")
    dc_id = ensure_sqlite_connection(session, cdv_url)
    print(f"    connection id={dc_id}")

    print("4/6 Stage + confirm CSV import in CDV")
    import_result = stage_csv_import(session, cdv_url, dc_id, csv_path)
    print(f"    staged table={import_result.get('datasource_tablename')} errors={import_result.get('datasource_errors')}")

    table = "main.datapulse_events"
    dataset_id: int | None = None
    if table_exists(session, cdv_url, dc_id, table):
        print("5/6 Create dataset on imported sqlite table")
        dataset_id = ensure_dataset(dc_id, table, "DataPulse Events")
        print(f"    dataset id={dataset_id}")
    else:
        print("5/6 Import table not queryable; falling back to NYC Taxicab samples dataset")
        print("    NOTE: direct CDV→Lakehouse Trino connection validation fails on this cluster.")
        print("    Fix trino-datapulse OAuth/network, or complete CSV import in CDV UI (Data → Import).")
        dataset_id = 1  # NYC Taxicab Rides — numeric demo until sqlite import lands

    print("6/6 Build dashboard via CDV MCP (create_smart_visual + create_dashboard)")
    if dataset_id == 1:
        # Map sample columns to MCP-compatible specs
        from cdv_mcp_server.tools import visuals_tools

        specs = [
            (
                "Rides by Borough (Trino fallback demo)",
                "trellis-bars",
                [
                    {"column_name": "pickup_boro"},
                    {"column_name": "ride_cnt", "aggregate_function": "sum"},
                ],
            ),
            (
                "Rides by Neighborhood",
                "trellis-bars",
                [
                    {"column_name": "pickup_neighborhood"},
                    {"column_name": "ride_cnt", "aggregate_function": "sum"},
                ],
            ),
            (
                "Ride Share by Borough",
                "pie",
                [
                    {"column_name": "pickup_boro"},
                    {"column_name": "ride_cnt", "aggregate_function": "sum"},
                ],
            ),
        ]
        visuals = []
        for title, vtype, columns in specs:
            raw = visuals_tools.create_smart_visual(
                dataset_id="1",
                visual_type=vtype,
                title=title,
                columns=columns,
                workspace_id=1,
            )
            result = json.loads(raw)
            visuals.append(int(result["id"]))
        dash = json.loads(
            visuals_tools.create_dashboard(
                title="DataPulse CDV Dashboard (sample fallback)",
                workspace_id=1,
                visual_ids=visuals,
                dataset_id=1,
                description="Built with CDV MCP; switch to DataPulse Events dataset when Trino/sqlite path is live",
            )
        )
    else:
        dash = build_dashboard(dataset_id)

    dash_id = dash.get("id") or dash.get("visual_id")
    url = dash.get("url") or f"{cdv_url.rstrip('/')}/arc/apps/app/{dash_id}"
    print(json.dumps({"dashboard_id": dash_id, "url": url, "dataset_id": dataset_id}, indent=2))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    cdv_url = _env("CDV_BASE_URL", DEFAULT_CDV_URL)
    knox = _env("KNOX_URL", DEFAULT_KNOX)
    user = _env("CDV_USERNAME", _env("CONSOLE_USER", "admin"))
    password = _env("CDV_PASSWORD", _env("CONSOLE_PASSWORD", "awc-admin-password"))
    knox_login(cdv_url, knox, user, password)
    patch_cdv_mcp_session(cdv_url)

    from cdv_mcp_server.tools import connections_tools, datasets_tools, visuals_tools

    print("Connections:")
    print(json.dumps(json.loads(connections_tools.list_connections()), indent=2))
    print("\nDatasets (datapulse/*):")
    datasets = json.loads(datasets_tools.list_datasets())
    print(json.dumps([d for d in datasets if "datapulse" in d.get("name", "").lower()], indent=2))
    print("\nDashboards in Public workspace:")
    visuals = json.loads(visuals_tools.list_visuals(workspace_id=1))
    print(
        json.dumps(
            [v for v in visuals if "datapulse" in v.get("title", "").lower()],
            indent=2,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CDV DataPulse dashboard setup via official CDV MCP Server")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="Export Trino data and build CDV dashboard")
    sub.add_parser("status", help="Show CDV connections/datasets/dashboards")
    args = parser.parse_args()
    if args.cmd == "setup":
        return cmd_setup(args)
    if args.cmd == "status":
        return cmd_status(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
