#!/usr/bin/env python3
"""Repeatable Kafka → Iceberg ingestion on AWC (CAI Trino micro-batch path).

CSA Flink remains blocked (Kafka OAuth classloader + Lakehouse datalake binding).
This CLI uses the proven CAI path: Kafka CLI OAuth → Trino INSERT micro-batches.

Examples:
  export CONSOLE_PASSWORD=awc-admin-password  # Trino OAuth (optional if creds in external.properties)
  python3 scripts/ingest_kafka_iceberg.py probe-kafka
  python3 scripts/ingest_kafka_iceberg.py probe-trino
  python3 scripts/ingest_kafka_iceberg.py run
  python3 scripts/ingest_kafka_iceberg.py verify
  python3 scripts/ingest_kafka_iceberg.py e2e --publish
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_TOPIC = "datapulse-events"
DEFAULT_TABLE = "iceberg.datapulse.events"


def _load_kafka_creds_from_properties() -> None:
    """Populate KAFKA_CLIENT_ID/SECRET from config/kafka/external.properties when unset."""
    if os.getenv("KAFKA_CLIENT_ID") and os.getenv("KAFKA_CLIENT_SECRET"):
        return
    props_path = Path(os.getenv("KAFKA_CONFIG_DIR", "config/kafka")) / "external.properties"
    if not props_path.is_file():
        return
    props: dict[str, str] = {}
    for line in props_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    os.environ.setdefault("KAFKA_CLIENT_ID", props.get("sasl.oauthbearer.client.id", ""))
    os.environ.setdefault("KAFKA_CLIENT_SECRET", props.get("sasl.oauthbearer.client.secret", ""))
    os.environ.setdefault(
        "KAFKA_TOKEN_URL",
        props.get(
            "sasl.oauthbearer.token.endpoint.url",
            "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
        ),
    )


def _trino_env() -> None:
    _load_kafka_creds_from_properties()
    # Pin target table for this pipeline (ignore CSA Flink probe env leftovers).
    os.environ["TRINO_CATALOG"] = os.getenv("INGEST_TRINO_CATALOG", "iceberg")
    os.environ["ICEBERG_DATABASE"] = os.getenv("INGEST_ICEBERG_DATABASE", "datapulse")
    os.environ["ICEBERG_TABLE"] = os.getenv("INGEST_ICEBERG_TABLE", "events")
    os.environ.setdefault(
        "TRINO_HOST",
        "lakehouse-bp-556b64.cldr-csk-lakehouse.a70735.test.cldr.work",
    )
    os.environ.setdefault("TRINO_PORT", "443")
    os.environ.setdefault("TRINO_VERIFY_SSL", "false")


def cmd_probe_kafka(args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "verify_kafka_cli_consumer.py"),
        "--max-messages",
        str(args.max_messages),
        "--timeout",
        str(args.timeout),
    ]
    return subprocess.call(cmd, cwd=ROOT)


def cmd_probe_trino(_args: argparse.Namespace) -> int:
    _trino_env()
    from app.trino_lakehouse import TrinoOAuthClient, get_trino_settings

    client = TrinoOAuthClient()
    info = client.info()
    settings = get_trino_settings()
    print(json.dumps({"trino": info, "qualified_table": settings.qualified_table}, indent=2))
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    _trino_env()
    from app.trino_lakehouse import verify_via_trino

    result = verify_via_trino(limit=args.limit)
    count = result["count"]["rows"][0][0]
    print(json.dumps(result, indent=2))
    print(f"\n{result['qualified_table']}: {count} rows")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    if not os.getenv("CAI_KEY") and not os.getenv("CDSW_APIV2_KEY"):
        print("CAI_KEY (or CDSW_APIV2_KEY) is required to submit CAI jobs", file=sys.stderr)
        return 1

    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "cai_submit_lakehouse_job.py"),
        "ensure-and-run",
        "--mode",
        "trino-ingest",
        "--env",
        f"TRINO_INGEST_MAX_BATCHES={args.max_batches}",
        "--env",
        f"TRINO_INGEST_BATCH_SIZE={args.batch_size}",
        "--env",
        f"TRINO_INGEST_START_MODE={args.start_mode}",
    ]
    if args.no_wait:
        cmd.append("--no-wait")
    return subprocess.call(cmd, cwd=ROOT)


def cmd_e2e(args: argparse.Namespace) -> int:
    _trino_env()
    from app.trino_lakehouse import verify_via_trino

    before = verify_via_trino(limit=1)["count"]["rows"][0][0]
    print(f"Before: {DEFAULT_TABLE} = {before} rows")

    if args.publish:
        pub = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "publish_test_event.py"), "--name", "ingest_e2e_probe"],
            cwd=ROOT,
            check=False,
        )
        if pub.returncode != 0:
            return pub.returncode

    run_rc = cmd_run(args)
    if run_rc != 0:
        return run_rc

    after = verify_via_trino(limit=3)["count"]["rows"][0][0]
    print(f"After:  {DEFAULT_TABLE} = {after} rows (delta {after - before})")
    if args.publish and after <= before:
        print("Warning: row count did not increase after publish+ingest", file=sys.stderr)
        return 1
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    _trino_env()
    from app.trino_lakehouse import get_trino_settings, verify_via_trino

    settings = get_trino_settings()
    result = verify_via_trino(limit=1)
    count = result["count"]["rows"][0][0]
    checkpoint = Path(
        os.getenv(
            "TRINO_INGEST_CHECKPOINT",
            "config/lakehouse/.checkpoints/trino-ingest-offset.json",
        )
    )
    payload = {
        "path": "cai-trino-micro-batch",
        "topic": os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC),
        "qualified_table": settings.qualified_table,
        "row_count": count,
        "checkpoint_exists": checkpoint.is_file(),
        "checkpoint_path": str(checkpoint),
    }
    if checkpoint.is_file():
        payload["checkpoint"] = json.loads(checkpoint.read_text(encoding="utf-8"))
    print(json.dumps(payload, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_kafka = sub.add_parser("probe-kafka", help="Read 1 message from CSM Kafka via OAuth CLI")
    p_kafka.add_argument("--max-messages", type=int, default=1)
    p_kafka.add_argument("--timeout", type=int, default=30)

    sub.add_parser("probe-trino", help="Trino /v1/info via Console OAuth")

    p_verify = sub.add_parser("verify", help="SELECT count(*) and sample from iceberg.datapulse.events")
    p_verify.add_argument("--limit", type=int, default=3)

    p_run = sub.add_parser("run", help="Submit CAI trino-ingest micro-batch job")
    p_run.add_argument("--max-batches", type=int, default=20)
    p_run.add_argument("--batch-size", type=int, default=50)
    p_run.add_argument(
        "--start-mode",
        default="checkpoint",
        choices=("checkpoint", "earliest", "latest"),
        help="checkpoint=use CAI project checkpoint file; earliest/latest only when checkpoint empty",
    )
    p_run.add_argument("--no-wait", action="store_true")

    p_e2e = sub.add_parser("e2e", help="verify count → optional publish → run ingest → verify count")
    p_e2e.add_argument("--publish", action="store_true", help="Publish one test event before ingest")
    p_e2e.add_argument("--max-batches", type=int, default=10)
    p_e2e.add_argument("--batch-size", type=int, default=50)
    p_e2e.add_argument("--start-mode", default="checkpoint", choices=("checkpoint", "earliest", "latest"))
    p_e2e.add_argument("--no-wait", action="store_true")

    sub.add_parser("status", help="Row count + Trino ingest checkpoint summary")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    handlers = {
        "probe-kafka": cmd_probe_kafka,
        "probe-trino": cmd_probe_trino,
        "verify": cmd_verify,
        "run": cmd_run,
        "e2e": cmd_e2e,
        "status": cmd_status,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
