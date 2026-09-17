#!/usr/bin/env python3
"""Verify Kafka OAuth CLI consumer can read at least one message."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.kafka_client_properties import write_external_properties
from app.kafka_settings import get_kafka_settings


def ensure_kafka_cli(kafka_home: Path, version: str = "3.9.0", scala: str = "2.13") -> Path:
    consumer = kafka_home / "bin" / "kafka-console-consumer.sh"
    if consumer.is_file():
        return consumer
    archive_url = f"https://archive.apache.org/dist/kafka/{version}/kafka_{scala}-{version}.tgz"
    kafka_home.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Kafka CLI from {archive_url} ...")
    with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        urllib.request.urlretrieve(archive_url, tmp_path)
        with tarfile.open(tmp_path) as archive:
            archive.extractall(kafka_home.parent)
        tmp_path.unlink(missing_ok=True)
    if not consumer.is_file():
        raise RuntimeError(f"Kafka CLI not found after download: {consumer}")
    return consumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--group", default="datapulse-cli-verify")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_kafka_settings()
    kafka_home = settings.kafka_home or Path.home() / ".cache" / "kafka" / "kafka_2.13-3.9.0"
    consumer = ensure_kafka_cli(kafka_home)

    missing = []
    if not settings.client_id:
        missing.append("KAFKA_CLIENT_ID is missing")
    if not settings.client_secret:
        missing.append("KAFKA_CLIENT_SECRET is missing")
    if not settings.kafka_ca_path.is_file():
        missing.append(f"missing {settings.kafka_ca_path}")
    if not settings.oauth_ca_path.is_file():
        missing.append(f"missing {settings.oauth_ca_path}")
    if missing:
        print("Kafka not ready:", "; ".join(missing), file=sys.stderr)
        return 1

    write_external_properties(
        settings.config_dir,
        bootstrap_servers=settings.bootstrap_servers,
        token_url=settings.token_url,
        client_id=settings.client_id,
        client_secret=settings.client_secret,
    )
    env = os.environ.copy()
    env["KAFKA_OPTS"] = f"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={settings.token_url}"

    cmd = [
        str(consumer),
        "--bootstrap-server",
        settings.bootstrap_servers,
        "--consumer.config",
        str(settings.properties_path.name),
        "--topic",
        settings.topic,
        "--group",
        args.group,
        "--from-beginning",
        "--max-messages",
        str(args.max_messages),
    ]
    print("Running:", " ".join(cmd), f"(cwd={settings.config_dir})")
    result = subprocess.run(
        cmd,
        cwd=settings.config_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=args.timeout,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        print("Kafka CLI consumer failed:", detail, file=sys.stderr)
        return 1

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    print(f"OK: read {len(lines)} message(s)")
    if lines:
        print("sample:", lines[0][:240])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
