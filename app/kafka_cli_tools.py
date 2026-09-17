"""Minimal Kafka CLI helpers for CAI jobs (OAuth SASL, no JVM consumer in Python)."""

from __future__ import annotations

import os
import re
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from app.kafka_client_properties import write_external_properties


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def kafka_home() -> Path:
    explicit = _env("KAFKA_HOME")
    if explicit:
        return Path(explicit)
    version = _env("KAFKA_VERSION", "3.9.0")
    scala = _env("KAFKA_SCALA", "2.13")
    return Path.home() / ".cache" / "kafka" / f"kafka_{scala}-{version}"


def ensure_kafka_cli() -> Path:
    """Return path to kafka-console-consumer.sh, downloading the CLI if needed."""
    home = kafka_home()
    consumer = home / "bin" / "kafka-console-consumer.sh"
    if consumer.is_file():
        os.environ.setdefault("KAFKA_HOME", str(home))
        return consumer

    version = _env("KAFKA_VERSION", "3.9.0")
    scala = _env("KAFKA_SCALA", "2.13")
    archive_url = f"https://archive.apache.org/dist/kafka/{version}/kafka_{scala}-{version}.tgz"
    home.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Kafka {version} CLI to {home} ...")
    with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        urllib.request.urlretrieve(archive_url, tmp_path)
        with tarfile.open(tmp_path) as archive:
            archive.extractall(home.parent)
        tmp_path.unlink(missing_ok=True)

    if not consumer.is_file():
        raise RuntimeError(f"Kafka CLI not found after download: {consumer}")
    os.environ["KAFKA_HOME"] = str(home)
    return consumer


def kafka_config_dir() -> Path:
    return Path(_env("KAFKA_CONFIG_DIR", "config/kafka")).expanduser()


def prepare_kafka_properties() -> Path:
    config_dir = kafka_config_dir()
    client_id = _env("KAFKA_CLIENT_ID")
    client_secret = _env("KAFKA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("KAFKA_CLIENT_ID and KAFKA_CLIENT_SECRET are required")

    return write_external_properties(
        config_dir,
        bootstrap_servers=_env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        ),
        token_url=_env(
            "KAFKA_TOKEN_URL",
            "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
        ),
        client_id=client_id,
        client_secret=client_secret,
        offset_reset=_env("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    )


def kafka_cli_env() -> dict[str, str]:
    env = os.environ.copy()
    token_url = _env(
        "KAFKA_TOKEN_URL",
        "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
    )
    env["KAFKA_OPTS"] = f"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={token_url}"
    return env


def list_topic_end_offsets(topic: str | None = None) -> dict[int, int]:
    """Return partition -> log end offset (next append position)."""
    topic = topic or _env("KAFKA_TOPIC", "datapulse-events")
    home = kafka_home()
    run_class = home / "bin" / "kafka-run-class.sh"
    config_dir = kafka_config_dir()
    prepare_kafka_properties()
    cmd = [
        str(run_class),
        "kafka.tools.GetOffsetShell",
        "--bootstrap-server",
        _env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        ),
        "--command-config",
        "external.properties",
        "--topic",
        topic,
        "--time",
        "-1",
    ]
    result = subprocess.run(
        cmd,
        cwd=str(config_dir),
        env=kafka_cli_env(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        raise RuntimeError(f"GetOffsetShell failed: {detail[:500]}")

    offsets: dict[int, int] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.count(":") < 2:
            continue
        _, partition_text, offset_text = line.rsplit(":", 2)
        offsets[int(partition_text)] = int(offset_text)
    return offsets


def list_topic_partitions(topic: str | None = None) -> list[int]:
    topic = topic or _env("KAFKA_TOPIC", "datapulse-events")
    home = kafka_home()
    topics_sh = home / "bin" / "kafka-topics.sh"
    config_dir = kafka_config_dir()
    prepare_kafka_properties()

    cmd = [
        str(topics_sh),
        "--bootstrap-server",
        _env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        ),
        "--command-config",
        "external.properties",
        "--describe",
        "--topic",
        topic,
    ]
    result = subprocess.run(
        cmd,
        cwd=str(config_dir),
        env=kafka_cli_env(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        raise RuntimeError(f"kafka-topics --describe failed: {detail[:500]}")

    partitions: set[int] = set()
    for line in result.stdout.splitlines():
        match = re.search(r"Partition:\s*(\d+)", line)
        if match:
            partitions.add(int(match.group(1)))
    if not partitions:
        raise RuntimeError(f"no partitions found for topic {topic!r}")
    return sorted(partitions)


def read_partition_messages(
    *,
    topic: str,
    partition: int,
    start_offset: int,
    max_messages: int,
    timeout_ms: int,
) -> list[tuple[int, str]]:
    """Read up to max_messages from a single partition starting at start_offset."""
    consumer = ensure_kafka_cli()
    config_dir = kafka_config_dir()
    prepare_kafka_properties()

    cmd = [
        str(consumer),
        "--bootstrap-server",
        _env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        ),
        "--consumer.config",
        "external.properties",
        "--topic",
        topic,
        "--partition",
        str(partition),
        "--offset",
        str(start_offset),
        "--max-messages",
        str(max_messages),
        "--timeout-ms",
        str(timeout_ms),
    ]
    result = subprocess.run(
        cmd,
        cwd=str(config_dir),
        env=kafka_cli_env(),
        capture_output=True,
        text=True,
        timeout=max(30, timeout_ms // 1000 + 30),
        check=False,
    )
    if result.returncode not in {0, 1}:
        detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
        raise RuntimeError(
            f"kafka-console-consumer failed for partition={partition} offset={start_offset}: "
            f"{detail[:500]}"
        )

    messages: list[tuple[int, str]] = []
    for index, line in enumerate(result.stdout.splitlines()):
        text = line.strip()
        if text:
            messages.append((start_offset + index, text))
    return messages
