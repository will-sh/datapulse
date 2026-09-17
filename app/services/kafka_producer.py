import json
import logging
import os
import subprocess
import time
from typing import Any

from app.kafka_client_properties import write_external_properties as write_kafka_external_properties
from app.kafka_settings import KafkaSettings, get_kafka_settings
from app.metrics import KAFKA_PUBLISH_DURATION

logger = logging.getLogger(__name__)


def write_client_properties(settings: KafkaSettings) -> None:
    write_kafka_external_properties(
        settings.config_dir,
        bootstrap_servers=settings.bootstrap_servers,
        token_url=settings.token_url,
        client_id=settings.client_id,
        client_secret=settings.client_secret,
    )


def publish_event(event: dict[str, Any], settings: KafkaSettings | None = None) -> None:
    settings = settings or get_kafka_settings()
    if not settings.is_ready():
        raise RuntimeError("; ".join(settings.readiness_issues()))

    write_client_properties(settings)
    producer_script = settings.producer_script
    if producer_script is None:
        raise RuntimeError("Kafka CLI producer script is not available")

    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    env = os.environ.copy()
    env["KAFKA_OPTS"] = (
        f"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={settings.allowed_urls}"
    )

    started = time.perf_counter()
    result = subprocess.run(
        [
            str(producer_script),
            "--bootstrap-server",
            settings.bootstrap_servers,
            "--producer.config",
            str(settings.properties_path.name),
            "--topic",
            settings.topic,
        ],
        input=payload.encode("utf-8"),
        capture_output=True,
        cwd=settings.config_dir,
        env=env,
        timeout=45,
        check=False,
    )
    KAFKA_PUBLISH_DURATION.observe(time.perf_counter() - started)

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        logger.error("Kafka publish failed: %s", detail)
        raise RuntimeError(detail)
