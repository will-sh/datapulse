import json
import logging
import os
import subprocess
from typing import Any

from app.kafka_settings import KafkaSettings, get_kafka_settings

logger = logging.getLogger(__name__)

PROPERTIES_TEMPLATE = """bootstrap.servers={bootstrap_servers}
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.token.endpoint.url={token_url}
sasl.oauthbearer.client.id={client_id}
sasl.oauthbearer.client.secret={client_secret}
sasl.oauthbearer.client.credentials.client.id={client_id}
sasl.oauthbearer.client.credentials.client.secret={client_secret}
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{client_id}" clientSecret="{client_secret}" ssl.truststore.location=oauth-ca.crt ssl.truststore.type=PEM;
ssl.truststore.location=kafka-ca.crt
ssl.truststore.type=PEM
"""


def write_client_properties(settings: KafkaSettings) -> None:
    settings.config_dir.mkdir(parents=True, exist_ok=True)
    content = PROPERTIES_TEMPLATE.format(
        bootstrap_servers=settings.bootstrap_servers,
        token_url=settings.token_url,
        client_id=settings.client_id,
        client_secret=settings.client_secret,
    )
    settings.properties_path.write_text(content, encoding="utf-8")


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

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        logger.error("Kafka publish failed: %s", detail)
        raise RuntimeError(detail)
