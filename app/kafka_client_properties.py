from __future__ import annotations

from pathlib import Path

EXTERNAL_PROPERTIES_TEMPLATE = """bootstrap.servers={bootstrap_servers}
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
auto.offset.reset={offset_reset}
enable.auto.commit=true
"""


def write_external_properties(
    config_dir: Path,
    *,
    bootstrap_servers: str,
    token_url: str,
    client_id: str,
    client_secret: str,
    offset_reset: str = "earliest",
) -> Path:
    config_dir = config_dir.expanduser()
    kafka_ca = config_dir / "kafka-ca.crt"
    oauth_ca = config_dir / "oauth-ca.crt"
    missing = [path for path in (kafka_ca, oauth_ca) if not path.is_file()]
    if missing:
        raise RuntimeError(
            "Kafka TLS certs missing: "
            + ", ".join(str(path) for path in missing)
            + f" (KAFKA_CONFIG_DIR={config_dir})"
        )

    content = EXTERNAL_PROPERTIES_TEMPLATE.format(
        bootstrap_servers=bootstrap_servers,
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        offset_reset=offset_reset,
    )
    path = config_dir / "external.properties"
    config_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path
