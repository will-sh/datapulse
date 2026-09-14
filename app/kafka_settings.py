import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_DIR = BASE_DIR / "config" / "kafka"
DEFAULT_TOKEN_URL = (
    "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token"
)
DEFAULT_BOOTSTRAP = (
    "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443"
)
DEFAULT_TOPIC = "datapulse-events"


@dataclass(frozen=True)
class KafkaSettings:
    enabled: bool
    bootstrap_servers: str
    topic: str
    client_id: str
    client_secret: str
    token_url: str
    config_dir: Path
    kafka_home: Path | None
    allowed_urls: str

    @property
    def properties_path(self) -> Path:
        return self.config_dir / "external.properties"

    @property
    def kafka_ca_path(self) -> Path:
        return self.config_dir / "kafka-ca.crt"

    @property
    def oauth_ca_path(self) -> Path:
        return self.config_dir / "oauth-ca.crt"

    @property
    def producer_script(self) -> Path | None:
        if not self.kafka_home:
            return None
        script = self.kafka_home / "bin" / "kafka-console-producer.sh"
        return script if script.is_file() else None

    def is_ready(self) -> bool:
        if not self.enabled:
            return False
        if not self.client_id or not self.client_secret:
            return False
        if not self.kafka_ca_path.is_file() or not self.oauth_ca_path.is_file():
            return False
        if self.producer_script is None:
            return False
        return True

    def readiness_issues(self) -> list[str]:
        issues: list[str] = []
        if not self.enabled:
            issues.append("KAFKA_ENABLED is false")
        if not self.client_id:
            issues.append("KAFKA_CLIENT_ID is missing")
        if not self.client_secret:
            issues.append("KAFKA_CLIENT_SECRET is missing")
        if not self.kafka_ca_path.is_file():
            issues.append(f"missing {self.kafka_ca_path.name} in {self.config_dir}")
        if not self.oauth_ca_path.is_file():
            issues.append(f"missing {self.oauth_ca_path.name} in {self.config_dir}")
        if self.producer_script is None:
            issues.append("Kafka CLI not found (set KAFKA_HOME or run cai-start script)")
        return issues


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@lru_cache
def get_kafka_settings() -> KafkaSettings:
    config_dir = Path(
        os.getenv("KAFKA_CONFIG_DIR", str(DEFAULT_CONFIG_DIR)),
    ).expanduser()
    kafka_home_raw = os.getenv("KAFKA_HOME", "").strip()
    kafka_home = Path(kafka_home_raw).expanduser() if kafka_home_raw else None
    token_url = os.getenv("KAFKA_TOKEN_URL", DEFAULT_TOKEN_URL).strip()

    return KafkaSettings(
        enabled=_env_bool("KAFKA_ENABLED"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP).strip(),
        topic=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC).strip(),
        client_id=os.getenv("KAFKA_CLIENT_ID", "").strip(),
        client_secret=os.getenv("KAFKA_CLIENT_SECRET", "").strip(),
        token_url=token_url,
        config_dir=config_dir,
        kafka_home=kafka_home,
        allowed_urls=token_url,
    )
