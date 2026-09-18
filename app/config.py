import os
from functools import lru_cache

from app.kafka_settings import get_kafka_settings


@lru_cache
def get_settings() -> dict[str, str | bool]:
    posthog_key = os.getenv("POSTHOG_KEY", "")
    posthog_host = os.getenv("POSTHOG_HOST") or "https://us.i.posthog.com"
    kafka = get_kafka_settings()
    domain = os.getenv(
        "CDSW_DOMAIN",
        "ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work",
    )
    consumer_url = os.getenv(
        "CONSUMER_URL",
        f"https://datapulse-spark-consumer.{domain}",
    ).rstrip("/")
    monitoring_url = os.getenv(
        "MONITORING_URL",
        os.getenv("GRAFANA_ROOT_URL", f"https://datapulse-mon-7rrlwp.{domain}"),
    ).rstrip("/")

    project_id = os.getenv("DATAPULSE_PROJECT_ID", "awc-demo")
    capture_endpoint = os.getenv("DATAPULSE_CAPTURE_ENDPOINT", "/v1/capture")

    return {
        "posthog_key": posthog_key,
        "posthog_host": posthog_host,
        "posthog_enabled": bool(posthog_key),
        "kafka_enabled": kafka.enabled,
        "kafka_ready": kafka.is_ready(),
        "consumer_url": consumer_url,
    "monitoring_url": monitoring_url,
    "docs_base_url": os.getenv("CLOUDERA_DOCS_BASE_URL", "https://docs-beta.cloudera.com/"),
    "project_id": project_id,
        "capture_endpoint": capture_endpoint,
        "app_title": "Cloudera Anywhere Cloud — DataPulse Demo",
    }
