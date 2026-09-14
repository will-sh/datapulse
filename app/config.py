import os
from functools import lru_cache

from app.kafka_settings import get_kafka_settings


@lru_cache
def get_settings() -> dict[str, str | bool]:
    posthog_key = os.getenv("POSTHOG_KEY") or os.getenv("NEXT_PUBLIC_POSTHOG_KEY", "")
    posthog_host = (
        os.getenv("POSTHOG_HOST")
        or os.getenv("NEXT_PUBLIC_POSTHOG_HOST")
        or "https://us.i.posthog.com"
    )
    kafka = get_kafka_settings()

    return {
        "posthog_key": posthog_key,
        "posthog_host": posthog_host,
        "posthog_enabled": bool(posthog_key),
        "kafka_enabled": kafka.enabled,
        "kafka_ready": kafka.is_ready(),
        "app_title": "DataPulse — 用户行为分析 Demo",
    }
