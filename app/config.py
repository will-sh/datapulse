import os
from functools import lru_cache


@lru_cache
def get_settings() -> dict[str, str | bool]:
    posthog_key = os.getenv("POSTHOG_KEY") or os.getenv("NEXT_PUBLIC_POSTHOG_KEY", "")
    posthog_host = (
        os.getenv("POSTHOG_HOST")
        or os.getenv("NEXT_PUBLIC_POSTHOG_HOST")
        or "https://us.i.posthog.com"
    )

    return {
        "posthog_key": posthog_key,
        "posthog_host": posthog_host,
        "posthog_enabled": bool(posthog_key),
        "app_title": "DataPulse — 用户行为分析 Demo",
    }
