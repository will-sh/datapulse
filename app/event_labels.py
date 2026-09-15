from __future__ import annotations

import re
from typing import Any

# Ordered keys used to derive a low-cardinality component label from event properties.
_COMPONENT_KEYS = ("button", "feature", "action", "tab", "label", "target", "href")

_LABEL_UNSAFE = re.compile(r"[^\w.\:/-]+", re.UNICODE)
_DEFAULT_COMPONENT = "-"


def _sanitize(value: str, *, max_len: int = 64) -> str:
    cleaned = _LABEL_UNSAFE.sub("_", value.strip())[:max_len].strip("_")
    return cleaned or "unknown"


def event_component(event_name: str, properties: dict[str, Any] | None) -> str:
    """Map an event to a stable Prometheus label value for component-level charts."""
    props = properties or {}
    for key in _COMPONENT_KEYS:
        raw = props.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        return f"{key}:{_sanitize(text)}"

    if event_name == "$pageview":
        path = props.get("path")
        if path is not None and str(path).strip():
            return f"path:{_sanitize(str(path))}"

    return _DEFAULT_COMPONENT
