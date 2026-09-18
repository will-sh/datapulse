"""Product analytics for Live (buffer) and Insights (Lakehouse via Trino)."""

from __future__ import annotations

import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from app.live.service import pipeline_out
from app.live.warehouse_insights import load_insights_events
from app.spark_stream.store import StreamEvent

FORM_SUBMIT_EVENTS = frozenset(
    {
        "awc_lead_form_submitted",
        "demo_form_submitted",
    }
)

MARKETPLACE_EVENTS = frozenset(
    {
        "marketplace_details_clicked",
        "marketplace_doc_viewed",
        "marketplace_preview_clicked",
        "marketplace_item_doc_opened",
        "marketplace_doc_link_clicked",
        "marketplace_doc_back",
    }
)

MARKETPLACE_BROWSE_EVENTS = MARKETPLACE_EVENTS | frozenset(
    {
        "marketplace_view_all_clicked",
        "blueprint_card_clicked",
        "hero_secondary_clicked",
        "blueprint_evaluated",
    }
)

BLUEPRINT_INTERACTION_EVENTS = frozenset(
    {
        "marketplace_details_clicked",
        "blueprint_card_clicked",
        "blueprint_evaluated",
        "marketplace_doc_viewed",
        "marketplace_preview_clicked",
        "marketplace_item_doc_opened",
    }
)

DOC_VIEW_EVENTS = frozenset(
    {
        "marketplace_doc_viewed",
        "marketplace_item_doc_opened",
    }
)

CTA_EVENTS = frozenset(
    {
        "hero_cta_clicked",
        "awc_lead_form_submit_clicked",
        "cta_header_clicked",
    }
)

IDENTIFY_EVENTS = frozenset({"user_identified"})

BLUEPRINT_PROP_KEYS = ("blueprint", "blueprint_name", "title", "service")


def _default_window() -> int:
    return int(os.getenv("LIVE_STATS_WINDOW", "300"))


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 1)


def _mask_email(email: str) -> str:
    text = email.strip()
    if "@" not in text:
        return text[:2] + "***" if len(text) > 2 else "***"
    local, domain = text.split("@", 1)
    if not local:
        return f"***@{domain}"
    return f"{local[0]}***@{domain}"


def _session_key(event: StreamEvent) -> str:
    if event.session_id:
        return event.session_id
    if event.anonymous_id:
        return f"anon:{event.anonymous_id}"
    return f"evt:{event.received_at}"


def _actor_key(event: StreamEvent) -> str:
    if event.user_id:
        return f"user:{event.user_id}"
    if event.anonymous_id:
        return f"anon:{event.anonymous_id}"
    return _session_key(event)


def _form_email(event: StreamEvent) -> str | None:
    props = event.properties or {}
    for key in ("email", "business_email", "userId"):
        value = props.get(key)
        if value:
            text = str(value).strip()
            if text:
                return text
    if event.user_id and "@" in event.user_id:
        return event.user_id
    return None


def _build_sessions(events: list[StreamEvent]) -> dict[str, list[StreamEvent]]:
    grouped: dict[str, list[StreamEvent]] = defaultdict(list)
    for event in sorted(events, key=lambda item: item.received_at):
        grouped[_session_key(event)].append(event)
    return grouped


def _truncate_id(value: str | None, *, limit: int = 14) -> str | None:
    if not value:
        return None
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def _session_user_display(session_events: list[StreamEvent]) -> str | None:
    for event in reversed(session_events):
        if event.user_id:
            text = str(event.user_id)
            return _mask_email(text) if "@" in text else text
        email = _form_email(event)
        if email:
            return _mask_email(email)
    return None


def _session_paths(session_events: list[StreamEvent]) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for event in session_events:
        if event.page_path and event.page_path not in seen:
            seen.add(event.page_path)
            paths.append(event.page_path)
    return paths


def _build_session_rows(sessions: dict[str, list[StreamEvent]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sid, session_events in sessions.items():
        if not session_events:
            continue

        submit_event = next(
            (event for event in session_events if event.name in FORM_SUBMIT_EVENTS),
            None,
        )
        anonymous_id = next(
            (event.anonymous_id for event in session_events if event.anonymous_id),
            None,
        )
        first_at = session_events[0].received_at
        last_at = session_events[-1].received_at
        marketplace_engaged = any(
            event.name in MARKETPLACE_EVENTS for event in session_events
        )

        rows.append(
            {
                "session_id": sid,
                "session_id_short": _truncate_id(sid),
                "anonymous_id": anonymous_id,
                "anonymous_id_short": _truncate_id(anonymous_id),
                "user_display": _session_user_display(session_events),
                "converted": submit_event is not None,
                "form_event": submit_event.name if submit_event else None,
                "marketplace_engaged": marketplace_engaged,
                "pages": _session_paths(session_events),
                "event_count": len(session_events),
                "first_activity_at": first_at,
                "last_activity_at": last_at,
            }
        )

    rows.sort(key=lambda row: row["last_activity_at"], reverse=True)
    return rows


def _session_has_event(
    session_events: list[StreamEvent],
    names: frozenset[str],
) -> bool:
    return any(event.name in names for event in session_events)


def _session_marketplace_browse(session_events: list[StreamEvent]) -> bool:
    if _session_has_event(session_events, MARKETPLACE_BROWSE_EVENTS):
        return True
    return any(
        event.name == "nav_link_clicked"
        and str((event.properties or {}).get("href", "")).startswith("/features")
        for event in session_events
    ) or any(event.page_path == "/features" for event in session_events)


def _blueprint_label(event: StreamEvent) -> str | None:
    props = event.properties or {}
    for key in BLUEPRINT_PROP_KEYS:
        value = props.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _build_enhanced_funnel(
    sessions: dict[str, list[StreamEvent]],
    *,
    session_count: int,
) -> list[dict[str, Any]]:
    pageview_sessions = {
        sid
        for sid, session_events in sessions.items()
        if any(event.name == "$pageview" for event in session_events)
    }
    browse_sessions = {
        sid
        for sid, session_events in sessions.items()
        if _session_marketplace_browse(session_events)
    }
    detail_sessions = {
        sid
        for sid, session_events in sessions.items()
        if _session_has_event(session_events, frozenset({"marketplace_details_clicked"}))
    }
    doc_sessions = {
        sid
        for sid, session_events in sessions.items()
        if _session_has_event(session_events, DOC_VIEW_EVENTS)
    }
    cta_sessions = {
        sid
        for sid, session_events in sessions.items()
        if _session_has_event(session_events, CTA_EVENTS)
    }
    form_sessions = {
        sid
        for sid, session_events in sessions.items()
        if _session_has_event(session_events, FORM_SUBMIT_EVENTS)
    }

    steps = [
        ("pageview", "Visited site ($pageview)", pageview_sessions),
        ("marketplace_browse", "Browsed Marketplace", browse_sessions),
        ("blueprint_detail", "Opened Blueprint detail", detail_sessions),
        ("doc_read", "Read Blueprint doc", doc_sessions),
        ("cta_click", "Clicked lead/demo CTA", cta_sessions),
        ("form_submit", "Submitted lead form", form_sessions),
    ]
    funnel: list[dict[str, Any]] = []
    previous = session_count or 1
    for step, label, matched in steps:
        count = len(matched)
        base = previous if step != "pageview" else (session_count or 1)
        funnel.append(
            {
                "step": step,
                "label": label,
                "sessions": count,
                "rate_pct": _pct(count, base),
            }
        )
        if count > 0:
            previous = count
    return funnel


def _bucket_key(received_at: float, *, hourly: bool) -> str:
    dt = datetime.fromtimestamp(received_at, tz=timezone.utc)
    if hourly:
        return dt.strftime("%Y-%m-%d %H:00")
    return dt.strftime("%Y-%m-%d")


def _build_activity_trends(
    events: list[StreamEvent],
    sessions: dict[str, list[StreamEvent]],
    *,
    window_seconds: int,
) -> dict[str, Any]:
    hourly = window_seconds < 86400
    event_buckets: Counter[str] = Counter()
    session_buckets: Counter[str] = Counter()
    form_buckets: Counter[str] = Counter()

    for event in events:
        key = _bucket_key(event.received_at, hourly=hourly)
        event_buckets[key] += 1
        if event.name in FORM_SUBMIT_EVENTS:
            form_buckets[key] += 1

    for session_events in sessions.values():
        if not session_events:
            continue
        key = _bucket_key(session_events[0].received_at, hourly=hourly)
        session_buckets[key] += 1

    labels = sorted(set(event_buckets) | set(session_buckets) | set(form_buckets))
    return {
        "granularity": "hour" if hourly else "day",
        "labels": labels,
        "events": [event_buckets[label] for label in labels],
        "sessions": [session_buckets[label] for label in labels],
        "form_submissions": [form_buckets[label] for label in labels],
    }


def _build_blueprint_leaderboard(events: list[StreamEvent]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for event in events:
        if event.name not in BLUEPRINT_INTERACTION_EVENTS:
            continue
        label = _blueprint_label(event)
        if label:
            counts[label] += 1
    return [{"blueprint": name, "interactions": count} for name, count in counts.most_common(8)]


def _session_pageview_paths(session_events: list[StreamEvent]) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for event in sorted(session_events, key=lambda item: item.received_at):
        if event.name != "$pageview":
            continue
        path = event.page_path or (event.properties or {}).get("path")
        if path is None:
            continue
        text = str(path).strip()
        if text and text not in seen:
            seen.add(text)
            paths.append(text)
    return paths


def _build_top_paths(
    sessions: dict[str, list[StreamEvent]],
    *,
    max_hops: int = 3,
    limit: int = 8,
) -> list[dict[str, Any]]:
    path_stats: dict[str, dict[str, int]] = {}
    for session_events in sessions.values():
        paths = _session_pageview_paths(session_events)[:max_hops]
        if not paths:
            continue
        signature = " → ".join(paths)
        converted = _session_has_event(session_events, FORM_SUBMIT_EVENTS)
        stats = path_stats.setdefault(signature, {"sessions": 0, "converted": 0})
        stats["sessions"] += 1
        if converted:
            stats["converted"] += 1

    rows = [
        {
            "path": signature,
            "sessions": stats["sessions"],
            "converted": stats["converted"],
            "conversion_rate_pct": _pct(stats["converted"], stats["sessions"]),
        }
        for signature, stats in path_stats.items()
    ]
    rows.sort(key=lambda row: (row["sessions"], row["converted"]), reverse=True)
    return rows[:limit]


def compute_insights(
    window_seconds: int | None = None,
    *,
    source: str = "auto",
    window_days: int | None = None,
) -> dict[str, Any]:
    events, data_source, window, note, warehouse_meta = load_insights_events(
        source=source,
        window_seconds=window_seconds,
        window_days=window_days,
    )
    sessions = _build_sessions(events)

    session_count = len(sessions)
    actors = {_actor_key(event) for event in events}
    actor_count = len(actors)

    pageview_sessions = {
        sid
        for sid, session_events in sessions.items()
        if any(event.name == "$pageview" for event in session_events)
    }
    marketplace_sessions = {
        sid
        for sid, session_events in sessions.items()
        if any(event.name in MARKETPLACE_EVENTS for event in session_events)
    }
    form_sessions = {
        sid
        for sid, session_events in sessions.items()
        if any(event.name in FORM_SUBMIT_EVENTS for event in session_events)
    }
    identified_actors = {
        _actor_key(event)
        for event in events
        if event.user_id or event.name in IDENTIFY_EVENTS
    }

    multi_page_sessions = 0
    engaged_sessions = 0
    for session_events in sessions.values():
        paths = {
            event.page_path
            for event in session_events
            if event.page_path
        }
        if len(paths) >= 2:
            multi_page_sessions += 1
        if len(session_events) >= 3:
            engaged_sessions += 1

    funnel = _build_enhanced_funnel(sessions, session_count=session_count)
    activity_trends = _build_activity_trends(events, sessions, window_seconds=window)
    blueprint_leaderboard = _build_blueprint_leaderboard(events)
    top_paths = _build_top_paths(sessions)

    converters: list[dict[str, Any]] = []
    for sid, session_events in sessions.items():
        submit_event = next(
            (event for event in session_events if event.name in FORM_SUBMIT_EVENTS),
            None,
        )
        if submit_event is None:
            continue

        email = _form_email(submit_event)
        paths: list[str] = []
        seen_paths: set[str] = set()
        for event in session_events:
            if event.page_path and event.page_path not in seen_paths:
                seen_paths.add(event.page_path)
                paths.append(event.page_path)

        converters.append(
            {
                "session_id": sid,
                "event_name": submit_event.name,
                "submitted_at": submit_event.received_at,
                "email_masked": _mask_email(email) if email else None,
                "user_id": submit_event.user_id,
                "pages": paths,
                "event_count": len(session_events),
            }
        )

    converters.sort(key=lambda row: row["submitted_at"], reverse=True)

    top_pages = Counter(
        event.page_path
        for event in events
        if event.name == "$pageview" and event.page_path
    ).most_common(8)

    top_events = Counter(event.name for event in events).most_common(10)

    form_submits = sum(1 for event in events if event.name in FORM_SUBMIT_EVENTS)
    all_sessions = _build_session_rows(sessions)
    converted_count = sum(1 for row in all_sessions if row["converted"])
    anonymous_only_count = sum(1 for row in all_sessions if not row["user_display"])

    window_days_effective = max(1, round(window / 86400)) if window >= 86400 else None
    payload: dict[str, Any] = {
        "generated_at": time.time(),
        "window_seconds": window,
        "window_days": window_days_effective,
        "data_source": data_source,
        "pipeline": pipeline_out().model_dump(),
        "note": note,
        "kpis": {
            "sessions": session_count,
            "actors": actor_count,
            "identified_users": len(identified_actors),
            "form_submissions": form_submits,
            "conversion_rate_pct": _pct(len(form_sessions), len(pageview_sessions) or 1),
            "marketplace_engagement_rate_pct": _pct(
                len(marketplace_sessions),
                len(pageview_sessions) or 1,
            ),
            "multi_page_session_rate_pct": _pct(multi_page_sessions, session_count or 1),
            "engaged_session_rate_pct": _pct(engaged_sessions, session_count or 1),
        },
        "funnel": funnel,
        "activity_trends": activity_trends,
        "blueprint_leaderboard": blueprint_leaderboard,
        "top_paths": top_paths,
        "top_pages": [{"path": path, "views": count} for path, count in top_pages],
        "top_events": [{"name": name, "count": count} for name, count in top_events],
        "recent_converters": converters[:20],
        "all_sessions": all_sessions,
        "session_summary": {
            "total": len(all_sessions),
            "converted": converted_count,
            "not_converted": len(all_sessions) - converted_count,
            "anonymous_only": anonymous_only_count,
            "identified": len(all_sessions) - anonymous_only_count,
        },
    }
    if warehouse_meta is not None:
        payload["warehouse"] = warehouse_meta
    return payload


def compute_insights_buffer(window_seconds: int | None = None) -> dict[str, Any]:
    """Backward-compatible buffer-only insights."""
    return compute_insights(window_seconds, source="buffer")
