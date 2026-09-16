"""Product analytics computed from the in-memory Kafka consumer buffer."""

from __future__ import annotations

import os
import time
from collections import Counter, defaultdict
from typing import Any

from app.live.service import pipeline_out
from app.spark_stream.store import STORE, StreamEvent

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

IDENTIFY_EVENTS = frozenset({"user_identified"})


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


def compute_insights(window_seconds: int | None = None) -> dict[str, Any]:
    window = window_seconds if window_seconds is not None else _default_window()
    events = STORE.events_in_window(window)
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

    funnel = [
        {
            "step": "pageview",
            "label": "Visited site ($pageview)",
            "sessions": len(pageview_sessions),
            "rate_pct": _pct(len(pageview_sessions), session_count or 1),
        },
        {
            "step": "marketplace",
            "label": "Engaged Marketplace",
            "sessions": len(marketplace_sessions),
            "rate_pct": _pct(len(marketplace_sessions), len(pageview_sessions) or 1),
        },
        {
            "step": "form_submit",
            "label": "Submitted lead form",
            "sessions": len(form_sessions),
            "rate_pct": _pct(len(form_sessions), len(pageview_sessions) or 1),
        },
    ]

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

    return {
        "generated_at": time.time(),
        "window_seconds": window,
        "pipeline": pipeline_out().model_dump(),
        "note": "Insights reflect the in-memory consumer buffer only (not historical warehouse data).",
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
        "retention": {
            "multi_page_sessions": multi_page_sessions,
            "engaged_sessions": engaged_sessions,
            "description": "Proxy metrics within the live buffer window: multi-page = 2+ paths; engaged = 3+ events.",
        },
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
