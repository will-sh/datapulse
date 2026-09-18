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

CTA_LEADERBOARD_EVENTS = CTA_EVENTS | frozenset(
    {
        "button_clicked",
        "hero_secondary_clicked",
        "nav_link_clicked",
        "cta_header_clicked",
    }
)

FAQ_EVENTS = frozenset({"faq_opened", "faq_expanded"})

PLAYGROUND_EVENTS = frozenset({"engine_deploy_clicked", "playground_tab_changed"})

IDENTIFY_EVENTS = frozenset({"user_identified"})

BLUEPRINT_PROP_KEYS = ("blueprint", "blueprint_name", "title", "service")
CTA_PROP_KEYS = ("label", "button", "target", "name", "href")
FAQ_PROP_KEYS = ("question",)
ENGINE_PROP_KEYS = ("engine", "engine_title", "tab")

HEATMAP_DOW_LABELS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
TIME_TO_CONVERT_BUCKETS = (
    ("under_1m", "Under 1 min", 0, 60),
    ("1_5m", "1–5 min", 60, 300),
    ("5_30m", "5–30 min", 300, 1800),
    ("over_30m", "Over 30 min", 1800, None),
)


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


def _prop_label(event: StreamEvent, keys: tuple[str, ...]) -> str | None:
    props = event.properties or {}
    for key in keys:
        value = props.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _cta_label(event: StreamEvent) -> str:
    props = event.properties or {}
    parts: list[str] = [event.name]
    for key in CTA_PROP_KEYS:
        value = props.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            parts.append(text)
            break
    return " · ".join(parts)


def _filter_events_to_window(events: list[StreamEvent], window_seconds: int) -> list[StreamEvent]:
    cutoff = time.time() - window_seconds
    return [event for event in events if event.received_at >= cutoff]


def _split_events_for_comparison(
    events: list[StreamEvent],
    *,
    window_seconds: int,
    full_prior_period: bool,
) -> tuple[list[StreamEvent], list[StreamEvent], str]:
    now = time.time()
    current_start = now - window_seconds
    current = [event for event in events if event.received_at >= current_start]
    if full_prior_period:
        previous_start = now - (2 * window_seconds)
        previous = [
            event
            for event in events
            if previous_start <= event.received_at < current_start
        ]
        return current, previous, "vs prior period"
    midpoint = current_start + (window_seconds / 2)
    previous = [
        event for event in events
        if current_start <= event.received_at < midpoint
    ]
    current_half = [event for event in events if event.received_at >= midpoint]
    return current_half, previous, "vs first half"


def _delta_pct(current: int | float, previous: int | float) -> float | None:
    if previous <= 0:
        return None if current <= 0 else 100.0
    return round(((current - previous) / previous) * 100, 1)


def _build_kpi_comparison(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "sessions",
        "form_submissions",
        "conversion_rate_pct",
        "identified_users",
        "marketplace_engagement_rate_pct",
    )
    comparison: dict[str, Any] = {}
    for key in keys:
        comparison[key] = _delta_pct(current.get(key, 0), previous.get(key, 0))
    return comparison


def _compute_kpis(
    events: list[StreamEvent],
    sessions: dict[str, list[StreamEvent]],
) -> dict[str, Any]:
    session_count = len(sessions)
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
        paths = {event.page_path for event in session_events if event.page_path}
        if len(paths) >= 2:
            multi_page_sessions += 1
        if len(session_events) >= 3:
            engaged_sessions += 1

    form_submits = sum(1 for event in events if event.name in FORM_SUBMIT_EVENTS)
    return {
        "sessions": session_count,
        "actors": len({_actor_key(event) for event in events}),
        "identified_users": len(identified_actors),
        "form_submissions": form_submits,
        "conversion_rate_pct": _pct(len(form_sessions), len(pageview_sessions) or 1),
        "marketplace_engagement_rate_pct": _pct(
            len(marketplace_sessions),
            len(pageview_sessions) or 1,
        ),
        "multi_page_session_rate_pct": _pct(multi_page_sessions, session_count or 1),
        "engaged_session_rate_pct": _pct(engaged_sessions, session_count or 1),
    }


def _build_time_to_convert(
    sessions: dict[str, list[StreamEvent]],
) -> dict[str, Any]:
    durations: list[float] = []
    bucket_counts = {bucket_id: 0 for bucket_id, _, _, _ in TIME_TO_CONVERT_BUCKETS}

    for session_events in sessions.values():
        submit_event = next(
            (event for event in session_events if event.name in FORM_SUBMIT_EVENTS),
            None,
        )
        if submit_event is None:
            continue
        first_pageview = next(
            (event for event in session_events if event.name == "$pageview"),
            session_events[0],
        )
        duration = max(0.0, submit_event.received_at - first_pageview.received_at)
        durations.append(duration)
        for bucket_id, _, lower, upper in TIME_TO_CONVERT_BUCKETS:
            if duration >= lower and (upper is None or duration < upper):
                bucket_counts[bucket_id] += 1
                break

    durations.sort()
    median_seconds = None
    if durations:
        mid = len(durations) // 2
        if len(durations) % 2:
            median_seconds = round(durations[mid], 1)
        else:
            median_seconds = round((durations[mid - 1] + durations[mid]) / 2, 1)

    return {
        "converted_sessions": len(durations),
        "median_seconds": median_seconds,
        "buckets": [
            {
                "id": bucket_id,
                "label": label,
                "sessions": bucket_counts[bucket_id],
            }
            for bucket_id, label, _, _ in TIME_TO_CONVERT_BUCKETS
        ],
    }


def _build_activity_heatmap(events: list[StreamEvent]) -> dict[str, Any]:
    matrix = [[0 for _ in range(24)] for _ in range(7)]
    for event in events:
        dt = datetime.fromtimestamp(event.received_at, tz=timezone.utc)
        matrix[dt.weekday()][dt.hour] += 1

    max_count = max((count for row in matrix for count in row), default=0)
    return {
        "days": list(HEATMAP_DOW_LABELS),
        "hours": list(range(24)),
        "matrix": matrix,
        "max_count": max_count,
    }


def _build_cta_leaderboard(events: list[StreamEvent]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for event in events:
        if event.name not in CTA_LEADERBOARD_EVENTS:
            continue
        counts[_cta_label(event)] += 1
    return [{"label": label, "clicks": count} for label, count in counts.most_common(8)]


def _build_faq_leaderboard(events: list[StreamEvent]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for event in events:
        if event.name not in FAQ_EVENTS:
            continue
        question = _prop_label(event, FAQ_PROP_KEYS) or event.name
        counts[question] += 1
    return [{"question": question, "opens": count} for question, count in counts.most_common(8)]


def _build_engine_leaderboard(events: list[StreamEvent]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for event in events:
        if event.name not in PLAYGROUND_EVENTS:
            continue
        label = _prop_label(event, ENGINE_PROP_KEYS) or event.name
        counts[label] += 1
    return [{"engine": label, "interactions": count} for label, count in counts.most_common(8)]


def _build_visitor_mix(
    current_events: list[StreamEvent],
    previous_events: list[StreamEvent],
) -> dict[str, Any]:
    previous_anonymous = {
        event.anonymous_id
        for event in previous_events
        if event.anonymous_id
    }
    current_anonymous = {
        event.anonymous_id
        for event in current_events
        if event.anonymous_id
    }
    returning = len(current_anonymous & previous_anonymous)
    new_visitors = len(current_anonymous - previous_anonymous)
    unknown = len(
        {
            _session_key(event)
            for event in current_events
            if not event.anonymous_id
        }
    )
    total = returning + new_visitors + unknown
    return {
        "returning": returning,
        "new_visitors": new_visitors,
        "unknown": unknown,
        "total": total,
        "returning_rate_pct": _pct(returning, total or 1),
        "new_visitor_rate_pct": _pct(new_visitors, total or 1),
    }


def compute_insights(
    window_seconds: int | None = None,
    *,
    source: str = "auto",
    window_days: int | None = None,
) -> dict[str, Any]:
    events_fetched, data_source, window, note, warehouse_meta = load_insights_events(
        source=source,
        window_seconds=window_seconds,
        window_days=window_days,
        comparison=True,
    )
    full_prior_period = bool(
        warehouse_meta and warehouse_meta.get("comparison_enabled")
    )
    current_events, previous_events, comparison_label = _split_events_for_comparison(
        events_fetched,
        window_seconds=window,
        full_prior_period=full_prior_period,
    )
    if not full_prior_period:
        current_events = _filter_events_to_window(events_fetched, window)

    sessions = _build_sessions(current_events)
    previous_sessions = _build_sessions(previous_events)

    session_count = len(sessions)
    kpis = _compute_kpis(current_events, sessions)
    previous_kpis = _compute_kpis(previous_events, previous_sessions) if previous_events else None
    kpi_comparison = (
        _build_kpi_comparison(kpis, previous_kpis)
        if previous_kpis is not None
        else None
    )

    funnel = _build_enhanced_funnel(sessions, session_count=session_count)
    activity_trends = _build_activity_trends(current_events, sessions, window_seconds=window)
    blueprint_leaderboard = _build_blueprint_leaderboard(current_events)
    top_paths = _build_top_paths(sessions)
    time_to_convert = _build_time_to_convert(sessions)
    activity_heatmap = _build_activity_heatmap(current_events)
    cta_leaderboard = _build_cta_leaderboard(current_events)
    faq_leaderboard = _build_faq_leaderboard(current_events)
    engine_leaderboard = _build_engine_leaderboard(current_events)
    visitor_mix = _build_visitor_mix(current_events, previous_events)

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

        first_pageview = next(
            (event for event in session_events if event.name == "$pageview"),
            session_events[0],
        )
        converters.append(
            {
                "session_id": sid,
                "event_name": submit_event.name,
                "submitted_at": submit_event.received_at,
                "email_masked": _mask_email(email) if email else None,
                "user_id": submit_event.user_id,
                "pages": paths,
                "event_count": len(session_events),
                "time_to_convert_seconds": max(
                    0.0,
                    submit_event.received_at - first_pageview.received_at,
                ),
            }
        )

    converters.sort(key=lambda row: row["submitted_at"], reverse=True)

    top_pages = Counter(
        event.page_path
        for event in current_events
        if event.name == "$pageview" and event.page_path
    ).most_common(8)

    top_events = Counter(event.name for event in current_events).most_common(10)

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
        "kpis": kpis,
        "kpi_comparison": kpi_comparison,
        "comparison_label": comparison_label if kpi_comparison else None,
        "funnel": funnel,
        "activity_trends": activity_trends,
        "activity_heatmap": activity_heatmap,
        "time_to_convert": time_to_convert,
        "cta_leaderboard": cta_leaderboard,
        "faq_leaderboard": faq_leaderboard,
        "engine_leaderboard": engine_leaderboard,
        "visitor_mix": visitor_mix,
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
