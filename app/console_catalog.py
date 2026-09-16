"""Load AWC Console marketplace catalog synced from the real Console API."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

CATALOG_PATH = Path(__file__).resolve().parent.parent / "data" / "console_catalog.json"

ENGINE_CATEGORY_RULES: tuple[tuple[str, str], ...] = (
    ("kafka", "Streaming"),
    ("csa", "Streaming"),
    ("streaming", "Streaming"),
    ("trino", "Lakehouse"),
    ("lakehouse", "Lakehouse"),
    ("hive", "Lakehouse"),
    ("ozone", "Lakehouse"),
    ("cai", "AI"),
    ("cde", "Data Engineering"),
    ("cdf", "Data Flow"),
    ("cdx", "Analytics"),
    ("dataviz", "Analytics"),
    ("data-engineering", "Data Engineering"),
    ("postgres", "Data Services"),
    ("valkey", "Data Services"),
    ("prometheus", "Observability"),
    ("surveyor", "Observability"),
    ("udf", "Governance"),
    ("secret", "Platform"),
    ("cadence", "Orchestration"),
    ("cwo", "Orchestration"),
    ("yunikorn", "Platform"),
)

ENGINE_ICON_RULES: tuple[tuple[str, str], ...] = (
    ("kafka", "KM"),
    ("csa", "CSA"),
    ("streaming", "SA"),
    ("trino", "TR"),
    ("lakehouse", "LH"),
    ("hive", "HM"),
    ("ozone", "OZ"),
    ("cai", "AI"),
    ("cde", "DE"),
    ("cdf", "DF"),
    ("cdx", "DX"),
    ("dataviz", "DV"),
    ("postgres", "PG"),
    ("prometheus", "PM"),
    ("surveyor", "SV"),
    ("udf", "UDF"),
    ("cadence", "CW"),
    ("cwo", "WO"),
)

POPULAR_BLUEPRINTS = frozenset(
    {
        "cloudera-lakehouse-engine",
        "csa",
        "cai",
    }
)


def _match_rule(name: str, rules: tuple[tuple[str, str], ...], default: str) -> str:
    lowered = name.lower()
    for needle, value in rules:
        if needle in lowered:
            return value
    return default


@lru_cache
def load_catalog() -> dict[str, Any]:
    if not CATALOG_PATH.is_file():
        return {
            "synced_at": None,
            "console_url": None,
            "experiences": [],
            "engines": [],
            "blueprints": [],
        }
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


def catalog_meta() -> dict[str, Any]:
    catalog = load_catalog()
    return {
        "synced_at": catalog.get("synced_at"),
        "console_url": catalog.get("console_url"),
        "experience_count": len(catalog.get("experiences") or []),
        "engine_count": len(catalog.get("engines") or []),
        "blueprint_count": len(catalog.get("blueprints") or []),
    }


def marketplace_experiences() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for exp in load_catalog().get("experiences") or []:
        if not isinstance(exp, dict):
            continue
        landing = exp.get("landingPageUrl") or ""
        if isinstance(landing, str) and "," in landing:
            landing = landing.split(",", 1)[0].strip()
        items.append(
            {
                "id": str(exp.get("id") or exp.get("name") or ""),
                "name": str(exp.get("name") or ""),
                "title": str(exp.get("appName") or exp.get("name") or ""),
                "status": str(exp.get("status") or "unknown"),
                "cluster": str(exp.get("clusterName") or ""),
                "version": str(exp.get("version") or ""),
                "provider": str(exp.get("cloudCredentialProvider") or ""),
                "landing_url": landing,
                "created_on": str(exp.get("createdOn") or ""),
            }
        )
    return sorted(items, key=lambda item: item["created_on"], reverse=True)


def marketplace_engines() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for engine in load_catalog().get("engines") or []:
        if not isinstance(engine, dict):
            continue
        name = str(engine.get("name") or "")
        items.append(
            {
                "id": name,
                "engine_id": engine.get("engineId"),
                "icon": _match_rule(name, ENGINE_ICON_RULES, name[:2].upper()),
                "category": _match_rule(name, ENGINE_CATEGORY_RULES, "Platform"),
                "title": str(engine.get("displayName") or name),
                "description": str(engine.get("description") or ""),
                "version": str(engine.get("latestVersion") or ""),
            }
        )
    return sorted(items, key=lambda item: item["title"].lower())


def blueprint_plans() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for blueprint in load_catalog().get("blueprints") or []:
        if not isinstance(blueprint, dict):
            continue
        name = str(blueprint.get("name") or "")
        description = str(blueprint.get("description") or "")
        owner = str(blueprint.get("owner") or "Cloudera")
        version = str(blueprint.get("latestVersion") or "")
        items.append(
            {
                "id": name,
                "blueprint_id": blueprint.get("blueprintId"),
                "name": str(blueprint.get("displayName") or name),
                "description": description,
                "outcome": description,
                "features": [
                    f"Blueprint: {name}",
                    f"Owner: {owner}",
                    f"Latest version: {version}",
                ],
                "owner": owner,
                "version": version,
                "popular": name in POPULAR_BLUEPRINTS,
            }
        )
    return sorted(items, key=lambda item: (not item["popular"], item["name"].lower()))


def playground_blueprints(limit: int = 3) -> list[dict[str, Any]]:
    plans = blueprint_plans()
    preferred = [plan for plan in plans if plan["popular"]]
    if len(preferred) >= limit:
        return preferred[:limit]
    seen = {plan["id"] for plan in preferred}
    for plan in plans:
        if plan["id"] in seen:
            continue
        preferred.append(plan)
        if len(preferred) >= limit:
            break
    return preferred


def playground_deploy_targets(limit: int = 3) -> list[dict[str, Any]]:
    engines = marketplace_engines()
    preferred_names = ("kafka-plugin", "trino-engine", "cai-plugin")
    selected: list[dict[str, Any]] = []
    by_name = {engine["id"]: engine for engine in engines}
    for name in preferred_names:
        if name in by_name:
            selected.append(by_name[name])
    for engine in engines:
        if engine["id"] in {item["id"] for item in selected}:
            continue
        selected.append(engine)
        if len(selected) >= limit:
            break
    return selected[:limit]
