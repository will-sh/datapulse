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


def _blueprint_index() -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for blueprint in load_catalog().get("blueprints") or []:
        if isinstance(blueprint, dict) and blueprint.get("name"):
            index[str(blueprint["name"])] = blueprint
    return index


def _blueprint_key_for_experience(name: str) -> str | None:
    if name.startswith("lakehouse-bp"):
        return "cloudera-lakehouse-engine"
    if name.startswith("csm-bp"):
        return "csm"
    if name.startswith("cai-bp") or name == "cai-bp":
        return "cai"
    return None


def _experience_record(raw: dict[str, Any]) -> dict[str, Any]:
    name = str(raw.get("name") or "")
    experience_id = str(raw.get("id") or name)
    blueprint_key = _blueprint_key_for_experience(name)
    blueprint = _blueprint_index().get(blueprint_key or "", {})
    return {
        "id": experience_id,
        "name": name,
        "title": str(raw.get("appName") or name),
        "description": str(blueprint.get("description") or ""),
        "blueprint_name": str(blueprint.get("displayName") or raw.get("appName") or ""),
        "blueprint_id": str(blueprint.get("name") or blueprint_key or ""),
        "status": str(raw.get("status") or "unknown"),
        "cluster": str(raw.get("clusterName") or ""),
        "cluster_id": str(raw.get("clusterId") or ""),
        "version": str(raw.get("version") or ""),
        "provider": str(raw.get("cloudCredentialProvider") or ""),
        "credential": str(raw.get("cloudCredentialName") or ""),
        "created_on": str(raw.get("createdOn") or ""),
        "doc_path": f"/experiences/{experience_id}",
    }


def marketplace_experiences() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for exp in load_catalog().get("experiences") or []:
        if isinstance(exp, dict):
            items.append(_experience_record(exp))
    return sorted(items, key=lambda item: item["created_on"], reverse=True)


def get_experience(experience_key: str) -> dict[str, Any] | None:
    key = experience_key.strip()
    for exp in marketplace_experiences():
        if exp["id"] == key or exp["name"] == key:
            return exp
    return None


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


def marketplace_items() -> list[dict[str, Any]]:
    """Console Marketplace catalog — deployable blueprints (/api/v0/console/blueprints)."""
    items: list[dict[str, Any]] = []
    for blueprint in load_catalog().get("blueprints") or []:
        if not isinstance(blueprint, dict):
            continue
        name = str(blueprint.get("name") or "")
        items.append(
            {
                "id": name,
                "blueprint_id": blueprint.get("blueprintId"),
                "title": str(blueprint.get("displayName") or name),
                "description": str(blueprint.get("description") or ""),
                "owner": str(blueprint.get("owner") or "Cloudera"),
                "version": str(blueprint.get("latestVersion") or ""),
                "doc_path": f"/marketplace/{name}",
            }
        )
    return sorted(items, key=lambda item: item["title"].lower())


def get_marketplace_item(item_key: str) -> dict[str, Any] | None:
    key = item_key.strip()
    for item in marketplace_items():
        if item["id"] == key:
            return item
    return None


def blueprint_plans() -> list[dict[str, Any]]:
    return [
        {
            **item,
            "name": item["title"],
            "outcome": item["description"],
            "features": [
                f"Blueprint: {item['id']}",
                f"Owner: {item['owner']}",
                f"Latest version: {item['version']}",
            ],
            "popular": item["id"] in POPULAR_BLUEPRINTS,
        }
        for item in marketplace_items()
    ]


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
    return [
        {
            "id": item["id"],
            "title": item["title"],
            "description": item["description"],
            "version": item["version"],
        }
        for item in marketplace_items()[:limit]
    ]
