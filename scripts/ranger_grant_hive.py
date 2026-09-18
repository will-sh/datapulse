#!/usr/bin/env python3
"""Grant Hive (HMS) Ranger policies for CSA Flink / service users via Ranger REST API."""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import urllib.request
from typing import Any

KNOX_BASE = os.getenv("KNOX_BASE", "https://knox.readygo.a70735.test.cldr.work").rstrip("/")
RANGER_BASE = os.getenv(
    "RANGER_BASE",
    "https://ranger.lakehouse-bp-6b4b81.cldr-csk-lakehouse.a70735.test.cldr.work",
).rstrip("/")
CONSOLE_USER = os.getenv("CONSOLE_USER", "admin")
CONSOLE_PASSWORD = os.getenv("CONSOLE_PASSWORD", "awc-admin-password")
COOKIEJAR = os.getenv("RANGER_COOKIEJAR", "/tmp/ranger-cookies.txt")
HIVE_SERVICE = os.getenv("RANGER_HIVE_SERVICE", "udf_hive")

DEFAULT_USERS = [
    "admin",
    "hive",
    "flink",
    "ssb",
    "hadoop",
    "anonymous",
    # Console Access Key used by Kafka OAuth clients (also HMS when impersonated).
    "72491e16-87cc-4e7a-8ee0-2cae66dd9876",
]
DEFAULT_GROUPS = ["public", "hive"]
DEFAULT_ACCESSES = [
    "select",
    "update",
    "create",
    "drop",
    "alter",
    "index",
    "lock",
    "read",
    "write",
    "refresh",
]
ICEBERG_STORAGE_ACCESSES = ["rwstorage"]


def login() -> None:
    auth = base64.b64encode(f"{CONSOLE_USER}:{CONSOLE_PASSWORD}".encode()).decode()
    original = f"{RANGER_BASE}/"
    url = (
        f"{KNOX_BASE}/gateway/knox-cdpsso/api/v1/websso"
        f"?originalUrl={urllib.request.quote(original, safe='')}"
    )
    subprocess.check_call(
        [
            "curl",
            "-sk",
            "-c",
            COOKIEJAR,
            "-X",
            "POST",
            "-H",
            f"Authorization: Basic {auth}",
            url,
        ]
    )


def ranger_request(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    cmd = [
        "curl",
        "-sk",
        "-b",
        COOKIEJAR,
        "-X",
        method,
        "-H",
        "Accept: application/json",
        f"{RANGER_BASE}{path}",
    ]
    if payload is not None:
        cmd.extend(["-H", "Content-Type: application/json", "-d", json.dumps(payload)])
    raw = subprocess.check_output(cmd, text=True).strip()
    if not raw:
        return {}
    return json.loads(raw)


def list_policies(service: str = HIVE_SERVICE) -> list[dict[str, Any]]:
    data = ranger_request("GET", f"/service/public/v2/api/policy?serviceName={service}")
    return data if isinstance(data, list) else []


def add_users_to_policy(policy_id: int, users: list[str], groups: list[str] | None = None) -> dict[str, Any]:
    policy = ranger_request("GET", f"/service/public/v2/api/policy/{policy_id}")
    if not policy.get("policyItems"):
        raise RuntimeError(f"policy {policy_id} has no policyItems")
    item = policy["policyItems"][0]
    merged_users = sorted(set(item.get("users", []) + users))
    item["users"] = merged_users
    if groups:
        item["groups"] = sorted(set(item.get("groups", []) + groups))
    return ranger_request("PUT", f"/service/public/v2/api/policy/{policy_id}", policy)


def ensure_iceberg_storage_policy(users: list[str], groups: list[str]) -> dict[str, Any]:
    for policy in list_policies():
        resources = policy.get("resources") or {}
        storage_type = (resources.get("storage-type") or {}).get("values") or []
        if storage_type == ["iceberg"]:
            return add_users_to_policy(int(policy["id"]), users, groups)

    payload = {
        "service": HIVE_SERVICE,
        "name": "iceberg storage handler RW storage",
        "policyType": 0,
        "policyPriority": 0,
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": {
            "storage-type": {"values": ["iceberg"], "isExcludes": False, "isRecursive": False},
            "storage-url": {"values": ["*"], "isExcludes": False, "isRecursive": False},
        },
        "policyItems": [
            {
                "accesses": [{"type": access, "isAllowed": True} for access in ICEBERG_STORAGE_ACCESSES],
                "users": users,
                "groups": groups,
                "roles": [],
                "conditions": [],
                "delegateAdmin": False,
            }
        ],
        "isDenyAllElse": False,
    }
    return ranger_request("POST", "/service/public/v2/api/policy", payload)


def ensure_datapulse_policy(users: list[str], groups: list[str]) -> dict[str, Any]:
    for policy in list_policies():
        resources = policy.get("resources") or {}
        db = (resources.get("database") or {}).get("values") or []
        if db == ["datapulse"]:
            return add_users_to_policy(int(policy["id"]), users, groups)

    payload = {
        "service": HIVE_SERVICE,
        "name": "datapulse database service access",
        "policyType": 0,
        "policyPriority": 0,
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": {
            "database": {"values": ["datapulse"], "isExcludes": False, "isRecursive": False},
            "table": {"values": ["*"], "isExcludes": False, "isRecursive": False},
            "column": {"values": ["*"], "isExcludes": False, "isRecursive": False},
        },
        "policyItems": [
            {
                "accesses": [{"type": access, "isAllowed": True} for access in DEFAULT_ACCESSES],
                "users": users,
                "groups": groups,
                "roles": [],
                "conditions": [],
                "delegateAdmin": False,
            }
        ],
        "isDenyAllElse": False,
    }
    return ranger_request("POST", "/service/public/v2/api/policy", payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["list", "grant-datapulse", "grant-service-users"],
        help="list hive policies or grant access",
    )
    parser.add_argument("--users", default=",".join(DEFAULT_USERS), help="comma-separated Ranger users")
    parser.add_argument("--groups", default=",".join(DEFAULT_GROUPS), help="comma-separated Ranger groups")
    args = parser.parse_args()

    if not CONSOLE_PASSWORD:
        print("CONSOLE_PASSWORD is required", file=sys.stderr)
        return 1

    login()
    users = [u.strip() for u in args.users.split(",") if u.strip()]
    groups = [g.strip() for g in args.groups.split(",") if g.strip()]

    if args.action == "list":
        policies = list_policies()
        print(json.dumps(policies, indent=2))
        return 0

    if args.action == "grant-datapulse":
        result = ensure_datapulse_policy(users, groups)
        print(json.dumps(result, indent=2))
        return 0

    # grant-service-users: update known baseline policies (same pattern as Trino cm_trino fixes)
    updates = {"iceberg storage handler RW storage": ensure_iceberg_storage_policy(users, groups)}
    for policy_id, label in [(21, "datapulse database admin access"), (1, "default database"), (38, "admin all databases")]:
        updates[label] = add_users_to_policy(policy_id, users, groups)
    print(json.dumps(updates, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
