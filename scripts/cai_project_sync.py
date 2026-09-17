#!/usr/bin/env python3
"""Keep a CAI project file tree aligned with the local git checkout (prefer main).

Usage:
  python3 scripts/cai_project_sync.py upload              # upload manifest files
  python3 scripts/cai_project_sync.py upload --prune      # upload + delete remote extras
  python3 scripts/cai_project_sync.py verify              # compare remote vs manifest
  python3 scripts/cai_project_sync.py deploy              # upload + recreate applications

Environment:
  CAI_BASE, CAI_PID, CAI_KEY (or CDSW_APIV2_KEY)
  CAI_REQUIRE_BRANCH=main          # fail upload if not on this branch (default: main)
  CAI_SYNC_EXTRA=config/kafka/kafka-ca.crt,config/kafka/oauth-ca.crt
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

CAI_BASE = os.getenv("CAI_BASE", "").rstrip("/")
CAI_PID = os.getenv("CAI_PID", "")
CAI_KEY = os.getenv("CAI_KEY", os.getenv("CDSW_APIV2_KEY", ""))
REQUIRE_BRANCH = os.getenv("CAI_REQUIRE_BRANCH", "main")

# Never upload or require on CAI
EXCLUDE_PREFIXES = (
    ".git/",
    ".cursor/",
    "agent-tools/",
    ".venv/",
    ".cache/",
    "node_modules/",
)

EXCLUDE_GLOBS = {
    "__pycache__",
    ".pyc",
    ".DS_Store",
}

# Remote paths we must not delete during prune (CAI runtime / user data)
PRUNE_PROTECT_PREFIXES = (
    ".git/",
    ".local/",
    ".cache/",
    ".ivy2/",
    ".venv/",
    "config/lakehouse/.checkpoints/",
    "config/lakehouse/last-job-run.json",
)


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def current_branch() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(ROOT),
    )
    return result.stdout.strip()


def assert_branch() -> None:
    branch = current_branch()
    if branch != REQUIRE_BRANCH:
        raise SystemExit(
            f"Refusing CAI sync on branch {branch!r}; checkout {REQUIRE_BRANCH!r} first "
            f"(or unset CAI_REQUIRE_BRANCH)."
        )


def git_tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        capture_output=True,
        check=True,
        cwd=str(ROOT),
    )
    files: list[str] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8")
        if _excluded(rel):
            continue
        files.append(rel)
    return sorted(files)


def extra_local_files() -> list[str]:
    raw = os.getenv(
        "CAI_SYNC_EXTRA",
        "config/kafka/kafka-ca.crt,config/kafka/oauth-ca.crt",
    )
    files: list[str] = []
    for part in raw.split(","):
        rel = part.strip()
        if not rel:
            continue
        path = ROOT / rel
        if path.is_file():
            files.append(rel)
    return files


def build_manifest() -> list[str]:
    manifest = sorted(set(git_tracked_files()) | set(extra_local_files()))
    return manifest


def _excluded(rel: str) -> bool:
    if any(rel.startswith(prefix) for prefix in EXCLUDE_PREFIXES):
        return True
    return any(part in EXCLUDE_GLOBS for part in rel.split("/"))


def api_request(method: str, path: str, payload: dict | None = None, *, timeout: int = 120) -> tuple[int, object]:
    url = f"{CAI_BASE}{path}"
    headers = {"Authorization": f"Bearer {CAI_KEY}", "Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=ssl_context()) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: object = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"raw": body[:1000]}
        return exc.code, parsed


def list_remote_files() -> list[str]:
    paths: list[str] = []
    page_token = ""
    while True:
        query = f"/api/v2/projects/{CAI_PID}/files?page_size=500"
        if page_token:
            query += f"&page_token={page_token}"
        status, payload = api_request("GET", query)
        if status != 200:
            raise RuntimeError(f"list files failed: status={status} payload={payload}")
        if isinstance(payload, dict):
            entries = payload.get("files") or payload.get("project_files") or []
            page_token = payload.get("next_page_token") or payload.get("nextPageToken") or ""
        elif isinstance(payload, list):
            entries = payload
            page_token = ""
        else:
            entries = []
            page_token = ""
        for entry in entries:
            if isinstance(entry, dict):
                path = entry.get("path") or entry.get("name") or ""
            else:
                path = str(entry)
            if path:
                paths.append(path)
        if not page_token:
            break
    return sorted(set(paths))


def delete_remote_file(rel_path: str) -> None:
    url = f"{CAI_BASE}/api/v2/projects/{CAI_PID}/files/{rel_path}"
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {CAI_KEY}"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(request, timeout=60, context=ssl_context()) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        if exc.code not in {404, 400}:
            raise


def read_upload_bytes(local_path: Path) -> bytes:
    content = local_path.read_bytes()
    # CAI rejects zero-byte uploads with EOF; keep git content otherwise unchanged.
    if not content:
        content = b"\n"
    return content


def is_prefix_conflict(remote_path: str, manifest: set[str]) -> bool:
    if remote_path.endswith("/"):
        return False
    prefix = remote_path + "/"
    return any(path.startswith(prefix) for path in manifest)


def should_prune_remote(rel_path: str, manifest: set[str]) -> bool:
    if rel_path in manifest:
        return False
    if any(rel_path.startswith(prefix) for prefix in PRUNE_PROTECT_PREFIXES):
        return False
    if rel_path.endswith("/"):
        return False
    return True


def cleanup_remote(*, manifest: list[str], prune: bool) -> int:
    manifest_set = set(manifest)
    remote = list_remote_files()
    deleted = 0
    for rel_path in remote:
        if rel_path in manifest_set:
            continue
        if not prune and not is_prefix_conflict(rel_path, manifest_set):
            continue
        if not should_prune_remote(rel_path, manifest_set) and not is_prefix_conflict(
            rel_path, manifest_set
        ):
            continue
        delete_remote_file(rel_path)
        print(f"removed stale {rel_path}")
        deleted += 1
    return deleted


def upload_file(rel_path: str, *, attempts: int = 5) -> None:
    local_path = ROOT / rel_path
    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    delete_remote_file(rel_path)

    boundary = "----datapulse-upload"
    content = read_upload_bytes(local_path)
    content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
    body_parts = [
        f"--{boundary}\r\n".encode(),
        f'Content-Disposition: form-data; name="file"; filename="{local_path.name}"\r\n'.encode(),
        f"Content-Type: {content_type}\r\n\r\n".encode(),
        content,
        b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ]
    payload = b"".join(body_parts)

    url = f"{CAI_BASE}/api/v2/projects/{CAI_PID}/files/{rel_path}"
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Bearer {CAI_KEY}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120, context=ssl_context()) as response:
                result = response.read().decode("utf-8", errors="replace")
                print(f"uploaded {rel_path}: {response.status} {result[:80]}")
                return
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            last_error = RuntimeError(f"HTTP {exc.code} for {rel_path}: {body[:500]}")
            retryable = exc.code in {408, 429, 500, 502, 503, 504} or "EOF" in body
        except (TimeoutError, urllib.error.URLError) as exc:
            last_error = exc
            retryable = True
        if attempt < attempts and retryable:
            delay = min(2**attempt, 16)
            print(f"retry {rel_path} attempt={attempt}/{attempts} sleep={delay}s", file=sys.stderr)
            time.sleep(delay)
            continue
        break
    raise RuntimeError(f"upload failed for {rel_path}") from last_error


def cmd_upload(*, prune: bool) -> int:
    assert_branch()
    if not CAI_BASE or not CAI_PID or not CAI_KEY:
        print("CAI_BASE, CAI_PID, and CAI_KEY are required", file=sys.stderr)
        return 1

    manifest = build_manifest()
    print(f"branch={current_branch()} manifest_files={len(manifest)} project={CAI_PID}")

    removed = cleanup_remote(manifest=manifest, prune=prune)
    if removed:
        print(f"pre-upload cleanup removed {removed} remote file(s)")

    for index, rel_path in enumerate(manifest, start=1):
        upload_file(rel_path)
        if index % 20 == 0:
            time.sleep(0.5)

    if prune:
        removed_after = cleanup_remote(manifest=manifest, prune=True)
        print(f"post-upload prune removed {removed_after} remote-only file(s)")
    return 0


def cmd_verify() -> int:
    manifest = set(build_manifest())
    remote = set(list_remote_files())
    missing = sorted(manifest - remote)
    extra = sorted(
        path
        for path in remote - manifest
        if not any(path.startswith(prefix) for prefix in PRUNE_PROTECT_PREFIXES)
    )
    print(json.dumps({"manifest": len(manifest), "remote": len(remote), "missing": missing[:30], "extra": extra[:30]}, indent=2))
    return 1 if missing else 0


def cmd_deploy() -> int:
    code = cmd_upload(prune=False)
    if code != 0:
        return code
    deploy = ROOT / "scripts" / "cai_deploy.py"
    return subprocess.call([sys.executable, str(deploy), "--skip-upload"], cwd=str(ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("upload", "verify", "deploy"))
    parser.add_argument("--prune", action="store_true", help="Delete CAI files not in manifest")
    parser.add_argument(
        "--allow-branch",
        help="Override CAI_REQUIRE_BRANCH for this run (e.g. feature branch smoke test)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    global REQUIRE_BRANCH
    if args.allow_branch:
        REQUIRE_BRANCH = args.allow_branch
    if args.action == "upload":
        return cmd_upload(prune=args.prune)
    if args.action == "verify":
        return cmd_verify()
    if args.action == "deploy":
        return cmd_deploy()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
