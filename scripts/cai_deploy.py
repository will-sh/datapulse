#!/usr/bin/env python3
"""Upload project files to CAI, then recreate applications (delete + create)."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UPLOAD = ROOT / "scripts" / "cai_upload_files.py"
RECREATE = ROOT / "scripts" / "cai_recreate_applications.py"


def run(script: Path, extra_args: list[str]) -> int:
    cmd = [sys.executable, str(script), *extra_args]
    print(f"\n>>> {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=str(ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apps",
        default="datapulse-app,datapulse-spark-consumer,datapulse-monitoring",
        help="Comma-separated application names passed to recreate step",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Only recreate applications; do not upload files",
    )
    parser.add_argument(
        "--skip-recreate",
        action="store_true",
        help="Only upload files; do not recreate applications",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.skip_upload and args.skip_recreate:
        print("Nothing to do: both upload and recreate were skipped", file=sys.stderr)
        return 1

    if not args.skip_upload:
        code = run(UPLOAD, [])
        if code != 0:
            return code

    if not args.skip_recreate:
        code = run(RECREATE, ["--apps", args.apps])
        if code != 0:
            return code

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
