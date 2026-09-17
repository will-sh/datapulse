#!/usr/bin/env python3
"""Upload project files to CAI — delegates to cai_project_sync.py (main-aligned)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    script = ROOT / "scripts" / "cai_project_sync.py"
    return subprocess.call([sys.executable, str(script), "upload"], cwd=str(ROOT))


if __name__ == "__main__":
    raise SystemExit(main())
