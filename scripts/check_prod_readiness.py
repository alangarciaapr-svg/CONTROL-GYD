from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core_db import fetch_value
from segav_core.prod_readiness import data_readiness, environment_readiness, summarize_readiness


def main():
    summary = summarize_readiness(environment_readiness(), data_readiness(fetch_value))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if os.environ.get("SEGAV_STRICT_READINESS", "0") == "1" and not summary["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
