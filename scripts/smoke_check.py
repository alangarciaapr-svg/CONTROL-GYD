from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import importlib
from core_db import DB_BACKEND, fetch_value
from segav_core.db_migrations import apply_runtime_migrations


def main():
    api = importlib.import_module("api_rest")
    apply_runtime_migrations(api.execute, api.fetch_value, DB_BACKEND)
    _ = fetch_value("SELECT COUNT(*) FROM segav_schema_migrations", default=0)
    print({"api": api.app.title, "db_backend": DB_BACKEND})


if __name__ == "__main__":
    main()
