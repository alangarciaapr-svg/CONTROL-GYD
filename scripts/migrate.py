from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core_db import DB_BACKEND, execute, fetch_value
from segav_core.db_migrations import apply_runtime_migrations, migration_status


def main():
    apply_runtime_migrations(execute, fetch_value, DB_BACKEND)
    for row in migration_status(fetch_value):
        print(f"{row['version_key']} :: {'OK' if row['applied'] else 'PENDIENTE'} :: {row['description']}")


if __name__ == "__main__":
    main()
