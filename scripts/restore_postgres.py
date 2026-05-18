from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from segav_core.backup_restore import restore_native_postgres_backup


def main(path_str: str):
    print(restore_native_postgres_backup(Path(path_str)))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Uso: python scripts/restore_postgres.py dist/segav_backup_xxx.json|.dump")
    main(sys.argv[1])
