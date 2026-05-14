from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from segav_core.backup_restore import build_native_postgres_backup


def main():
    artifact, mode = build_native_postgres_backup(out_dir=Path("dist"))
    print({"artifact": str(artifact), "mode": mode})


if __name__ == "__main__":
    main()
