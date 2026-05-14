from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from core_db import DB_BACKEND, PG_DSN, execute, fetch_df

UTC = timezone.utc
DEFAULT_TABLES = [
    "segav_erp_clientes",
    "segav_erp_parametros_cliente",
    "user_client_access",
    "users",
    "faenas",
    "trabajadores",
    "empresa_documentos",
    "faena_empresa_documentos",
    "trabajador_documentos",
    "sgsst_alertas",
    "segav_integrations",
    "segav_integration_events",
    "segav_rule_engine_rules",
    "segav_signature_requests",
    "segav_mobile_inspections",
]


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_table_dump(table: str):
    try:
        df = fetch_df(f"SELECT * FROM {table}")
        rows = [] if df is None or df.empty else df.fillna("").to_dict(orient="records")
        return {"rows": rows, "count": len(rows)}
    except Exception as exc:
        return {"rows": [], "count": 0, "error": str(exc)}


def build_json_backup(out_dir: str | Path = "dist", tables: Iterable[str] | None = None) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out = out_dir / f"segav_backup_{stamp}.json"
    payload = {"generated_at": _now(), "db_backend": DB_BACKEND, "dsn_present": bool(str(PG_DSN or "").strip()), "tables": {}}
    for table in (tables or DEFAULT_TABLES):
        payload["tables"][table] = _safe_table_dump(table)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def build_backup_manifest(artifact_path: str | Path, *, mode: str, tables: Iterable[str] | None = None) -> Path:
    artifact = Path(artifact_path)
    manifest = artifact.with_suffix(artifact.suffix + ".manifest.json")
    table_stats = {}
    for table in (tables or DEFAULT_TABLES):
        bundle = _safe_table_dump(table)
        table_stats[table] = {"count": bundle.get("count", 0), "error": bundle.get("error", "")}
    payload = {
        "generated_at": _now(),
        "mode": mode,
        "db_backend": DB_BACKEND,
        "artifact": artifact.name,
        "artifact_sha256": _sha256_file(artifact),
        "artifact_bytes": artifact.stat().st_size if artifact.exists() else 0,
        "tables": table_stats,
    }
    manifest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def build_native_postgres_backup(out_dir: str | Path = "dist") -> tuple[Path, str]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    if DB_BACKEND == "postgres" and shutil.which("pg_dump") and str(PG_DSN or "").strip():
        out = out_dir / f"segav_pg_backup_{stamp}.dump"
        subprocess.run(["pg_dump", "--dbname", str(PG_DSN), "--format", "custom", "--file", str(out)], check=True, env=os.environ.copy())
        build_backup_manifest(out, mode="pg_dump")
        return out, "pg_dump"
    out = build_json_backup(out_dir=out_dir)
    build_backup_manifest(out, mode="json")
    return out, "json"


def restore_json_backup(path_str: str | Path):
    payload = json.loads(Path(path_str).read_text(encoding="utf-8"))
    upsert_targets = {
        "segav_erp_clientes": ["cliente_key", "cliente_nombre", "rut", "vertical", "modo_implementacion", "activo", "contacto", "email", "observaciones", "created_at", "updated_at"],
        "segav_erp_parametros_cliente": ["cliente_key", "param_key", "param_value", "updated_at"],
        "segav_integrations": ["cliente_key", "integration_type", "provider", "config_json", "is_active", "last_sync_at", "last_status", "created_at", "updated_at"],
        "segav_rule_engine_rules": ["cliente_key", "rule_name", "target_scope", "severity", "rule_json", "is_active", "created_at", "updated_at"],
        "segav_signature_requests": ["cliente_key", "title", "document_type", "signer_name", "signer_email", "signature_level", "status", "metadata_json", "requested_at", "signed_at"],
        "segav_mobile_inspections": ["cliente_key", "faena_id", "inspection_type", "inspector_name", "gps_lat", "gps_lng", "observations", "evidence_json", "signature_name", "status", "created_at", "synced_at"],
    }
    for table, bundle in payload.get("tables", {}).items():
        rows = bundle.get("rows", []) if isinstance(bundle, dict) else []
        if table not in upsert_targets or not isinstance(rows, list):
            continue
        cols = upsert_targets[table]
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT INTO {table}({','.join(cols)}) VALUES({placeholders})"
        for row in rows:
            execute(sql, tuple(row.get(c) for c in cols))
    return {"status": "restore_completed", "tables": list(payload.get("tables", {}).keys())}


def restore_native_postgres_backup(path_str: str | Path):
    path = Path(path_str)
    if path.suffix == ".json":
        return restore_json_backup(path)
    if DB_BACKEND == "postgres" and shutil.which("pg_restore") and str(PG_DSN or "").strip():
        subprocess.run(["pg_restore", "--clean", "--if-exists", "--no-owner", "--dbname", str(PG_DSN), str(path)], check=True, env=os.environ.copy())
        return {"status": "restore_completed", "mode": "pg_restore", "artifact": path.name}
    raise RuntimeError("No fue posible restaurar con pg_restore; usa respaldo JSON o instala pg_restore.")
