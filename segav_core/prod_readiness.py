from __future__ import annotations

import os
from shutil import which

DEV_API_SECRET = "segav-dev-secret-change-me"


def environment_readiness() -> list[dict]:
    checks = []
    api_secret = str(os.environ.get("SEGAV_API_SECRET") or "").strip()
    checks.append({
        "key": "api_secret_configured",
        "ok": bool(api_secret and api_secret != DEV_API_SECRET),
        "detail": "SEGAV_API_SECRET configurado y distinto al secreto por defecto." if api_secret and api_secret != DEV_API_SECRET else "Falta SEGAV_API_SECRET robusto o sigue el secreto por defecto.",
    })
    db_pref = str(os.environ.get("SEGAV_DB_BACKEND") or "postgres").strip().lower()
    checks.append({
        "key": "db_backend_preferred",
        "ok": db_pref == "postgres",
        "detail": f"SEGAV_DB_BACKEND={db_pref or 'postgres'}; producción recomendada con postgres.",
    })
    checks.append({
        "key": "pg_dump_available",
        "ok": which("pg_dump") is not None,
        "detail": "pg_dump disponible para respaldos nativos." if which("pg_dump") else "pg_dump no está en PATH; se usará respaldo JSON fallback.",
    })
    checks.append({
        "key": "pg_restore_available",
        "ok": which("pg_restore") is not None,
        "detail": "pg_restore disponible para restauración nativa." if which("pg_restore") else "pg_restore no está en PATH; se usará restore lógico fallback.",
    })
    return checks


def data_readiness(fetch_value, tenant: str | None = None) -> list[dict]:
    def safe(sql: str, params=(), default=0):
        try:
            return fetch_value(sql, params, default=default)
        except Exception:
            return default

    where = " WHERE COALESCE(cliente_key,'')=?" if tenant else ""
    params = (tenant,) if tenant else ()
    rows = []
    for key, table in [
        ("clientes", "segav_erp_clientes"),
        ("usuarios", "users"),
        ("faenas", "faenas"),
        ("trabajadores", "trabajadores"),
    ]:
        n = int(safe(f"SELECT COUNT(*) FROM {table}{where}", params, default=0) or 0)
        rows.append({"key": key, "ok": n > 0, "count": n, "detail": f"{table}: {n} registros"})
    return rows


def summarize_readiness(env_checks: list[dict], data_checks: list[dict]) -> dict:
    checks = list(env_checks) + list(data_checks)
    passed = sum(1 for item in checks if item.get("ok"))
    total = len(checks)
    return {"ok": total > 0 and passed == total, "passed": passed, "total": total, "checks": checks}
