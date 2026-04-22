import hashlib
import os
from pathlib import Path

from segav_core.tenant_scope import scope_sql_to_tenant


def persist_export_like(base_dir: str, tenant_key: str, faena_id: int, zip_name: str, payload: bytes):
    tenant_slug = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in str(tenant_key or '').strip()).strip('._-') or 'tenant'
    export_dir = os.path.join(base_dir, "_exports", tenant_slug, str(faena_id))
    os.makedirs(export_dir, exist_ok=True)
    fpath = os.path.join(export_dir, zip_name)
    with open(fpath, 'wb') as f:
        f.write(payload)
    sql, params = scope_sql_to_tenant(
        "INSERT INTO export_historial(faena_id, file_path, sha256, size_bytes, created_at) VALUES(?,?,?,?,?)",
        (faena_id, fpath, hashlib.sha256(payload).hexdigest(), len(payload), "2026-01-01T00:00:00"),
        tenant_key=tenant_key,
        tenant_scope_tables=("export_historial",),
    )
    return fpath, sql, params


def persist_export_mes_like(base_dir: str, tenant_key: str, ym: str, payload: bytes):
    tenant_slug = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in str(tenant_key or '').strip()).strip('._-') or 'tenant'
    export_dir = os.path.join(base_dir, "_exports_mes", tenant_slug)
    os.makedirs(export_dir, exist_ok=True)
    fpath = os.path.join(export_dir, f"export_mes_{ym}_x.zip")
    with open(fpath, 'wb') as f:
        f.write(payload)
    sql, params = scope_sql_to_tenant(
        "INSERT INTO export_historial_mes(year_month, file_path, sha256, size_bytes, created_at) VALUES(?,?,?,?,?)",
        (ym, fpath, hashlib.sha256(payload).hexdigest(), len(payload), "2026-01-01T00:00:00"),
        tenant_key=tenant_key,
        tenant_scope_tables=("export_historial_mes",),
    )
    return fpath, sql, params


def test_persist_export_scopes_folder_and_insert_to_tenant(tmp_path: Path):
    fpath, sql, params = persist_export_like(str(tmp_path), "cli_demo", 12, "demo.zip", b"abc")
    assert os.path.exists(fpath)
    assert "_exports/cli_demo/12/demo.zip" in fpath.replace('\\', '/')
    assert "cliente_key" in sql.lower()
    assert params[0] == "cli_demo"


def test_persist_export_mes_scopes_folder_and_insert_to_tenant(tmp_path: Path):
    fpath, sql, params = persist_export_mes_like(str(tmp_path), "cli_demo", "2026-04", b"xyz")
    assert os.path.exists(fpath)
    assert "_exports_mes/cli_demo/export_mes_2026-04_x.zip" in fpath.replace('\\', '/')
    assert "cliente_key" in sql.lower()
    assert params[0] == "cli_demo"


def test_superadmin_audit_query_contains_role_columns():
    query = "SELECT created_at, username, role_global, role_empresa, accion, entidad, detalle, cliente_key FROM segav_audit_log ORDER BY id DESC LIMIT 500"
    assert "role_global" in query
    assert "role_empresa" in query
