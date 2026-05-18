from __future__ import annotations

import json

COMPANY_ROLE_DEFAULTS = {
    "SUPERADMIN": {
        "view_dashboard": True, "view_sgsst": True, "view_mandantes": True, "view_contratos": True,
        "view_faenas": True, "view_trabajadores": True, "view_docs_empresa": True, "view_docs_empresa_faena": True,
        "view_asignaciones": True, "view_docs_trabajador": True, "view_export": True, "view_backup": True,
        "manage_users": True, "approve_legal_docs": True, "view_legal_audit": True,
    },
    "ADMIN": {
        "view_dashboard": True, "view_sgsst": True, "view_mandantes": True, "view_contratos": True,
        "view_faenas": True, "view_trabajadores": True, "view_docs_empresa": True, "view_docs_empresa_faena": True,
        "view_asignaciones": True, "view_docs_trabajador": True, "view_export": True, "view_backup": False,
        "manage_users": True, "approve_legal_docs": True, "view_legal_audit": True,
    },
    "OPERADOR": {
        "view_dashboard": True, "view_sgsst": True, "view_mandantes": True, "view_contratos": True,
        "view_faenas": True, "view_trabajadores": True, "view_docs_empresa": True, "view_docs_empresa_faena": True,
        "view_asignaciones": True, "view_docs_trabajador": True, "view_export": True, "view_backup": False,
        "manage_users": False, "approve_legal_docs": False, "view_legal_audit": False,
    },
    "LECTOR": {
        "view_dashboard": True, "view_sgsst": True, "view_mandantes": True, "view_contratos": True,
        "view_faenas": True, "view_trabajadores": True, "view_docs_empresa": True, "view_docs_empresa_faena": True,
        "view_asignaciones": True, "view_docs_trabajador": True, "view_export": True, "view_backup": False,
        "manage_users": False, "approve_legal_docs": False, "view_legal_audit": True,
    },
}

def ensure_user_client_module_perms_table(execute, db_backend: str):
    if db_backend == "postgres":
        execute("""
        CREATE TABLE IF NOT EXISTS user_client_module_perms (
            user_id BIGINT NOT NULL,
            cliente_key TEXT NOT NULL,
            perms_json TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, cliente_key)
        );
        """)
        return
    execute("""
    CREATE TABLE IF NOT EXISTS user_client_module_perms (
        user_id INTEGER NOT NULL,
        cliente_key TEXT NOT NULL,
        perms_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (user_id, cliente_key)
    );
    """)

def _clean_perms(raw, allowed_keys):
    base = {k: False for k in allowed_keys}
    try:
        data = json.loads(raw or "{}") if not isinstance(raw, dict) else raw
    except Exception:
        data = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if k in base:
                base[k] = bool(v)
    return base

def effective_company_perms(fetch_df, user_id: int, cliente_key: str, global_role: str, global_perms: dict, allowed_perm_keys, company_role: str):
    grole = str(global_role or "OPERADOR").upper()
    if grole == "SUPERADMIN":
        return {k: True for k in allowed_perm_keys}
    role_key = str(company_role or grole or "OPERADOR").upper()
    base = {k: bool(global_perms.get(k, False)) for k in allowed_perm_keys}
    for k, v in COMPANY_ROLE_DEFAULTS.get(role_key, COMPANY_ROLE_DEFAULTS["OPERADOR"]).items():
        if k in base:
            base[k] = bool(v)
    try:
        df = fetch_df(
            "SELECT perms_json FROM user_client_module_perms WHERE user_id=? AND cliente_key=? LIMIT 1",
            (int(user_id or 0), str(cliente_key or "").strip()),
        )
        if df is not None and not df.empty:
            overrides = _clean_perms(df.iloc[0].get("perms_json"), allowed_perm_keys)
            for k in allowed_perm_keys:
                if k in overrides:
                    base[k] = bool(overrides[k])
    except Exception:
        pass
    return base
