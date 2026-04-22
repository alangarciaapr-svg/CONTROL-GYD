from __future__ import annotations

import pandas as pd


def allowed_client_keys_for_user(fetch_df, user_id: int, role: str):
    if str(role or "").upper() == "SUPERADMIN":
        return None
    if int(user_id or 0) <= 0:
        return []
    try:
        df = fetch_df(
            "SELECT cliente_key FROM user_client_access WHERE user_id=? ORDER BY cliente_key",
            (int(user_id),),
        )
    except Exception:
        return []
    if df is None or df.empty or "cliente_key" not in df.columns:
        return []
    keys = [str(x).strip() for x in df["cliente_key"].astype(str).tolist() if str(x).strip()]
    return list(dict.fromkeys(keys))


def filter_visible_clientes_df(clientes_df, allowed_client_keys, *, is_superadmin: bool = False):
    if clientes_df is None:
        return pd.DataFrame()
    if getattr(clientes_df, "empty", True):
        return clientes_df.copy()
    df = clientes_df.copy()
    if is_superadmin or allowed_client_keys is None:
        return df
    allowed = {str(x).strip() for x in (allowed_client_keys or []) if str(x).strip()}
    if not allowed:
        return df.iloc[0:0].copy()
    return df[df["cliente_key"].astype(str).isin(allowed)].copy()


def resolve_active_client_key(visible_clientes_df, *preferred_keys: str):
    if visible_clientes_df is None or getattr(visible_clientes_df, "empty", True):
        return ""
    df = visible_clientes_df.copy()
    keys = [str(x).strip() for x in df["cliente_key"].astype(str).tolist() if str(x).strip()]
    for candidate in preferred_keys:
        cand = str(candidate or "").strip()
        if cand and cand in keys:
            return cand
    active_df = df
    if "activo" in active_df.columns:
        try:
            active_df = active_df[active_df["activo"].fillna(1).astype(int) == 1]
        except Exception:
            active_df = df
    if active_df is not None and not active_df.empty:
        val = str(active_df.iloc[0].get("cliente_key") or "").strip()
        if val:
            return val
    return keys[0] if keys else ""


def client_key_is_visible(visible_clientes_df, client_key: str) -> bool:
    key = str(client_key or "").strip()
    if not key or visible_clientes_df is None or getattr(visible_clientes_df, "empty", True):
        return False
    try:
        keys = set(visible_clientes_df["cliente_key"].astype(str).tolist())
    except Exception:
        return False
    return key in keys


def active_company_admin_flag(fetch_df, user_id: int, cliente_key: str) -> bool:
    if int(user_id or 0) <= 0 or not str(cliente_key or "").strip():
        return False
    try:
        df = fetch_df(
            "SELECT COALESCE(is_company_admin,0) AS is_company_admin FROM user_client_access WHERE user_id=? AND cliente_key=? LIMIT 1",
            (int(user_id), str(cliente_key).strip()),
        )
        if df is None or df.empty:
            return False
        return bool(int(df.iloc[0].get("is_company_admin", 0) or 0))
    except Exception:
        return False



def company_role_for_user(fetch_df, user_id: int, cliente_key: str, global_role: str = "OPERADOR") -> str:
    grole = str(global_role or "OPERADOR").upper()
    if grole == "SUPERADMIN":
        return "SUPERADMIN"
    if int(user_id or 0) <= 0 or not str(cliente_key or "").strip():
        return grole if grole in {"ADMIN", "OPERADOR", "LECTOR"} else "OPERADOR"
    try:
        df = fetch_df(
            "SELECT COALESCE(is_company_admin,0) AS is_company_admin, COALESCE(role_empresa,'') AS role_empresa FROM user_client_access WHERE user_id=? AND cliente_key=? LIMIT 1",
            (int(user_id), str(cliente_key).strip()),
        )
        if df is None or df.empty:
            return grole if grole in {"ADMIN", "OPERADOR", "LECTOR"} else "OPERADOR"
        role_emp = str(df.iloc[0].get("role_empresa") or "").strip().upper()
        is_admin = bool(int(df.iloc[0].get("is_company_admin", 0) or 0))
        if role_emp in {"SUPERADMIN", "ADMIN", "OPERADOR", "LECTOR"}:
            if is_admin and role_emp in {"OPERADOR", "LECTOR"}:
                return "ADMIN"
            return role_emp
        if is_admin:
            return "ADMIN"
    except Exception:
        pass
    return grole if grole in {"ADMIN", "OPERADOR", "LECTOR"} else "OPERADOR"


def company_caps_for_user(fetch_df, user_id: int, cliente_key: str, global_role: str = "OPERADOR") -> dict:
    role_emp = company_role_for_user(fetch_df, user_id, cliente_key, global_role=global_role)
    grole = str(global_role or "OPERADOR").upper()
    is_super = grole == "SUPERADMIN" or role_emp == "SUPERADMIN"
    can_manage_users = is_super or role_emp == "ADMIN"
    can_write = is_super or role_emp in {"ADMIN", "OPERADOR"}
    can_delete_files = is_super or role_emp == "ADMIN"
    can_view_audit = is_super or role_emp == "ADMIN"
    return {
        "role_empresa": role_emp,
        "can_manage_users": bool(can_manage_users),
        "can_write": bool(can_write),
        "can_delete_files": bool(can_delete_files),
        "can_view_audit": bool(can_view_audit),
    }


def _tenant_storage_prefix(cliente_key: str) -> str:
    safe = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in str(cliente_key or '').strip())
    safe = safe.strip('._-') or 'tenant'
    return f"clientes/{safe}/"


def tenant_object_path_allowed(object_path: str, cliente_key: str, *, is_superadmin: bool = False) -> bool:
    op = str(object_path or '').strip().lstrip('/')
    if not op:
        return False
    if is_superadmin:
        return True
    ckey = str(cliente_key or '').strip()
    if not ckey:
        return False
    return op.startswith(_tenant_storage_prefix(ckey))
