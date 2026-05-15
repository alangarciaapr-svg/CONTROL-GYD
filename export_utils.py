from __future__ import annotations

from segav_core.ui_tenant import company_role_for_user

TENANT_HEADER = "x-segav-cliente-key"


def user_allowed_clientes(fetch_df, user_id: int, role: str):
    if str(role or "").upper() == "SUPERADMIN":
        return None
    if int(user_id or 0) <= 0:
        return []
    try:
        df = fetch_df("SELECT cliente_key FROM user_client_access WHERE user_id=? ORDER BY cliente_key", (int(user_id),))
    except Exception:
        return []
    if df is None or df.empty:
        return []
    return [str(x) for x in df["cliente_key"].astype(str).tolist() if str(x).strip()]


def resolve_tenant_for_user(fetch_df, user: dict, requested_tenant: str | None = None, allow_none_for_superadmin: bool = True):
    requested = str(requested_tenant or "").strip()
    allowed = user_allowed_clientes(fetch_df, int((user or {}).get("sub") or 0), str((user or {}).get("role") or "OPERADOR"))
    if allowed is None:
        return requested or (None if allow_none_for_superadmin else "")
    if not allowed:
        return None
    if requested:
        return requested if requested in allowed else None
    return allowed[0]


def visible_clientes_sql(role: str) -> str:
    if str(role or "").upper() == "SUPERADMIN":
        return "SELECT cliente_key, cliente_nombre, rut, vertical, activo FROM segav_erp_clientes ORDER BY cliente_nombre"
    return (
        "SELECT c.cliente_key, c.cliente_nombre, c.rut, c.vertical, c.activo "
        "FROM segav_erp_clientes c JOIN user_client_access a ON a.cliente_key=c.cliente_key "
        "WHERE a.user_id=? ORDER BY c.cliente_nombre"
    )


def audit_api_action(execute, username: str, action: str, detail: str, cliente_key: str = "", *, user_id: int = 0, role_global: str = "API", fetch_df=None):
    role_emp = company_role_for_user(fetch_df, int(user_id or 0), str(cliente_key or ""), str(role_global or "API")) if fetch_df else str(role_global or "API")
    for stamp in ["datetime('now')", "now()"]:
        try:
            execute(
                f"INSERT INTO segav_audit_log(cliente_key, username, user_id, role_global, role_empresa, accion, entidad, detalle, created_at) VALUES(?,?,?,?,?,?,?,?,{stamp})",
                (str(cliente_key or ""), str(username or "api"), int(user_id or 0), str(role_global or "API"), str(role_emp or "API"), str(action or "API"), "api", str(detail or "")),
            )
            return
        except Exception:
            pass
