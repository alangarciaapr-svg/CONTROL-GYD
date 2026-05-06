"""SEGAV ERP – Persistent Notifications.

Stores notifications in a database table so they survive page reloads
and are visible across sessions.  Supports per-user and broadcast
notifications with read/unread state.

Tables created by bootstrap:
    segav_notifications (id, cliente_key, user_id, username, category,
                         title, body, link_page, is_read, created_at)
"""

from __future__ import annotations

from datetime import datetime
from typing import Callable

# Notification categories
CAT_USER_PENDING = "user_pending"       # New user awaiting approval
CAT_DOC_EXPIRED = "doc_expired"         # Document expired
CAT_DOC_UPLOADED = "doc_uploaded"       # New document uploaded
CAT_APPROVAL_REQ = "approval_request"  # Legal approval requested
CAT_SYSTEM = "system"                   # System/generic

CATEGORY_ICONS = {
    CAT_USER_PENDING: "👤",
    CAT_DOC_EXPIRED: "⚠️",
    CAT_DOC_UPLOADED: "📎",
    CAT_APPROVAL_REQ: "✅",
    CAT_SYSTEM: "🔔",
}

CATEGORY_LABELS = {
    CAT_USER_PENDING: "Usuario pendiente",
    CAT_DOC_EXPIRED: "Documento vencido",
    CAT_DOC_UPLOADED: "Documento cargado",
    CAT_APPROVAL_REQ: "Aprobación solicitada",
    CAT_SYSTEM: "Sistema",
}

# SQL for table creation (SQLite)
CREATE_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS segav_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cliente_key TEXT NOT NULL DEFAULT '',
    user_id INTEGER,
    username TEXT DEFAULT '',
    category TEXT NOT NULL DEFAULT 'system',
    title TEXT NOT NULL,
    body TEXT DEFAULT '',
    link_page TEXT DEFAULT '',
    is_read INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

CREATE_TABLE_POSTGRES = """
CREATE TABLE IF NOT EXISTS segav_notifications (
    id SERIAL PRIMARY KEY,
    cliente_key TEXT NOT NULL DEFAULT '',
    user_id INTEGER,
    username TEXT DEFAULT '',
    category TEXT NOT NULL DEFAULT 'system',
    title TEXT NOT NULL,
    body TEXT DEFAULT '',
    link_page TEXT DEFAULT '',
    is_read INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
"""

CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_segav_notif_user ON segav_notifications(user_id, is_read);"


def ensure_notifications_table(execute_fn: Callable, db_backend: str = "sqlite") -> None:
    """Create the notifications table if it doesn't exist."""
    try:
        sql = CREATE_TABLE_POSTGRES if db_backend == "postgres" else CREATE_TABLE_SQLITE
        execute_fn(sql)
        execute_fn(CREATE_INDEX)
    except Exception:
        pass  # Table may already exist


def send_notification(
    execute_fn: Callable,
    *,
    cliente_key: str = "",
    user_id: int | None = None,
    username: str = "",
    category: str = CAT_SYSTEM,
    title: str = "",
    body: str = "",
    link_page: str = "",
) -> None:
    """Insert a notification record."""
    if not title:
        return
    try:
        execute_fn(
            "INSERT INTO segav_notifications(cliente_key, user_id, username, category, title, body, link_page) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (str(cliente_key), user_id, str(username), category, title, body, link_page),
        )
    except Exception:
        pass


def get_unread_count(fetch_value_fn: Callable, user_id: int | None = None, is_superadmin: bool = False) -> int:
    """Return the number of unread notifications for a user."""
    try:
        if is_superadmin:
            # Superadmin sees all unread
            return int(fetch_value_fn(
                "SELECT COUNT(*) FROM segav_notifications WHERE is_read=0",
                (), default=0, fresh=True,
            ) or 0)
        if user_id:
            return int(fetch_value_fn(
                "SELECT COUNT(*) FROM segav_notifications WHERE is_read=0 AND (user_id=? OR user_id IS NULL)",
                (user_id,), default=0, fresh=True,
            ) or 0)
    except Exception:
        pass
    return 0


def get_notifications(
    fetch_df_fn: Callable,
    user_id: int | None = None,
    is_superadmin: bool = False,
    *,
    limit: int = 50,
    unread_only: bool = False,
):
    """Return recent notifications as a DataFrame."""
    try:
        where = "WHERE 1=1"
        params: list = []
        if not is_superadmin and user_id:
            where += " AND (user_id=? OR user_id IS NULL)"
            params.append(user_id)
        if unread_only:
            where += " AND is_read=0"
        params.append(limit)
        sql = f"SELECT * FROM segav_notifications {where} ORDER BY id DESC LIMIT ?"
        return fetch_df_fn(sql, tuple(params))
    except Exception:
        return None


def mark_as_read(execute_fn: Callable, notification_id: int) -> None:
    """Mark a single notification as read."""
    try:
        execute_fn("UPDATE segav_notifications SET is_read=1 WHERE id=?", (notification_id,))
    except Exception:
        pass


def mark_all_read(execute_fn: Callable, user_id: int | None = None, is_superadmin: bool = False) -> None:
    """Mark all notifications as read for a user or all (superadmin)."""
    try:
        if is_superadmin:
            execute_fn("UPDATE segav_notifications SET is_read=1 WHERE is_read=0")
        elif user_id:
            execute_fn(
                "UPDATE segav_notifications SET is_read=1 WHERE is_read=0 AND (user_id=? OR user_id IS NULL)",
                (user_id,),
            )
    except Exception:
        pass


def render_notification_badge(st_module, count: int) -> None:
    """Render a notification count badge in the sidebar."""
    if count > 0:
        badge_text = str(count) if count < 100 else "99+"
        st_module.markdown(
            f'<div class="segav-sidecard segav-sidebar-center" style="background:linear-gradient(135deg,#fee2e2,#fecaca);border-color:#fca5a5;">'
            f'<strong style="color:#dc2626;">🔔 {badge_text} notificación{"es" if count != 1 else ""} sin leer</strong></div>',
            unsafe_allow_html=True,
        )


def render_notification_panel(st_module, fetch_df_fn, execute_fn, user_id, is_superadmin_flag, go_fn=None):
    """Render a full notification panel (for sidebar expander or page)."""
    st = st_module
    df = get_notifications(fetch_df_fn, user_id, is_superadmin_flag, limit=30)
    if df is None or df.empty:
        st.caption("No hay notificaciones recientes.")
        return

    if st.button("Marcar todas como leídas", key="notif_mark_all_read", use_container_width=True):
        mark_all_read(execute_fn, user_id, is_superadmin_flag)
        st.rerun()

    for _, row in df.iterrows():
        nid = int(row.get("id", 0))
        cat = str(row.get("category", "system"))
        icon = CATEGORY_ICONS.get(cat, "🔔")
        title = str(row.get("title", ""))
        body = str(row.get("body", ""))
        is_read = int(row.get("is_read", 0))
        link_page = str(row.get("link_page", ""))
        created = str(row.get("created_at", ""))[:16]

        style = "opacity:0.6;" if is_read else "font-weight:600;"
        st.markdown(
            f'<div style="{style} padding:4px 0; border-bottom:1px solid rgba(0,0,0,0.06);">'
            f'{icon} <strong>{title}</strong><br>'
            f'<span style="font-size:0.85em; opacity:0.7;">{body} · {created}</span></div>',
            unsafe_allow_html=True,
        )
        cols = st.columns([0.5, 0.5])
        if not is_read:
            with cols[0]:
                if st.button("✓ Leída", key=f"notif_read_{nid}", use_container_width=True):
                    mark_as_read(execute_fn, nid)
                    st.rerun()
        if link_page and go_fn:
            with cols[1]:
                if st.button(f"Ir a {link_page}", key=f"notif_go_{nid}", use_container_width=True):
                    st.session_state["nav_page"] = link_page
                    st.rerun()
