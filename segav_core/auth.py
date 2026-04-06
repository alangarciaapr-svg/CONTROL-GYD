"""Autenticación, roles y gestión base de usuarios para SEGAV ERP.

Fase 3: extrae auth/usuarios del archivo principal manteniendo la lógica actual.
"""

from __future__ import annotations

import os
import json
import base64
import hashlib
import secrets
from typing import Callable

import streamlit as st

from core_db import DB_BACKEND, execute, fetch_df

AUTH_ITERATIONS = 200_000

DEFAULT_PERMS = {
    "view_dashboard": True,
    "view_sgsst": True,
    "view_mandantes": True,
    "view_contratos": True,
    "view_faenas": True,
    "view_trabajadores": True,
    "view_docs_empresa": True,
    "view_docs_empresa_faena": True,
    "view_asignaciones": True,
    "view_docs_trabajador": True,
    "view_export": True,
    "view_backup": True,
    "manage_users": False,
}

ALL_PERM_KEYS = list(DEFAULT_PERMS.keys())
SUPERADMIN_PERMS = {k: True for k in ALL_PERM_KEYS}
USER_ROLE_OPTIONS = ["SUPERADMIN", "ADMIN", "OPERADOR", "LECTOR"]

ROLE_TEMPLATES = {
    "SUPERADMIN": SUPERADMIN_PERMS.copy(),
    "ADMIN": {**DEFAULT_PERMS, "manage_users": True},
    "OPERADOR": {**DEFAULT_PERMS, "manage_users": False},
    "LECTOR": {
        "view_dashboard": True,
        "view_sgsst": True,
        "view_mandantes": True,
        "view_contratos": True,
        "view_faenas": True,
        "view_trabajadores": True,
        "view_docs_empresa": True,
        "view_docs_empresa_faena": True,
        "view_asignaciones": True,
        "view_docs_trabajador": True,
        "view_export": True,
        "view_backup": False,
        "manage_users": False,
    },
}

def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")

def _b64d(s: str) -> bytes:
    return base64.b64decode((s or "").encode("utf-8"))

def hash_password(password: str, salt_b64: str | None = None) -> tuple[str, str]:
    if not password:
        raise ValueError("Password vacío")
    salt = _b64d(salt_b64) if salt_b64 else secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, AUTH_ITERATIONS)
    return _b64e(salt), _b64e(dk)

def verify_password(password: str, salt_b64: str, hash_b64: str) -> bool:
    try:
        _, h = hash_password(password, salt_b64=salt_b64)
        return secrets.compare_digest(h, hash_b64)
    except Exception:
        return False

def perms_from_row(role: str, perms_json: str | None):
    role = (role or "OPERADOR").upper()
    if role == "SUPERADMIN":
        return SUPERADMIN_PERMS.copy()
    perms = ROLE_TEMPLATES.get(role, ROLE_TEMPLATES["OPERADOR"]).copy()
    if perms_json:
        try:
            extra = json.loads(perms_json)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k in perms:
                        perms[k] = bool(v)
        except Exception:
            pass
    return perms

def ensure_users_table():
    if DB_BACKEND == "postgres":
        execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                salt_b64 TEXT NOT NULL,
                pass_hash_b64 TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'OPERADOR',
                perms_json TEXT,
                is_active BIGINT NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        return

    execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            salt_b64 TEXT NOT NULL,
            pass_hash_b64 TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'OPERADOR',
            perms_json TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")


def users_count() -> int:
    try:
        df = fetch_df("SELECT COUNT(*) AS n FROM users")
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0

def admins_count(active_only: bool = True) -> int:
    try:
        if active_only:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='ADMIN' AND is_active=1")
        else:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='ADMIN'")
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0

def superadmins_count(active_only: bool = True) -> int:
    try:
        if active_only:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='SUPERADMIN' AND is_active=1")
        else:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='SUPERADMIN'")
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0

def ensure_superadmin_exists():
    try:
        ensure_users_table()
        if superadmins_count(active_only=False) > 0:
            return
        src = fetch_df("SELECT id FROM users WHERE role='ADMIN' ORDER BY is_active DESC, id ASC LIMIT 1")
        if src.empty:
            return
        uid = int(src.iloc[0]["id"])
        execute(
            "UPDATE users SET role=?, perms_json=?, updated_at=datetime('now') WHERE id=?",
            ("SUPERADMIN", json.dumps(SUPERADMIN_PERMS), uid),
        )
    except Exception:
        pass


def auth_set_session(user_row: dict):
    st.session_state["auth_user"] = {
        "id": int(user_row["id"]),
        "username": str(user_row["username"]),
        "role": str(user_row.get("role") or "OPERADOR"),
        "perms": perms_from_row(str(user_row.get("role") or "OPERADOR"), user_row.get("perms_json")),
    }

def auth_logout():
    st.session_state.pop("auth_user", None)
    st.rerun()

def current_user():
    return st.session_state.get("auth_user")

def has_perm(perm: str) -> bool:
    u = current_user()
    if not u:
        return False
    if str(u.get("role") or "").upper() == "SUPERADMIN":
        return True
    return bool(u.get("perms", {}).get(perm, False))

def require_perm(perm: str):
    if not has_perm(perm):
        st.error("No tienes permisos para acceder a esta sección.")
        st.stop()

def auth_gate_ui(render_brand_logo: Callable | None = None, auto_backup_callback: Callable | None = None, brand_name: str = "SEGAV ERP"):
    """Pantalla de inicio: login con roles. Si la base está vacía, crea ADMIN por defecto."""

    st.markdown(
        """
        <style>
        .auth-wrap{max-width:980px;margin:0 auto;}
        .auth-card{border:1px solid rgba(255,255,255,0.08);border-radius:18px;padding:18px 18px 6px 18px;background:rgba(15,23,42,0.25);box-shadow:0 10px 30px rgba(0,0,0,0.18);}
        .auth-title{font-size:28px;font-weight:800;line-height:1.1;margin:6px 0 4px 0;}
        .auth-sub{opacity:0.85;margin:0 0 10px 0;}
        .auth-badge{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;opacity:0.9;border:1px solid rgba(255,255,255,0.12);}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="auth-wrap">', unsafe_allow_html=True)
    cL, cR = st.columns([1.1, 1], gap="large")

    with cL:
        try:
            render_brand_logo(width=260)
        except Exception:
            pass
        st.markdown(f'<div class="auth-title">{brand_name}</div>', unsafe_allow_html=True)
        st.markdown('<div class="auth-sub">Accede con tu usuario y contraseña para gestionar mandantes, faenas, trabajadores y exportaciones.</div>', unsafe_allow_html=True)
        st.markdown('<span class="auth-badge">🔒 Acceso seguro · Roles y poderes</span>', unsafe_allow_html=True)
        st.markdown("")
        st.caption("Consejo: para usuarios que solo revisan, usa rol **LECTOR**. Para operación diaria, **OPERADOR**.")

    with cR:
        st.markdown('<div class="auth-card">', unsafe_allow_html=True)

        # Asegura tabla users
        ensure_users_table()
        ensure_superadmin_exists()

        # Seed automático del SUPERADMIN por defecto si la tabla está vacía
        if users_count() == 0:
            DEFAULT_ADMIN_USER = os.environ.get("DEFAULT_ADMIN_USER", "a.garcia")
            DEFAULT_ADMIN_PASS = os.environ.get("DEFAULT_ADMIN_PASS", "225188")
            try:
                salt_b64, h_b64 = hash_password(DEFAULT_ADMIN_PASS)
                perms_json = json.dumps(SUPERADMIN_PERMS)
                execute(
                    "INSERT INTO users(username, salt_b64, pass_hash_b64, role, perms_json, is_active) VALUES(?,?,?,?,?,1)",
                    (DEFAULT_ADMIN_USER, salt_b64, h_b64, "SUPERADMIN", perms_json),
                )
                auto_backup_callback("users_seed_default_superadmin") if callable(auto_backup_callback) else None
            except Exception:
                # Si ya existe o hay algún problema, continuamos hacia login
                pass

        st.markdown("### Iniciar sesión")
        with st.form("form_login"):
            username = st.text_input("Usuario", placeholder="ej: a.garcia")
            password = st.text_input("Contraseña", type="password")
            ok = st.form_submit_button("Ingresar", type="primary", use_container_width=True)

        if ok:
            u = (username or "").strip()
            if not u or not password:
                st.error("Usuario y contraseña son obligatorios.")
                st.stop()
            df = fetch_df("SELECT * FROM users WHERE username=? AND is_active=1", (u,))
            if df.empty:
                st.error("Usuario no existe o está desactivado.")
                st.stop()
            row = df.iloc[0].to_dict()
            if not verify_password(password, row["salt_b64"], row["pass_hash_b64"]):
                st.error("Contraseña incorrecta.")
                st.stop()
            auth_set_session(row)
            st.success("Ingreso exitoso.")
            st.rerun()

        st.caption("¿Olvidaste tu contraseña? Pide al **ADMIN** que la reinicie desde **Usuarios**.")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()



