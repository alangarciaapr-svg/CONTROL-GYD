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
    """Pantalla de inicio corporativa, compacta y sin scroll normal en escritorio."""
    import base64
    from pathlib import Path

    st.markdown(
        """
        <style>
        html, body, .stApp {
            height: 100vh;
            overflow: hidden;
            margin: 0 !important;
            padding: 0 !important;
            background: #ffffff;
        }
        [data-testid="stHeader"], header, footer, #MainMenu {visibility:hidden !important; height:0 !important;}
        .block-container {
            padding-top: 0 !important;
            padding-bottom: 0 !important;
            padding-left: 0 !important;
            padding-right: 0 !important;
            max-width: 100% !important;
        }
        section.main > div, .main, [data-testid="stAppViewContainer"], [data-testid="stAppViewBlockContainer"] {
            padding-top: 0 !important;
            margin-top: 0 !important;
        }
        .login-shell {
            min-height: 100vh;
            max-height: 100vh;
            overflow: hidden;
            display: flex;
            align-items: stretch;
            background: #ffffff;
        }
        .login-left {
            display:flex;
            flex-direction:column;
            justify-content:center;
            min-height: 100vh;
            padding: 40px 46px 28px 46px;
            background: #ffffff;
        }
        .login-card {
            max-width: 430px;
            margin: 0 auto;
            width: 100%;
        }
        .login-eyebrow {
            color: #0F3B63;
            font-size: 13px;
            font-weight: 700;
            letter-spacing: .08em;
            text-transform: uppercase;
            margin-bottom: 10px;
        }
        .login-title {
            color: #0f172a;
            font-size: 34px;
            line-height: 1.08;
            font-weight: 800;
            margin: 0 0 10px 0;
        }
        .login-subtitle {
            color: #475569;
            font-size: 15px;
            line-height: 1.55;
            margin: 0 0 18px 0;
        }
        .login-logo-wrap {
            text-align:center;
            margin-top: 18px;
        }
        .login-brand-title {
            color:#0A2540;
            font-weight:700;
            font-size:18px;
            margin-top: 8px;
            letter-spacing:.02em;
        }
        .login-right {
            min-height: 100vh;
            padding: 0;
            overflow: hidden;
            background: #0A2540;
            display:flex;
            align-items:stretch;
            justify-content:stretch;
        }
        .login-right img {
            width: 100%;
            height: 100vh;
            object-fit: cover;
            object-position: center center;
            display:block;
        }
        div[data-testid="stForm"] {
            border: 1px solid rgba(15, 59, 99, 0.10);
            border-radius: 18px;
            padding: 18px 18px 10px 18px;
            box-shadow: 0 12px 26px rgba(15, 23, 42, 0.08);
            background: #ffffff;
        }
        div[data-testid="stForm"] button[kind="primary"] {
            border-radius: 12px !important;
            min-height: 46px;
            font-weight: 700;
        }
        @media (max-width: 1100px) {
            html, body, .stApp {overflow: auto;}
            .login-shell {min-height:auto; max-height:none; overflow:visible;}
            .login-left, .login-right {min-height:auto;}
            .login-right img {height:auto; max-height:420px;}
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    def _img64(p: str) -> str:
        try:
            return base64.b64encode(Path(p).read_bytes()).decode("utf-8")
        except Exception:
            return ""

    logo_path = "assets/branding/segav_logo.png"
    right_panel_path = "login_panel_blue.png"
    logo64 = _img64(logo_path)
    hero64 = _img64(right_panel_path)

    st.markdown('<div class="login-shell">', unsafe_allow_html=True)
    cL, cR = st.columns([0.42, 0.58], gap="small")

    with cL:
        st.markdown('<div class="login-left"><div class="login-card">', unsafe_allow_html=True)

        ensure_users_table()
        ensure_superadmin_exists()

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
                pass

        st.markdown('<div class="login-eyebrow">Bienvenido</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-title">Iniciar sesión</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-subtitle">Accede al ERP para gestionar cumplimiento, documentación, trabajadores y operación multiempresa.</div>', unsafe_allow_html=True)

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

        st.caption("¿Olvidaste tu contraseña? Pide al administrador que la reinicie desde Usuarios.")

        if logo64:
            st.markdown(
                f'<div class="login-logo-wrap"><img src="data:image/png;base64,{logo64}" width="120"><div class="login-brand-title">SEGAV ERP</div></div>',
                unsafe_allow_html=True,
            )

        st.markdown('</div></div>', unsafe_allow_html=True)

    with cR:
        st.markdown('<div class="login-right">', unsafe_allow_html=True)
        if hero64:
            st.markdown(f'<img src="data:image/png;base64,{hero64}" alt="SEGAV ERP panel">', unsafe_allow_html=True)
        else:
            st.markdown(
                """
                <div style="padding:80px;color:white;">
                    <h1 style="font-size:42px;margin:0 0 16px 0;">SEGAV ERP</h1>
                    <p style="font-size:18px;line-height:1.6;max-width:620px;">Plataforma integral para control documental, cumplimiento legal y gestión operativa.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
    st.stop()



