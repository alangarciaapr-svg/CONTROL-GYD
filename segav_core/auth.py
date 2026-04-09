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
    """Pantalla de acceso corporativa full-screen, sin scroll normal en escritorio."""

    def _img_b64(path: str) -> str:
        try:
            with open(path, "rb") as fp:
                return base64.b64encode(fp.read()).decode("utf-8")
        except Exception:
            return ""

    logo_path = "assets/branding/segav_logo_login.png"
    if not os.path.exists(logo_path):
        logo_path = "assets/branding/segav_logo.png"
    hero_path = "assets/branding/login_hero_approved.png"
    if not os.path.exists(hero_path):
        hero_path = "assets/branding/login_hero_segav.svg"

    logo_b64 = _img_b64(logo_path)
    hero_is_svg = str(hero_path).lower().endswith('.svg')
    hero_inline = ""
    if hero_is_svg:
        try:
            hero_inline = open(hero_path, 'r', encoding='utf-8').read()
        except Exception:
            hero_inline = ""
    hero_b64 = _img_b64(hero_path) if not hero_is_svg else ""

    st.markdown(
        f"""
        <style>
        html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stAppViewContainer"] > .main {{
            height: 100vh !important;
            overflow: hidden !important;
            background: #eef2f7;
        }}
        [data-testid="stSidebar"], header[data-testid="stHeader"], #MainMenu, footer {{display:none !important;}}
        .block-container {{padding: 0 !important; max-width: 100% !important;}}
        .segav-auth-shell {{
            height: 100vh;
            display:flex;
            align-items:center;
            justify-content:center;
            padding: 20px 26px;
            box-sizing:border-box;
            overflow:hidden;
        }}
        .segav-auth-card {{
            width:min(1300px, 96vw);
            height:min(760px, calc(100vh - 40px));
            background:#ffffff;
            border-radius:18px;
            overflow:hidden;
            box-shadow:0 16px 50px rgba(15, 23, 42, 0.16);
            border:1px solid rgba(15, 23, 42, 0.08);
            display:grid;
            grid-template-columns: 41% 59%;
        }}
        .segav-auth-left {{
            background:#fff;
            display:flex;
            flex-direction:column;
            justify-content:center;
            padding:44px 56px 34px 56px;
            min-width:0;
        }}
        .segav-auth-title {{font-size:32px; font-weight:800; color:#10264d; margin:0 0 34px 0;}}
        .segav-label {{font-size:19px; font-weight:700; color:#16233b; margin:10px 0 10px 0;}}
        .segav-help {{text-align:center; color:#1b63d0; font-size:16px; margin:12px 0 18px 0;}}
        .segav-logo-wrap {{margin-top:22px; text-align:center;}}
        .segav-logo-wrap img {{max-width:230px; width:52%; min-width:150px; height:auto; display:block; margin:0 auto;}}
        .segav-logo-text {{font-size:20px; font-weight:800; color:#1450a8; margin-top:8px; letter-spacing:0.3px;}}
        .segav-rule {{height:2px; background:linear-gradient(90deg, transparent, #3b82f6 16%, #3b82f6 84%, transparent); width:74%; margin:4px auto 0; opacity:0.7;}}
        .segav-auth-right {{
            position:relative;
            background:linear-gradient(135deg, #034da7 0%, #0a64ca 100%);
            display:flex;
            align-items:stretch;
            justify-content:center;
            min-width:0;
        }}
        .segav-auth-right img, .segav-auth-right svg {{width:100%; height:100%; object-fit:cover; display:block;}}
        div[data-testid="stForm"] {{border:none !important; padding:0 !important; background:transparent !important;}}
        div[data-testid="stTextInputRootElement"] input {{height:58px !important; font-size:18px !important;}}
        div[data-testid="stTextInput"] label p {{font-size:19px !important; font-weight:700 !important; color:#16233b !important;}}
        div[data-testid="stForm"] button[kind="primary"], .stButton > button {{height:56px !important; border-radius:10px !important; font-size:20px !important; font-weight:700 !important; background:#0e63d8 !important;}}
        @media (max-width: 980px) {{
            .segav-auth-card {{grid-template-columns: 1fr; height:auto; max-height:none; overflow:auto;}}
            .segav-auth-right {{display:none;}}
            html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stAppViewContainer"] > .main {{overflow:auto !important;}}
            .segav-auth-shell {{height:auto; min-height:100vh; overflow:auto;}}
        }}
        </style>
        <div class="segav-auth-shell"><div class="segav-auth-card"><div class="segav-auth-left"> 
        """,
        unsafe_allow_html=True,
    )

    # Ensure base user tables/seed
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

    st.markdown('<div class="segav-auth-title">Acceso al Sistema</div>', unsafe_allow_html=True)
    with st.form("form_login"):
        username = st.text_input("Usuario", placeholder="Ingrese su usuario")
        password = st.text_input("Contraseña", type="password", placeholder="Ingrese su contraseña")
        st.markdown('<div class="segav-help">¿Olvidó su contraseña?</div>', unsafe_allow_html=True)
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

    if logo_b64:
        st.markdown(
            f'<div class="segav-logo-wrap"><img src="data:image/png;base64,{logo_b64}" alt="SEGAV"><div class="segav-rule"></div><div class="segav-logo-text">SEGAV ERP</div></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown('<div class="segav-logo-wrap"><div class="segav-logo-text">SEGAV ERP</div></div>', unsafe_allow_html=True)

    st.markdown('</div><div class="segav-auth-right">', unsafe_allow_html=True)
    if hero_is_svg and hero_inline:
        st.markdown(hero_inline, unsafe_allow_html=True)
    elif hero_b64:
        st.markdown(f'<img src="data:image/png;base64,{hero_b64}" alt="SEGAV Hero">', unsafe_allow_html=True)
    st.markdown('</div></div></div>', unsafe_allow_html=True)
    st.stop()



