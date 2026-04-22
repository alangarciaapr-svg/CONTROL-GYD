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
    """Pantalla de inicio corporativa aprobada, compacta y sin scroll normal."""

    assets_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "branding")
    logo_path = os.path.join(assets_dir, "segav_logo_final.png")
    if not os.path.exists(logo_path):
        logo_path = os.path.join(assets_dir, "segav_logo.png")
    right_img_path = os.path.join(assets_dir, "login_right_exact.png")

    def _img_b64(path: str) -> str:
        try:
            with open(path, "rb") as fp:
                return base64.b64encode(fp.read()).decode("utf-8")
        except Exception:
            return ""

    logo_b64 = _img_b64(logo_path)
    hero_b64 = _img_b64(right_img_path)

    st.markdown(
        f"""
        <style>
        html, body, [data-testid="stAppViewContainer"], .stApp {{
            height: 100vh !important;
            overflow: hidden !important;
            background: #f3f3f3 !important;
        }}
        [data-testid="stHeader"] {{background: transparent;}}
        .block-container {{padding-top: 0 !important; padding-bottom: 0 !important; max-width: 100% !important;}}
        section[data-testid="stSidebar"] {{display: none !important;}}
        div[data-testid="stVerticalBlock"]:has(> .segav-login-shell) {{height: 100vh;}}
        .segav-login-shell {{
            height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
        }}
        .segav-login-frame {{
            width: min(1280px, 90vw);
            height: min(760px, 82vh);
            background: #fff;
            border-radius: 14px;
            box-shadow: 0 2px 12px rgba(16,24,40,.08);
            overflow: hidden;
            display: grid;
            grid-template-columns: 40% 60%;
            border: 1px solid rgba(15,23,42,0.08);
        }}
        .segav-left {{padding: 60px 54px 34px 54px; display:flex; flex-direction:column; justify-content:center;}}
        .segav-left h1 {{font-size: 28px; line-height:1.15; margin:0 0 42px 0; color:#0f2346; font-weight:800;}}
        .segav-form-label {{font-size: 15px; font-weight:700; color:#1b2740; margin:0 0 8px 0;}}
        .segav-forgot {{text-align:center; color:#1e64d4; margin: 6px 0 12px 0; font-size: 14px;}}
        .segav-logo-box {{margin-top: 28px; text-align:center;}}
        .segav-logo-box img {{max-width: 220px; height: auto;}}
        .segav-logo-text {{font-size: 18px; font-weight: 700; color:#1258b8; margin-top: 4px; letter-spacing:.3px;}}
        .segav-divider {{width: 180px; height: 2px; background: #1258b8; opacity:.55; margin: 8px auto 0 auto; border-radius: 999px;}}
        .segav-right {{background:#0d51ad; display:flex; align-items:stretch; justify-content:stretch;}}
        .segav-right img {{width:100%; height:100%; object-fit:cover; display:block;}}
        .segav-login-shell .stForm {{border:none !important; box-shadow:none !important; padding:0 !important; background:transparent !important;}}
        .segav-login-shell div[data-testid="stTextInputRootElement"] input {{height: 52px !important; font-size: 16px !important;}}
        .segav-login-shell div[data-testid="stTextInputRootElement"] {{margin-bottom: 18px;}}
        .segav-login-shell button[kind="primary"] {{height: 52px !important; border-radius: 8px !important; font-size: 17px !important; font-weight:700 !important;}}
        @media (max-width: 1200px) {{
            .segav-login-frame {{width: 95vw; height: min(740px, 88vh);}}
            .segav-left {{padding: 46px 38px 28px 38px;}}
        }}
        @media (max-width: 900px) {{
            html, body, [data-testid="stAppViewContainer"], .stApp {{overflow: auto !important;}}
            .segav-login-shell {{height:auto; min-height:100vh; padding:20px 0;}}
            .segav-login-frame {{grid-template-columns:1fr; width:min(96vw, 760px); height:auto;}}
            .segav-right {{display:none;}}
        }}
        </style>
        <div class="segav-login-shell"><div class="segav-login-frame">
          <div class="segav-left">
            <h1>Acceso al Sistema</h1>
        """,
        unsafe_allow_html=True,
    )

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

    with st.form("form_login"):
        st.markdown('<div class="segav-form-label">Usuario</div>', unsafe_allow_html=True)
        username = st.text_input("Usuario", label_visibility="collapsed", placeholder="Ingrese su usuario")
        st.markdown('<div class="segav-form-label">Contraseña</div>', unsafe_allow_html=True)
        password = st.text_input("Contraseña", type="password", label_visibility="collapsed", placeholder="Ingrese su contraseña")
        st.markdown('<div class="segav-forgot">¿Olvidó su contraseña?</div>', unsafe_allow_html=True)
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

    st.markdown(
        f"""
            <div class="segav-logo-box">
              <img src="data:image/png;base64,{logo_b64}" alt="SEGAV" />
              <div class="segav-logo-text">SEGAV ERP</div>
              <div class="segav-divider"></div>
            </div>
          </div>
          <div class="segav-right"><img src="data:image/png;base64,{hero_b64}" alt="SEGAV ERP" /></div>
        </div></div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

