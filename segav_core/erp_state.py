from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import streamlit as st

from core_db import DB_BACKEND, PG_DSN_FINGERPRINT, clear_app_caches, execute, fetch_df
from segav_core.auth import current_user
from segav_core.bootstrap import set_segav_erp_config_value
from segav_core.catalogs import DOC_EMPRESA_MENSUALES, DOC_EMPRESA_REQUERIDOS, DOC_OBLIGATORIOS, ERP_CLIENT_PARAM_DEFAULTS, ERP_TEMPLATE_PRESETS
from segav_core.app_config import APP_NAME


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_erp_config_map(_backend: str, _dsn: str):
    df = fetch_df("SELECT config_key, config_value FROM segav_erp_config ORDER BY config_key")
    if df is None or df.empty:
        return {}
    return {str(r['config_key']): str(r['config_value'] or '') for _, r in df.iterrows()}


def segav_erp_config_map():
    return get_segav_erp_config_map(DB_BACKEND, PG_DSN_FINGERPRINT)


def segav_erp_value(key: str, default: str = "") -> str:
    return str(segav_erp_config_map().get(key, default) or default)


def erp_brand_name() -> str:
    return segav_erp_value('erp_name', APP_NAME)


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_cargos_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT cargo_key, cargo_label, sort_order, activo FROM segav_erp_cargos ORDER BY sort_order, cargo_label")
    return df if df is not None else pd.DataFrame()


def segav_cargos_df():
    return get_segav_cargos_df(DB_BACKEND, PG_DSN_FINGERPRINT)


def segav_cargo_labels(active_only: bool = True) -> list[str]:
    df = segav_cargos_df()
    if df is None or df.empty:
        return []
    if active_only and 'activo' in df.columns:
        df = df[df['activo'].fillna(1).astype(int) == 1]
    labels = [str(v).strip() for v in df['cargo_label'].tolist() if str(v).strip()]
    return labels


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_cargo_rules(_backend: str, _dsn: str):
    df = fetch_df(
        """
        SELECT c.cargo_label, d.doc_tipo, d.sort_order
          FROM segav_erp_docs_cargo d
          LEFT JOIN segav_erp_cargos c ON c.cargo_key=d.cargo_key
         ORDER BY COALESCE(c.sort_order,9999), COALESCE(d.sort_order,9999), d.doc_tipo
        """
    )
    if df is None or df.empty:
        return {}
    rules = {}
    for _, r in df.iterrows():
        cargo = str(r.get('cargo_label') or '').strip()
        doc_tipo = str(r.get('doc_tipo') or '').strip()
        if not cargo or not doc_tipo:
            continue
        rules.setdefault(cargo, []).append(doc_tipo)
    return {k: list(dict.fromkeys(v)) for k, v in rules.items()}


def segav_cargo_rules():
    rules = get_segav_cargo_rules(DB_BACKEND, PG_DSN_FINGERPRINT)
    return rules or {}


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_empresa_docs_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT doc_tipo, obligatorio, mensual, por_mandante, por_faena, sort_order FROM segav_erp_docs_empresa ORDER BY sort_order, doc_tipo")
    return df if df is not None else pd.DataFrame()


def segav_empresa_docs_df():
    return get_segav_empresa_docs_df(DB_BACKEND, PG_DSN_FINGERPRINT)


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_templates_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT template_key, template_label, vertical, description, payload_json, sort_order, activo FROM segav_erp_templates ORDER BY sort_order, template_label")
    return df if df is not None else pd.DataFrame()


def segav_templates_df():
    return get_segav_templates_df(DB_BACKEND, PG_DSN_FINGERPRINT)


def segav_template_payload(template_key: str) -> dict:
    df = segav_templates_df()
    if df is not None and not df.empty:
        row = df[df['template_key'].astype(str) == str(template_key)]
        if not row.empty:
            raw = str(row.iloc[0].get('payload_json') or '')
            try:
                return json.loads(raw) if raw else {}
            except Exception:
                return {}
    return dict(ERP_TEMPLATE_PRESETS.get(str(template_key), {}))


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_clientes_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT cliente_key, cliente_nombre, rut, vertical, modo_implementacion, activo, contacto, email, observaciones, created_at, updated_at FROM segav_erp_clientes ORDER BY COALESCE(activo,1) DESC, cliente_nombre")
    return df if df is not None else pd.DataFrame()


def segav_clientes_df():
    return get_segav_clientes_df(DB_BACKEND, PG_DSN_FINGERPRINT)


def current_segav_client_key() -> str:
    return segav_erp_value('current_client_key', '')


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_cliente_params_df(_backend: str, _dsn: str, cliente_key: str):
    if not cliente_key:
        return pd.DataFrame(columns=['cliente_key','param_key','param_value'])
    df = fetch_df("SELECT cliente_key, param_key, param_value FROM segav_erp_parametros_cliente WHERE cliente_key=? ORDER BY param_key", (cliente_key,))
    return df if df is not None else pd.DataFrame()


def segav_cliente_params(cliente_key: str) -> dict:
    df = get_segav_cliente_params_df(DB_BACKEND, PG_DSN_FINGERPRINT, str(cliente_key or ''))
    if df is None or df.empty:
        return dict(ERP_CLIENT_PARAM_DEFAULTS)
    params = {str(r.get('param_key') or ''): str(r.get('param_value') or '') for _, r in df.iterrows()}
    merged = dict(ERP_CLIENT_PARAM_DEFAULTS)
    merged.update(params)
    return merged


def apply_segav_template(template_key: str):
    payload = segav_template_payload(template_key)
    if not payload:
        return False, 'Plantilla no disponible.'
    now = datetime.now().isoformat(timespec='seconds')
    cargos = [str(c).strip().upper() for c in payload.get('cargos', []) if str(c).strip()]
    cargo_rules = payload.get('cargo_rules', {}) or {}
    empresa_docs = [str(d).strip() for d in payload.get('empresa_docs', []) if str(d).strip()]
    for idx, cargo in enumerate(cargos, start=1):
        execute("DELETE FROM segav_erp_cargos WHERE cargo_key=?", (cargo,))
        execute("INSERT INTO segav_erp_cargos(cargo_key, cargo_label, sort_order, activo, updated_at) VALUES(?,?,?,?,?)", (cargo, cargo, idx, 1, now))
        docs = [str(d).strip() for d in cargo_rules.get(cargo, DOC_OBLIGATORIOS) if str(d).strip()]
        execute("DELETE FROM segav_erp_docs_cargo WHERE cargo_key=?", (cargo,))
        for d_idx, doc_tipo in enumerate(list(dict.fromkeys(docs)), start=1):
            execute("INSERT INTO segav_erp_docs_cargo(cargo_key, doc_tipo, sort_order, updated_at) VALUES(?,?,?,?)", (cargo, doc_tipo, d_idx, now))
    execute("DELETE FROM segav_erp_docs_empresa", ())
    for idx, doc_tipo in enumerate(list(dict.fromkeys(empresa_docs)), start=1):
        execute("INSERT INTO segav_erp_docs_empresa(doc_tipo, obligatorio, mensual, por_mandante, por_faena, sort_order, updated_at) VALUES(?,?,?,?,?,?,?)", (doc_tipo, 1, 1, 1, 1, idx, now))
    set_segav_erp_config_value('template_actual', template_key)
    if payload.get('vertical'):
        set_segav_erp_config_value('erp_vertical', str(payload.get('vertical')))
    clear_app_caches()
    return True, f"Plantilla {payload.get('label') or template_key} aplicada al catálogo ERP."


def get_empresa_required_doc_types() -> list[str]:
    df = segav_empresa_docs_df()
    if df is None or df.empty:
        return list(DOC_EMPRESA_REQUERIDOS)
    df = df[df['obligatorio'].fillna(1).astype(int) == 1]
    docs = [str(v).strip() for v in df['doc_tipo'].tolist() if str(v).strip()]
    return docs or list(DOC_EMPRESA_REQUERIDOS)


def get_empresa_monthly_doc_types() -> list[str]:
    df = segav_empresa_docs_df()
    if df is None or df.empty:
        return list(DOC_EMPRESA_MENSUALES)
    df = df[df['mensual'].fillna(1).astype(int) == 1]
    docs = [str(v).strip() for v in df['doc_tipo'].tolist() if str(v).strip()]
    return docs or list(DOC_EMPRESA_MENSUALES)


def sgsst_log(modulo: str, accion: str, detalle: str = ""):
    try:
        user = current_user()["username"] if current_user() else "sistema"
    except Exception:
        user = "sistema"
    try:
        execute(
            "INSERT INTO sgsst_auditoria(modulo, accion, detalle, usuario, created_at) VALUES(?,?,?,?,?)",
            (modulo, accion, detalle, user, datetime.now().isoformat(timespec='seconds')),
        )
    except Exception:
        pass
