from __future__ import annotations

import io
from datetime import date, datetime

import pandas as pd
import streamlit as st

from segav_core.formatters import clean_rut, format_rut_chileno


def _format_rut_session_value(key: str):
    st.session_state[key] = format_rut_chileno(st.session_state.get(key, ""))


def rut_input(label: str, *, key: str, value: str = "", placeholder: str = "12.345.678-9", help: str | None = None):
    current_value = st.session_state.get(key, value)
    formatted_value = format_rut_chileno(current_value)
    if st.session_state.get(key) != formatted_value:
        st.session_state[key] = formatted_value
    return st.text_input(
        label,
        key=key,
        placeholder=placeholder,
        help=help,
        on_change=_format_rut_session_value,
        args=(key,),
    )


def _reset_trabajador_create_state():
    defaults = {
        "trabajador_create_rut": "",
        "trabajador_create_nombres": "",
        "trabajador_create_apellidos": "",
        "trabajador_create_cargo": "",
        "trabajador_create_cc": "",
        "trabajador_create_email": "",
        "trabajador_create_fc": None,
        "trabajador_create_ve": None,
    }
    for _k, _v in defaults.items():
        st.session_state[_k] = _v


def _apply_pending_trabajador_create_reset():
    if st.session_state.pop("_trabajador_create_reset_pending", False):
        _reset_trabajador_create_state()


def _show_pending_trabajador_create_flash():
    msg = st.session_state.pop("_trabajador_create_flash", None)
    if msg:
        st.success(msg)


def build_trabajadores_template_xlsx() -> bytes:
    ejemplo = pd.DataFrame([
        {
            "RUT": "12.345.678-5",
            "NOMBRE": "Juan Carlos Perez Soto",
            "CARGO": "Operador",
            "CENTRO_COSTO": "FAENA A",
            "EMAIL": "juan.perez@empresa.cl",
            "FECHA DE CONTRATO": "2026-03-30",
            "VIGENCIA_EXAMEN": "2026-12-31",
        }
    ])
    instrucciones = pd.DataFrame(
        {
            "Campo": [
                "RUT",
                "NOMBRE",
                "CARGO",
                "CENTRO_COSTO",
                "EMAIL",
                "FECHA DE CONTRATO",
                "VIGENCIA_EXAMEN",
            ],
            "Obligatorio": ["Sí", "Sí", "No", "No", "No", "No", "No"],
            "Detalle": [
                "RUT chileno. La app lo normaliza al formato XX.XXX.XXX-X.",
                "Nombre completo del trabajador.",
                "Cargo o función.",
                "Centro de costo o faena.",
                "Correo electrónico.",
                "Fecha en formato YYYY-MM-DD o fecha Excel.",
                "Fecha en formato YYYY-MM-DD o fecha Excel.",
            ],
        }
    )
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        ejemplo.to_excel(writer, sheet_name="Trabajadores", index=False)
        instrucciones.to_excel(writer, sheet_name="Instrucciones", index=False)
    return out.getvalue()
