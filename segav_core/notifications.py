"""Confirmaciones visuales globales para acciones de SEGAV ERP.

Objetivo: que cada acción de modificación deje una respuesta visible y
uniforme para el usuario, incluso cuando Streamlit ejecuta `st.rerun()`.
"""
from __future__ import annotations

import re
from typing import Any, Callable

import streamlit as st

_FLASH_KEY = "_segav_action_feedback_queue"
_PATCHED_FLAG = "_segav_action_feedback_installed"

_DESTRUCTIVE_RE = re.compile(
    r"\b(eliminad[oa]s?|borrad[oa]s?|quitad[oa]s?|rechazad[oa]s?|desactivad[oa]s?|revocad[oa]s?)\b",
    re.IGNORECASE,
)

_ACTION_MESSAGES: dict[str, tuple[str, str]] = {
    # Trabajadores
    "trabajador": ("success", "Trabajador creado correctamente."),
    "trabajador_edit": ("success", "Trabajador actualizado correctamente."),
    "trabajador_delete": ("delete", "Trabajador eliminado."),
    "import_excel": ("success", "Importación de trabajadores completada correctamente."),
    "mass_docs_import": ("success", "Importación masiva de documentos completada correctamente."),
    # Asignaciones
    "asignacion": ("success", "Trabajadores asignados correctamente."),
    "asignacion_remove": ("delete", "Trabajadores quitados de la faena."),
    "import_asignar_faena": ("success", "Importación y asignación a faena completada correctamente."),
    # Documentos
    "doc_empresa": ("success", "Documento empresa guardado correctamente."),
    "doc_empresa_delete": ("delete", "Documento empresa eliminado."),
    "doc_empresa_faena_mensual": ("success", "Documento empresa por faena guardado correctamente."),
    "doc_empresa_faena_delete": ("delete", "Documento empresa por faena eliminado."),
    "doc_trabajador": ("success", "Documento de trabajador guardado correctamente."),
    "doc_trabajador_delete": ("delete", "Documento de trabajador eliminado."),
    # Exportaciones y respaldos
    "export_zip": ("success", "ZIP generado y guardado correctamente."),
    "export_zip_mes": ("success", "ZIP mensual generado y guardado correctamente."),
    "backup_restore": ("success", "Backup restaurado correctamente."),
    # Usuarios
    "users_create": ("success", "Usuario creado correctamente o enviado a aprobación."),
    "users_update": ("success", "Usuario actualizado correctamente."),
    "users_delete": ("delete", "Usuario eliminado correctamente."),
    "user_approve": ("success", "Usuario aprobado y activado correctamente."),
    "user_reject": ("delete", "Usuario rechazado correctamente."),
    "company_access_limits": ("success", "Límites de acceso guardados correctamente."),
    "company_user_access": ("success", "Permisos por empresa actualizados correctamente."),
    # Mandantes, contratos y faenas
    "mandante": ("success", "Mandante creado correctamente."),
    "mandante_edit": ("success", "Mandante actualizado correctamente."),
    "mandante_delete": ("delete", "Mandante eliminado."),
    "contrato_faena": ("success", "Contrato de faena creado correctamente."),
    "contrato_edit": ("success", "Contrato actualizado correctamente."),
    "contrato_archivo": ("success", "Archivo de contrato actualizado correctamente."),
    "contrato_delete": ("delete", "Contrato eliminado."),
    "faena": ("success", "Faena creada correctamente."),
    "faena_edit": ("success", "Faena actualizada correctamente."),
    "faena_delete": ("delete", "Faena eliminada."),
    "anexo_faena": ("success", "Anexo de faena guardado correctamente."),
    # Configuración y SGSST
    "segav_config": ("success", "Configuración ERP guardada correctamente."),
    "sgsst_save": ("success", "Registro SGSST guardado correctamente."),
}


def _plain_text(value: Any) -> str:
    try:
        if value is None:
            return ""
        return str(value)
    except Exception:
        return ""


def _looks_destructive(message: Any) -> bool:
    text = _plain_text(message)
    return bool(_DESTRUCTIVE_RE.search(text))


def _call_with_default_icon(func: Callable[..., Any], body: Any, default_icon: str, *args: Any, **kwargs: Any) -> Any:
    # No pisa iconos explícitos enviados por el código existente.
    if not args and "icon" not in kwargs:
        kwargs["icon"] = default_icon
    return func(body, *args, **kwargs)


def install_action_feedback() -> None:
    """Instala wrappers visuales sobre st.success/error/warning una sola vez.

    Esto mantiene compatibilidad con el código existente, pero estandariza iconos
    y fuerza que acciones destructivas comunicadas como `success` aparezcan en rojo.
    """
    if getattr(st, _PATCHED_FLAG, False):
        return

    original_success = st.success
    original_error = st.error
    original_warning = st.warning
    original_info = st.info

    def success(body: Any = None, *args: Any, **kwargs: Any) -> Any:
        if _looks_destructive(body):
            return _call_with_default_icon(original_error, body, "🟥", *args, **kwargs)
        return _call_with_default_icon(original_success, body, "✅", *args, **kwargs)

    def error(body: Any = None, *args: Any, **kwargs: Any) -> Any:
        return _call_with_default_icon(original_error, body, "❌", *args, **kwargs)

    def warning(body: Any = None, *args: Any, **kwargs: Any) -> Any:
        return _call_with_default_icon(original_warning, body, "⚠️", *args, **kwargs)

    def info(body: Any = None, *args: Any, **kwargs: Any) -> Any:
        return _call_with_default_icon(original_info, body, "ℹ️", *args, **kwargs)

    st.success = success  # type: ignore[assignment]
    st.error = error  # type: ignore[assignment]
    st.warning = warning  # type: ignore[assignment]
    st.info = info  # type: ignore[assignment]
    setattr(st, _PATCHED_FLAG, True)


def notify_success(message: str) -> None:
    install_action_feedback()
    st.success(message)


def notify_error(message: str) -> None:
    install_action_feedback()
    st.error(message)


def notify_warning(message: str) -> None:
    install_action_feedback()
    st.warning(message)


def notify_delete(message: str) -> None:
    install_action_feedback()
    # Acción destructiva: rojo aunque técnicamente haya sido exitosa.
    st.error(message, icon="🟥")


def queue_action_feedback(kind: str, message: str) -> None:
    """Guarda una confirmación para mostrarla tras `st.rerun()`."""
    if not message:
        return
    queue = list(st.session_state.get(_FLASH_KEY, []))
    queue.append({"kind": kind or "success", "message": str(message)})
    # Evita acumular mensajes antiguos si el usuario ejecuta muchas acciones rápidas.
    st.session_state[_FLASH_KEY] = queue[-5:]


def queue_action_feedback_from_tag(tag: str | None) -> None:
    """Convierte etiquetas internas de backup/auditoría en mensajes de usuario."""
    key = str(tag or "").strip()
    if not key:
        return
    kind, message = _ACTION_MESSAGES.get(key, _fallback_from_tag(key))
    queue_action_feedback(kind, message)


def render_action_feedback() -> None:
    """Muestra y limpia confirmaciones pendientes."""
    install_action_feedback()
    queue = list(st.session_state.pop(_FLASH_KEY, []) or [])
    for item in queue:
        kind = str(item.get("kind") or "success").lower()
        message = str(item.get("message") or "").strip()
        if not message:
            continue
        if kind in {"delete", "deleted", "destructive"}:
            notify_delete(message)
        elif kind in {"error", "danger"}:
            notify_error(message)
        elif kind in {"warning", "warn"}:
            notify_warning(message)
        else:
            notify_success(message)


def _fallback_from_tag(tag: str) -> tuple[str, str]:
    low = tag.lower()
    entity = tag.replace("_", " ").strip().capitalize() or "Acción"
    if any(word in low for word in ("delete", "remove", "eliminar", "quit")):
        return "delete", f"{entity} eliminado correctamente."
    if any(word in low for word in ("edit", "update", "actualiz")):
        return "success", f"{entity} actualizado correctamente."
    if any(word in low for word in ("create", "insert", "nuevo", "add")):
        return "success", f"{entity} creado correctamente."
    return "success", f"Acción completada correctamente: {entity}."
