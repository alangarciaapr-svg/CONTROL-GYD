from __future__ import annotations

from datetime import datetime


def record_soft_error(context: str, exc: Exception | None = None, session_state=None, limit: int = 30):
    """Guarda errores blandos sin romper la UI.

    Se mantiene desacoplado de Streamlit para permitir tests y uso temprano en el arranque.
    """
    try:
        store = session_state if session_state is not None else _resolve_session_state()
        if store is None:
            return None
        logs = store.setdefault("soft_errors", [])
        logs.append(
            {
                "at": datetime.now().isoformat(timespec="seconds"),
                "context": str(context or "").strip(),
                "message": (str(exc).strip() if exc is not None else "")[:300],
            }
        )
        if len(logs) > int(limit):
            del logs[:-int(limit)]
        return logs[-1]
    except Exception:
        return None


def get_soft_errors(session_state=None) -> list[dict]:
    store = session_state if session_state is not None else _resolve_session_state()
    if store is None:
        return []
    try:
        return list(store.get("soft_errors", []))
    except Exception:
        return []


def clear_soft_errors(session_state=None):
    store = session_state if session_state is not None else _resolve_session_state()
    if store is None:
        return
    try:
        store.pop("soft_errors", None)
    except Exception:
        return


def _resolve_session_state():
    try:
        import streamlit as st

        return st.session_state
    except Exception:
        return None
