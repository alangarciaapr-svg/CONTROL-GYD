"""SEGAV ERP – Global Search.

Provides a cross-entity search that scans trabajadores, faenas,
mandantes, contratos and documents in a single query.  Results are
returned as a list of dicts with entity type, id, label and match
context.

Usage::

    from segav_core.search import global_search
    results = global_search(fetch_df, tenant_key, "García", allowed_mandante_ids=None)
"""

from __future__ import annotations

import re
from typing import Callable


def _like_pattern(term: str) -> str:
    """Escape and wrap a search term for SQL LIKE."""
    safe = term.replace("%", "").replace("_", "").strip()
    return f"%{safe}%"


def global_search(
    fetch_df: Callable,
    tenant_key: str,
    query: str,
    *,
    allowed_mandante_ids: list[int] | None = None,
    max_results: int = 50,
) -> list[dict]:
    """Search across main entities and return unified results.

    Each result dict has keys:
        - entity: str  ("trabajador", "faena", "mandante", "doc_trabajador", "doc_empresa")
        - id: int
        - label: str   (display name / primary info)
        - detail: str  (secondary info / match context)
        - icon: str    (emoji)
    """
    if not query or not query.strip():
        return []

    term = query.strip()
    like = _like_pattern(term)
    results: list[dict] = []
    limit = max(5, max_results)

    # --- Trabajadores ---
    try:
        mand_filter = ""
        params: list = [tenant_key, like, like, like]
        if allowed_mandante_ids:
            ph = ",".join(["?"] * len(allowed_mandante_ids))
            mand_filter = f" AND EXISTS (SELECT 1 FROM asignaciones a2 JOIN faenas f2 ON f2.id=a2.faena_id WHERE a2.trabajador_id=t.id AND f2.mandante_id IN ({ph}))"
            params.extend(allowed_mandante_ids)
        params.append(limit)
        sql = f"""
            SELECT t.id, t.rut, t.nombres, t.apellidos, COALESCE(t.cargo,'') AS cargo
            FROM trabajadores t
            WHERE COALESCE(t.cliente_key,'')=?
              AND (t.rut LIKE ? OR LOWER(t.nombres||' '||t.apellidos) LIKE LOWER(?) OR LOWER(t.cargo) LIKE LOWER(?))
              {mand_filter}
            ORDER BY t.apellidos, t.nombres
            LIMIT ?
        """
        df = fetch_df(sql, tuple(params))
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                results.append({
                    "entity": "trabajador",
                    "id": int(r["id"]),
                    "label": f"{r['apellidos']}, {r['nombres']}",
                    "detail": f"RUT: {r['rut']} · Cargo: {r['cargo']}",
                    "icon": "👷",
                })
    except Exception:
        pass

    # --- Faenas ---
    try:
        params_f: list = [tenant_key, like, like]
        mand_filter_f = ""
        if allowed_mandante_ids:
            ph = ",".join(["?"] * len(allowed_mandante_ids))
            mand_filter_f = f" AND f.mandante_id IN ({ph})"
            params_f.extend(allowed_mandante_ids)
        params_f.append(limit)
        sql_f = f"""
            SELECT f.id, f.nombre, f.estado, COALESCE(f.direccion,'') AS direccion
            FROM faenas f
            WHERE COALESCE(f.cliente_key,'')=?
              AND (LOWER(f.nombre) LIKE LOWER(?) OR LOWER(COALESCE(f.direccion,'')) LIKE LOWER(?))
              {mand_filter_f}
            ORDER BY f.nombre
            LIMIT ?
        """
        df = fetch_df(sql_f, tuple(params_f))
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                results.append({
                    "entity": "faena",
                    "id": int(r["id"]),
                    "label": str(r["nombre"]),
                    "detail": f"Estado: {r['estado']} · {r['direccion']}",
                    "icon": "🛠️",
                })
    except Exception:
        pass

    # --- Mandantes ---
    try:
        params_m: list = [tenant_key, like, like, limit]
        sql_m = """
            SELECT m.id, m.nombre, COALESCE(m.rut,'') AS rut
            FROM mandantes m
            WHERE COALESCE(m.cliente_key,'')=?
              AND (LOWER(m.nombre) LIKE LOWER(?) OR m.rut LIKE ?)
            ORDER BY m.nombre
            LIMIT ?
        """
        df = fetch_df(sql_m, tuple(params_m))
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                results.append({
                    "entity": "mandante",
                    "id": int(r["id"]),
                    "label": str(r["nombre"]),
                    "detail": f"RUT: {r['rut']}",
                    "icon": "🏢",
                })
    except Exception:
        pass

    # --- Documentos trabajador ---
    try:
        params_d: list = [tenant_key, like, like, limit]
        sql_d = """
            SELECT td.id, td.nombre_archivo, td.doc_tipo, t.nombres, t.apellidos
            FROM trabajador_documentos td
            JOIN trabajadores t ON t.id = td.trabajador_id
            WHERE COALESCE(td.cliente_key,'')=?
              AND (LOWER(td.nombre_archivo) LIKE LOWER(?) OR LOWER(td.doc_tipo) LIKE LOWER(?))
            ORDER BY td.id DESC
            LIMIT ?
        """
        df = fetch_df(sql_d, tuple(params_d))
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                results.append({
                    "entity": "doc_trabajador",
                    "id": int(r["id"]),
                    "label": str(r["nombre_archivo"]),
                    "detail": f"Tipo: {r['doc_tipo']} · Trabajador: {r['apellidos']}, {r['nombres']}",
                    "icon": "📎",
                })
    except Exception:
        pass

    return results[:max_results]


def render_search_sidebar(st_module, fetch_df_fn: Callable, tenant_key: str, allowed_mandante_ids=None, go_fn=None):
    """Render a global search widget in the Streamlit sidebar.

    Parameters
    ----------
    st_module : streamlit
    fetch_df_fn : callable for DB queries
    tenant_key : current tenant
    allowed_mandante_ids : mandante scope for lectores
    go_fn : navigation function go(page, faena_id=None)
    """
    st = st_module
    with st.expander("🔍 Búsqueda global", expanded=False):
        q = st.text_input("Buscar trabajador, faena, mandante…", key="global_search_input", placeholder="RUT, nombre, faena…")
        if q and len(q.strip()) >= 2:
            results = global_search(fetch_df_fn, tenant_key, q, allowed_mandante_ids=allowed_mandante_ids)
            if not results:
                st.caption("Sin resultados")
            else:
                for r in results[:15]:
                    col1, col2 = st.columns([0.08, 0.92])
                    with col1:
                        st.markdown(r["icon"])
                    with col2:
                        btn_label = f"**{r['label']}**  \n{r['detail']}"
                        entity = r["entity"]
                        rid = r["id"]
                        # Navigation button
                        nav_map = {
                            "trabajador": "Trabajadores",
                            "faena": "Faenas",
                            "mandante": "Mandantes",
                            "doc_trabajador": "Documentos Trabajador",
                            "doc_empresa": "Documentos Empresa",
                        }
                        target_page = nav_map.get(entity, "Dashboard")
                        if st.button(
                            f"{r['icon']} {r['label']} — {r['detail']}",
                            key=f"gsearch_{entity}_{rid}",
                            use_container_width=True,
                        ):
                            st.session_state["nav_page"] = target_page
                            if entity == "faena":
                                st.session_state["selected_faena_id"] = rid
                            elif entity == "trabajador":
                                st.session_state["_search_trabajador_id"] = rid
                            st.rerun()
                if len(results) >= 15:
                    st.caption(f"Mostrando 15 de {len(results)} resultados")
