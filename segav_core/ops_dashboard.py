
from __future__ import annotations

from datetime import date, timedelta
from typing import Callable

import pandas as pd
import streamlit as st

from segav_core.ops_compliance import build_auto_alerts, ensure_multiempresa_compliance_schema


def _safe_current_client(clientes_df: pd.DataFrame, current_client_key: str) -> dict:
    if clientes_df is None or clientes_df.empty:
        return {"cliente_key": current_client_key or "cli_default", "cliente_nombre": "Empresa activa", "vertical": "General", "modo_implementacion": "CONFIGURABLE"}
    keys = clientes_df["cliente_key"].astype(str).tolist()
    key = str(current_client_key or "").strip()
    if not key or key not in keys:
        row = clientes_df.iloc[0]
    else:
        row = clientes_df[clientes_df["cliente_key"].astype(str) == key].iloc[0]
    return row.to_dict()


def _safe_fetch(fetch_df: Callable, query: str, params=()):
    try:
        df = fetch_df(query, params)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _safe_value(fetch_value: Callable, query: str, params=(), default=0):
    try:
        return fetch_value(query, params, default=default)
    except Exception:
        return default


def _program_stats(fetch_df: Callable, client_key: str) -> dict:
    df = _safe_fetch(
        fetch_df,
        "SELECT estado, avance, fecha_compromiso, actividad FROM sgsst_programa_anual WHERE COALESCE(cliente_key,'')=? ORDER BY fecha_compromiso ASC, id DESC",
        (client_key,),
    )
    if df.empty:
        return {
            "total": 0,
            "cerradas": 0,
            "abiertas": 0,
            "cerradas_pct": 0.0,
            "avance_promedio": 0.0,
            "vencidas": pd.DataFrame(),
        }
    work = df.copy()
    work["estado"] = work["estado"].fillna("PENDIENTE").astype(str)
    work["avance"] = pd.to_numeric(work.get("avance"), errors="coerce").fillna(0)
    today = date.today().isoformat()
    work["is_closed"] = work["estado"].str.upper().eq("CERRADO")
    work["is_overdue"] = (~work["is_closed"]) & work["fecha_compromiso"].fillna("").astype(str).str.strip().ne("") & (work["fecha_compromiso"].astype(str) < today)
    total = int(len(work))
    closed = int(work["is_closed"].sum())
    open_n = int(total - closed)
    return {
        "total": total,
        "cerradas": closed,
        "abiertas": open_n,
        "cerradas_pct": round((closed / total) * 100.0, 1) if total else 0.0,
        "avance_promedio": round(float(work["avance"].mean()), 1) if total else 0.0,
        "vencidas": work[work["is_overdue"]][["actividad", "fecha_compromiso", "estado"]].head(12).reset_index(drop=True),
    }


def _training_stats(fetch_df: Callable, client_key: str) -> dict:
    df = _safe_fetch(
        fetch_df,
        "SELECT tipo, tema, fecha, vigencia, estado FROM sgsst_capacitaciones WHERE COALESCE(cliente_key,'')=? ORDER BY COALESCE(vigencia, fecha) ASC, id DESC",
        (client_key,),
    )
    if df.empty:
        return {
            "total": 0,
            "vigentes": 0,
            "por_vencer": 0,
            "vencidas": 0,
            "vigentes_pct": 0.0,
            "agenda": pd.DataFrame(),
        }
    work = df.copy()
    today = date.today().isoformat()
    soon = (date.today() + timedelta(days=30)).isoformat()
    vig = work["vigencia"].fillna("").astype(str)
    work["is_vencida"] = vig.str.strip().ne("") & (vig < today)
    work["is_por_vencer"] = vig.str.strip().ne("") & (vig >= today) & (vig <= soon)
    work["is_vigente"] = ~work["is_vencida"]
    total = int(len(work))
    vigentes = int(work["is_vigente"].sum())
    por_vencer = int(work["is_por_vencer"].sum())
    vencidas = int(work["is_vencida"].sum())
    agenda = work[work["is_por_vencer"] | work["is_vencida"]][["tipo", "tema", "vigencia", "estado"]].head(12).reset_index(drop=True)
    return {
        "total": total,
        "vigentes": vigentes,
        "por_vencer": por_vencer,
        "vencidas": vencidas,
        "vigentes_pct": round((vigentes / total) * 100.0, 1) if total else 0.0,
        "agenda": agenda,
    }


def _actions_stats(fetch_df: Callable, client_key: str) -> dict:
    df = _safe_fetch(
        fetch_df,
        "SELECT severidad, estado, fecha_limite FROM sgsst_alertas WHERE COALESCE(cliente_key,'')=? ORDER BY id DESC",
        (client_key,),
    )
    if df.empty:
        return {"abiertas": 0, "vencidas": 0, "altas": 0}
    work = df.copy()
    today = date.today().isoformat()
    estado = work["estado"].fillna("ABIERTA").astype(str).str.upper()
    abiertas = estado.eq("ABIERTA")
    altas = abiertas & work["severidad"].fillna("").astype(str).isin(["CRITICA", "ALTA"])
    vencidas = abiertas & work["fecha_limite"].fillna("").astype(str).str.strip().ne("") & (work["fecha_limite"].astype(str) < today)
    return {
        "abiertas": int(abiertas.sum()),
        "vencidas": int(vencidas.sum()),
        "altas": int(altas.sum()),
    }


def _executive_score(*, cobertura: float, habilitacion: float, programa: float, capacitaciones: float, faenas_criticas: int, alertas_altas: int, planes_vencidos: int) -> float:
    base = (cobertura * 0.35) + (habilitacion * 0.25) + (programa * 0.20) + (capacitaciones * 0.20)
    penalty = min(45.0, (faenas_criticas * 6.0) + (alertas_altas * 1.8) + (planes_vencidos * 2.4))
    return round(max(0.0, min(100.0, base - penalty)), 1)


def _score_label(score: float) -> tuple[str, str]:
    if score >= 85:
        return "LIDERANDO", "🟢"
    if score >= 70:
        return "CONTROLADO", "🟡"
    if score >= 55:
        return "ATENCIÓN", "🟠"
    return "CRÍTICO", "🔴"


def _portfolio_rows(*, fetch_df: Callable, fetch_value: Callable, clientes_df: pd.DataFrame, required_doc_types: list[str], worker_required_docs: Callable[[str | None], list[str]]):
    rows: list[dict] = []
    if clientes_df is None or clientes_df.empty:
        return pd.DataFrame()
    active_df = clientes_df.copy()
    if "activo" in active_df.columns:
        active_df = active_df[active_df["activo"].fillna(1).astype(int) == 1]
    for _, crow in active_df.iterrows():
        client_key = str(crow.get("cliente_key") or "").strip()
        if not client_key:
            continue
        faena_risk = build_auto_alerts(
            fetch_df=fetch_df,
            fetch_value=fetch_value,
            client_key=client_key,
            client_name=str(crow.get("cliente_nombre") or client_key),
            required_doc_types=required_doc_types,
            worker_required_docs=worker_required_docs,
            doc_tipo_label=lambda x: str(x),
        )[1]
        if faena_risk is None or faena_risk.empty:
            trabajadores = int(_safe_value(fetch_value, "SELECT COUNT(*) FROM trabajadores WHERE COALESCE(cliente_key,'')=?", (client_key,), 0) or 0)
            crit = pend = 0
            cobertura = habilitacion = 0.0
        else:
            trabajadores = int(faena_risk["trabajadores_activos"].sum())
            crit = int((faena_risk["semaforo"] == "CRITICO").sum())
            pend = int((faena_risk["semaforo"] == "PENDIENTE").sum())
            cobertura = round(float(faena_risk["cobertura_docs_pct"].mean()), 1) if len(faena_risk) else 0.0
            habilitacion = round((float(faena_risk["trabajadores_ok"].sum()) / max(float(faena_risk["trabajadores_activos"].sum()), 1.0)) * 100.0, 1) if float(faena_risk["trabajadores_activos"].sum()) > 0 else 0.0
        programa = _program_stats(fetch_df, client_key)
        cap = _training_stats(fetch_df, client_key)
        actions = _actions_stats(fetch_df, client_key)
        score = _executive_score(
            cobertura=cobertura,
            habilitacion=habilitacion,
            programa=float(programa["cerradas_pct"]),
            capacitaciones=float(cap["vigentes_pct"]),
            faenas_criticas=crit,
            alertas_altas=int(actions["altas"]),
            planes_vencidos=int(actions["vencidas"]),
        )
        score_label, icon = _score_label(score)
        rows.append(
            {
                "Cliente": str(crow.get("cliente_nombre") or client_key),
                "Vertical": str(crow.get("vertical") or "General"),
                "Score": score,
                "Estado": f"{icon} {score_label}",
                "Faenas críticas": crit,
                "Faenas pendientes": pend,
                "Trabajadores": trabajadores,
                "Cobertura docs %": cobertura,
                "Habilitación %": habilitacion,
                "Planes abiertos": int(actions["abiertas"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["Score", "Faenas críticas", "Faenas pendientes"], ascending=[False, True, True]).reset_index(drop=True) if rows else pd.DataFrame()


def page_dashboard(
    *,
    st,
    ui_header: Callable,
    ui_tip: Callable,
    get_global_counts: Callable[[], dict],
    fetch_df: Callable,
    fetch_value: Callable,
    DB_BACKEND: str,
    conn: Callable,
    execute: Callable,
    current_segav_client_key: Callable[[], str],
    segav_clientes_df: Callable[[], pd.DataFrame],
    current_user: Callable[[], dict | None],
    get_empresa_monthly_doc_types: Callable[[], list[str]],
    worker_required_docs: Callable[[str | None], list[str]],
    doc_tipo_label: Callable[[str], str],
    go: Callable,
    clear_app_caches: Callable,
):
    clientes_df = segav_clientes_df()
    current_client = _safe_current_client(clientes_df, current_segav_client_key())
    current_key = str(current_client.get("cliente_key") or current_segav_client_key() or "cli_default")
    current_name = str(current_client.get("cliente_nombre") or "Empresa activa")

    try:
        ensure_multiempresa_compliance_schema(
            db_backend=DB_BACKEND,
            execute=execute,
            conn=conn,
            current_client_key=current_key,
        )
        clear_app_caches()
    except Exception:
        pass

    ui_header("Dashboard ejecutivo comercial", f"Visión gerencial y vendible para {current_name}: operación, cumplimiento y cartera multiempresa.")

    counts = get_global_counts() or {}
    auto_alerts, faena_risk = build_auto_alerts(
        fetch_df=fetch_df,
        fetch_value=fetch_value,
        client_key=current_key,
        client_name=current_name,
        required_doc_types=get_empresa_monthly_doc_types(),
        worker_required_docs=worker_required_docs,
        doc_tipo_label=doc_tipo_label,
    )
    program = _program_stats(fetch_df, current_key)
    cap = _training_stats(fetch_df, current_key)
    actions = _actions_stats(fetch_df, current_key)

    trabajadores_activos = int(faena_risk["trabajadores_activos"].sum()) if faena_risk is not None and not faena_risk.empty else 0
    trabajadores_ok = int(faena_risk["trabajadores_ok"].sum()) if faena_risk is not None and not faena_risk.empty else 0
    cobertura = round(float(faena_risk["cobertura_docs_pct"].mean()), 1) if faena_risk is not None and not faena_risk.empty else 0.0
    habilitacion = round((trabajadores_ok / trabajadores_activos) * 100.0, 1) if trabajadores_activos else 0.0
    crit_faenas = int((faena_risk["semaforo"] == "CRITICO").sum()) if faena_risk is not None and not faena_risk.empty else 0
    pend_faenas = int((faena_risk["semaforo"] == "PENDIENTE").sum()) if faena_risk is not None and not faena_risk.empty else 0
    auto_high = int((auto_alerts["severidad"].isin(["CRITICA", "ALTA"])).sum()) if auto_alerts is not None and not auto_alerts.empty else 0

    score = _executive_score(
        cobertura=cobertura,
        habilitacion=habilitacion,
        programa=float(program["cerradas_pct"]),
        capacitaciones=float(cap["vigentes_pct"]),
        faenas_criticas=crit_faenas,
        alertas_altas=auto_high,
        planes_vencidos=int(actions["vencidas"]),
    )
    score_label, score_icon = _score_label(score)

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1:
        st.metric("Score ejecutivo", f"{score:.1f}/100", help="Índice compuesto de cobertura documental, habilitación, programa anual, capacitaciones y criticidad.")
    with m2:
        st.metric("Faenas críticas", crit_faenas)
    with m3:
        st.metric("Habilitación", f"{habilitacion:.1f}%")
    with m4:
        st.metric("Cobertura docs", f"{cobertura:.1f}%")
    with m5:
        st.metric("Programa anual", f"{float(program['cerradas_pct']):.1f}%")
    with m6:
        st.metric("Alertas / planes", int(auto_high + actions["abiertas"]))

    st.caption(f"Estado gerencial: **{score_icon} {score_label}** · Cliente activo: **{current_name}** · Vertical: **{current_client.get('vertical') or 'General'}** · Modo: **{current_client.get('modo_implementacion') or 'CONFIGURABLE'}**")

    tabs = st.tabs(["🏢 Resumen ejecutivo", "🚦 Operación y cumplimiento", "💼 Comercial / multiempresa", "⚡ Acciones"])

    with tabs[0]:
        left, right = st.columns([1.2, 1])
        with left:
            st.markdown("### Panel gerencial")
            resumen_rows = pd.DataFrame([
                {"Indicador": "Empresa activa", "Valor": current_name},
                {"Indicador": "Faenas activas", "Valor": counts.get("faenas_activas", 0)},
                {"Indicador": "Trabajadores en faena", "Valor": trabajadores_activos},
                {"Indicador": "Trabajadores habilitados", "Valor": trabajadores_ok},
                {"Indicador": "Faenas pendientes", "Valor": pend_faenas},
                {"Indicador": "Planes abiertos", "Valor": int(actions["abiertas"])},
                {"Indicador": "Planes vencidos", "Valor": int(actions["vencidas"])},
                {"Indicador": "Capacitaciones vigentes", "Valor": f"{float(cap['vigentes_pct']):.1f}%"},
            ])
            st.dataframe(resumen_rows, use_container_width=True, hide_index=True)
            if faena_risk is not None and not faena_risk.empty:
                sem = faena_risk.groupby("semaforo")["faena_id"].count().rename("Faenas")
                st.markdown("#### Distribución de semáforo")
                st.bar_chart(sem)
        with right:
            st.markdown("### Prioridades ejecutivas")
            if auto_alerts is None or auto_alerts.empty:
                st.success("No hay alertas automáticas críticas para la empresa activa.")
            else:
                top = auto_alerts.head(8).copy()
                st.dataframe(top, use_container_width=True, hide_index=True)
            if not program["vencidas"].empty:
                st.markdown("#### Programa anual vencido")
                st.dataframe(program["vencidas"], use_container_width=True, hide_index=True)

    with tabs[1]:
        op1, op2 = st.columns([1.1, 1])
        with op1:
            st.markdown("### Riesgo operativo por faena")
            if faena_risk is None or faena_risk.empty:
                st.info("No hay faenas para construir el radar ejecutivo de esta empresa.")
            else:
                ranking = faena_risk.copy()
                ranking["criticidad"] = ranking["faltantes_trabajador"].fillna(0).astype(int) + (ranking["faltantes_empresa_mes"].fillna(0).astype(int) * 3)
                ranking = ranking.sort_values(["criticidad", "cobertura_docs_pct"], ascending=[False, True])
                st.dataframe(
                    ranking[["mandante", "faena", "estado", "trabajadores_activos", "trabajadores_ok", "faltantes_trabajador", "faltantes_empresa_mes", "cobertura_docs_pct", "semaforo"]].head(12),
                    use_container_width=True,
                    hide_index=True,
                )
                chart_df = ranking[["faena", "cobertura_docs_pct"]].head(8).set_index("faena")
                st.markdown("#### Cobertura documental por faena")
                st.bar_chart(chart_df)
        with op2:
            st.markdown("### Agenda de vencimientos")
            if cap["agenda"].empty and program["vencidas"].empty:
                st.info("No hay vencimientos relevantes en programa anual ni capacitaciones.")
            else:
                if not cap["agenda"].empty:
                    st.markdown("#### Capacitaciones / ODI")
                    st.dataframe(cap["agenda"], use_container_width=True, hide_index=True)
                if auto_alerts is not None and not auto_alerts.empty:
                    docs_month = auto_alerts[auto_alerts["categoria"].astype(str) == "Documentos empresa mensuales"][ ["titulo", "detalle", "fecha_limite", "responsable"] ].head(8)
                    if not docs_month.empty:
                        st.markdown("#### Documentos empresa mensuales")
                        st.dataframe(docs_month, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.markdown("### Vista comercial del producto")
        params = {
            "Cliente": current_name,
            "Vertical": str(current_client.get("vertical") or "General"),
            "Modo de implementación": str(current_client.get("modo_implementacion") or "CONFIGURABLE"),
            "Faenas registradas": int(counts.get("faenas", 0)),
            "Trabajadores registrados": int(counts.get("trabajadores", 0)),
            "Documentos cargados": int(counts.get("docs", 0)) + int(counts.get("docs_empresa", 0)) + int(counts.get("docs_empresa_faena", 0)),
            "Exportaciones emitidas": int(counts.get("exports", 0)) + int(counts.get("exports_mes", 0)),
            "Propuesta de valor": "Cumplimiento, operación documental, multiempresa y gerencia preventiva en una sola plataforma.",
        }
        st.dataframe(pd.DataFrame([{"Bloque": k, "Valor": v} for k, v in params.items()]), use_container_width=True, hide_index=True)

        user = current_user() or {}
        role = str(user.get("role") or "").upper()
        can_portfolio = role in {"SUPERADMIN", "ADMIN"}
        if can_portfolio and clientes_df is not None and len(clientes_df) > 1:
            port = _portfolio_rows(
                fetch_df=fetch_df,
                fetch_value=fetch_value,
                clientes_df=clientes_df,
                required_doc_types=get_empresa_monthly_doc_types(),
                worker_required_docs=worker_required_docs,
            )
            if port.empty:
                st.info("No hay cartera suficiente para mostrar comparativo multiempresa.")
            else:
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.metric("Clientes activos", int(len(port)))
                with c2:
                    st.metric("Promedio score", f"{float(port['Score'].mean()):.1f}")
                with c3:
                    st.metric("Clientes críticos", int((port["Score"] < 55).sum()))
                with c4:
                    st.metric("Clientes controlados", int((port["Score"] >= 70).sum()))
                st.markdown("#### Portafolio multiempresa")
                st.dataframe(port, use_container_width=True, hide_index=True)
                st.markdown("#### Ranking comercial de salud por cliente")
                st.bar_chart(port.set_index("Cliente")["Score"])
        else:
            st.info("La vista comparativa multiempresa aparece cuando el usuario tiene perfil administrativo y existen varias empresas activas.")

    with tabs[3]:
        st.markdown("### Acciones sugeridas")
        a1, a2, a3 = st.columns(3)
        with a1:
            if st.button("Ir a Cumplimiento / Alertas", use_container_width=True, key="dash_go_compliance"):
                go("Cumplimiento / Alertas")
        with a2:
            if st.button("Ir a Mi Empresa / SGSST", use_container_width=True, key="dash_go_sgsst"):
                go("Mi Empresa / SGSST")
        with a3:
            if st.button("Ir a Faenas", use_container_width=True, key="dash_go_faenas"):
                go("Faenas")
        b1, b2, b3 = st.columns(3)
        with b1:
            if st.button("Ir a Docs Empresa (Faena)", use_container_width=True, key="dash_go_docs_faena"):
                go("Documentos Empresa (Faena)")
        with b2:
            if st.button("Ir a Docs Trabajador", use_container_width=True, key="dash_go_docs_trab"):
                go("Documentos Trabajador")
        with b3:
            if st.button("Ir a Export (ZIP)", use_container_width=True, key="dash_go_export"):
                go("Export (ZIP)")
        if crit_faenas == 0 and auto_high == 0 and actions["abiertas"] == 0:
            st.success("La empresa activa no muestra urgencias críticas. Buen punto para pasar a branding, onboarding y expansión comercial.")
        else:
            ui_tip("Usa este tablero como sala de control gerencial: primero normaliza faenas críticas, luego cierra planes vencidos y por último empuja la cobertura documental a 100%.")
