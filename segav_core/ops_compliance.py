from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Callable

import pandas as pd
import streamlit as st


SGSST_SCOPE_TABLES = [
    "sgsst_empresa",
    "sgsst_matriz_legal",
    "sgsst_programa_anual",
    "sgsst_miper",
    "sgsst_inspecciones",
    "sgsst_incidentes",
    "sgsst_capacitaciones",
    "sgsst_auditoria",
]

CORE_SCOPE_TABLES = [
    "mandantes",
    "contratos_faena",
    "faenas",
    "faena_anexos",
    "trabajadores",
    "asignaciones",
    "trabajador_documentos",
    "empresa_documentos",
    "faena_empresa_documentos",
    "export_historial",
    "export_historial_mes",
]


def _sqlite_has_column(conn_factory: Callable, table: str, column: str) -> bool:
    with conn_factory() as c:
        try:
            info = c.execute(f"PRAGMA table_info({table});").fetchall()
        except Exception:
            return False
    return any(str(row[1]) == str(column) for row in info)



def _ensure_cliente_key_sqlite(conn_factory: Callable, table: str):
    if _sqlite_has_column(conn_factory, table, "cliente_key"):
        return
    with conn_factory() as c:
        c.execute(f"ALTER TABLE {table} ADD COLUMN cliente_key TEXT;")
        c.commit()



def _ensure_cliente_key_postgres(execute: Callable, table: str):
    execute(f"ALTER TABLE IF EXISTS {table} ADD COLUMN IF NOT EXISTS cliente_key TEXT;")
    try:
        execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_cliente_key ON {table}(cliente_key);")
    except Exception:
        pass



def _ensure_alerts_table(db_backend: str, execute: Callable):
    if db_backend == "postgres":
        execute(
            """
            CREATE TABLE IF NOT EXISTS sgsst_alertas (
                id BIGSERIAL PRIMARY KEY,
                cliente_key TEXT NOT NULL,
                categoria TEXT NOT NULL,
                severidad TEXT NOT NULL DEFAULT 'MEDIA',
                titulo TEXT NOT NULL,
                detalle TEXT,
                responsable TEXT,
                fecha_limite TEXT,
                estado TEXT NOT NULL DEFAULT 'ABIERTA',
                origen TEXT DEFAULT 'MANUAL',
                origen_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        try:
            execute("CREATE INDEX IF NOT EXISTS idx_sgsst_alertas_cliente_key ON sgsst_alertas(cliente_key);")
            execute("CREATE INDEX IF NOT EXISTS idx_sgsst_alertas_estado ON sgsst_alertas(estado);")
        except Exception:
            pass
        return

    execute(
        """
        CREATE TABLE IF NOT EXISTS sgsst_alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_key TEXT NOT NULL,
            categoria TEXT NOT NULL,
            severidad TEXT NOT NULL DEFAULT 'MEDIA',
            titulo TEXT NOT NULL,
            detalle TEXT,
            responsable TEXT,
            fecha_limite TEXT,
            estado TEXT NOT NULL DEFAULT 'ABIERTA',
            origen TEXT DEFAULT 'MANUAL',
            origen_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    try:
        execute("CREATE INDEX IF NOT EXISTS idx_sgsst_alertas_cliente_key ON sgsst_alertas(cliente_key);")
        execute("CREATE INDEX IF NOT EXISTS idx_sgsst_alertas_estado ON sgsst_alertas(estado);")
    except Exception:
        pass



def ensure_multiempresa_compliance_schema(*, db_backend: str, execute: Callable, conn: Callable, current_client_key: str):
    client_key = str(current_client_key or "").strip() or "cli_default"

    for table in CORE_SCOPE_TABLES + SGSST_SCOPE_TABLES:
        try:
            if db_backend == "postgres":
                _ensure_cliente_key_postgres(execute, table)
            else:
                _ensure_cliente_key_sqlite(conn, table)
        except Exception:
            continue

    _ensure_alerts_table(db_backend, execute)

    # Backfill seguro para bases antiguas.
    for table in CORE_SCOPE_TABLES + SGSST_SCOPE_TABLES:
        try:
            execute(
                f"UPDATE {table} SET cliente_key=? WHERE cliente_key IS NULL OR TRIM(COALESCE(cliente_key,''))=''",
                (client_key,),
            )
        except Exception:
            continue

    # Intenta dejar la ficha empresa principal asociada al cliente actual.
    try:
        execute(
            "UPDATE sgsst_empresa SET cliente_key=? WHERE cliente_key IS NULL OR TRIM(COALESCE(cliente_key,''))=''",
            (client_key,),
        )
    except Exception:
        pass



def _safe_current_client(clientes_df: pd.DataFrame, current_client_key: str) -> dict:
    if clientes_df is None or clientes_df.empty:
        return {"cliente_key": current_client_key or "cli_default", "cliente_nombre": "Empresa activa"}
    keys = clientes_df["cliente_key"].astype(str).tolist()
    key = str(current_client_key or "").strip()
    if not key or key not in keys:
        row = clientes_df.iloc[0]
    else:
        row = clientes_df[clientes_df["cliente_key"].astype(str) == key].iloc[0]
    return row.to_dict()



def _company_monthly_missing_by_faena(*, fetch_df: Callable, client_key: str, required_doc_types: list[str], year: int, month: int) -> dict[int, list[str]]:
    faenas = fetch_df(
        "SELECT id FROM faenas WHERE COALESCE(cliente_key,'')=? AND COALESCE(estado,'ACTIVA')='ACTIVA' ORDER BY id",
        (client_key,),
    )
    if faenas is None or faenas.empty:
        return {}
    faena_ids = [int(v) for v in faenas["id"].tolist()]
    placeholders = ",".join(["?"] * len(faena_ids))
    docs = fetch_df(
        f"SELECT faena_id, doc_tipo FROM faena_empresa_documentos WHERE faena_id IN ({placeholders}) AND COALESCE(cliente_key,'')=? AND COALESCE(periodo_anio,0)=? AND COALESCE(periodo_mes,0)=?",
        tuple(faena_ids) + (client_key, int(year), int(month)),
    )
    present: dict[int, set[str]] = {fid: set() for fid in faena_ids}
    if docs is not None and not docs.empty:
        for _, row in docs.iterrows():
            present.setdefault(int(row["faena_id"]), set()).add(str(row["doc_tipo"]))
    missing: dict[int, list[str]] = {}
    for fid in faena_ids:
        have = present.get(fid, set())
        miss = [d for d in required_doc_types if d not in have]
        if miss:
            missing[fid] = miss
    return missing



def build_faena_risk_table(*, fetch_df: Callable, client_key: str, required_doc_types: list[str], worker_required_docs: Callable[[str | None], list[str]]):
    faenas = fetch_df(
        """
        SELECT f.id AS faena_id, f.nombre AS faena, COALESCE(f.estado,'ACTIVA') AS estado,
               COALESCE(m.nombre,'') AS mandante
          FROM faenas f
          LEFT JOIN mandantes m ON m.id=f.mandante_id
         WHERE COALESCE(f.cliente_key,'')=?
         ORDER BY COALESCE(f.estado,'ACTIVA') DESC, f.nombre
        """,
        (client_key,),
    )
    if faenas is None or faenas.empty:
        return pd.DataFrame(columns=[
            "faena_id", "mandante", "faena", "estado", "trabajadores_activos",
            "trabajadores_ok", "faltantes_trabajador", "faltantes_empresa_mes",
            "cobertura_docs_pct", "semaforo"
        ])

    faena_ids = [int(v) for v in faenas["faena_id"].tolist()]
    placeholders = ",".join(["?"] * len(faena_ids))
    asign = fetch_df(
        f"""
        SELECT a.faena_id, a.trabajador_id, COALESCE(t.cargo,'') AS cargo
          FROM asignaciones a
          JOIN trabajadores t ON t.id=a.trabajador_id
         WHERE a.faena_id IN ({placeholders})
           AND COALESCE(a.cliente_key,'')=?
           AND COALESCE(NULLIF(TRIM(UPPER(a.estado)),''),'ACTIVA')='ACTIVA'
        """,
        tuple(faena_ids) + (client_key,),
    )

    worker_ids: list[int] = []
    if asign is not None and not asign.empty:
        worker_ids = sorted({int(v) for v in asign["trabajador_id"].tolist()})

    docs_map: dict[int, set[str]] = {}
    if worker_ids:
        worker_ph = ",".join(["?"] * len(worker_ids))
        docs = fetch_df(
            f"SELECT trabajador_id, doc_tipo FROM trabajador_documentos WHERE trabajador_id IN ({worker_ph}) AND COALESCE(cliente_key,'')=?",
            tuple(worker_ids) + (client_key,),
        )
        if docs is not None and not docs.empty:
            for tid, grp in docs.groupby("trabajador_id"):
                docs_map[int(tid)] = set(grp["doc_tipo"].astype(str).tolist())

    now = date.today()
    monthly_missing = _company_monthly_missing_by_faena(
        fetch_df=fetch_df,
        client_key=client_key,
        required_doc_types=required_doc_types,
        year=now.year,
        month=now.month,
    )

    worker_rows = []
    if asign is not None and not asign.empty:
        for _, row in asign.iterrows():
            tid = int(row["trabajador_id"])
            req_docs = list(dict.fromkeys(worker_required_docs(row.get("cargo"))))
            have = docs_map.get(tid, set())
            present = sum(1 for doc in req_docs if doc in have)
            worker_rows.append(
                {
                    "faena_id": int(row["faena_id"]),
                    "trabajador_id": tid,
                    "req_total": len(req_docs),
                    "req_present": present,
                    "faltantes": max(len(req_docs) - present, 0),
                    "ok": int(len(req_docs) > 0 and present >= len(req_docs)),
                }
            )

    agg = pd.DataFrame(worker_rows)
    if agg.empty:
        faenas = faenas.copy()
        faenas["trabajadores_activos"] = 0
        faenas["trabajadores_ok"] = 0
        faenas["faltantes_trabajador"] = 0
        faenas["faltantes_empresa_mes"] = faenas["faena_id"].map(lambda x: len(monthly_missing.get(int(x), []))).fillna(0).astype(int)
        faenas["cobertura_docs_pct"] = 0.0
    else:
        agg = (
            agg.groupby("faena_id")
            .agg(
                trabajadores_activos=("trabajador_id", "nunique"),
                trabajadores_ok=("ok", "sum"),
                faltantes_trabajador=("faltantes", "sum"),
                req_present=("req_present", "sum"),
                req_total=("req_total", "sum"),
            )
            .reset_index()
        )
        agg["cobertura_docs_pct"] = (agg["req_present"] / agg["req_total"]).where(agg["req_total"] > 0, 0.0) * 100.0
        faenas = faenas.merge(agg, on="faena_id", how="left")
        for col in ["trabajadores_activos", "trabajadores_ok", "faltantes_trabajador"]:
            faenas[col] = faenas[col].fillna(0).astype(int)
        faenas["cobertura_docs_pct"] = faenas["cobertura_docs_pct"].fillna(0.0).astype(float)
        faenas["faltantes_empresa_mes"] = faenas["faena_id"].map(lambda x: len(monthly_missing.get(int(x), []))).fillna(0).astype(int)

    def _semaforo(r):
        if int(r.get("faltantes_empresa_mes", 0) or 0) > 0:
            return "CRITICO"
        if int(r.get("faltantes_trabajador", 0) or 0) >= 3:
            return "CRITICO"
        if float(r.get("cobertura_docs_pct", 0.0) or 0.0) < 70.0 and int(r.get("trabajadores_activos", 0) or 0) > 0:
            return "CRITICO"
        if int(r.get("faltantes_trabajador", 0) or 0) > 0:
            return "PENDIENTE"
        if float(r.get("cobertura_docs_pct", 0.0) or 0.0) < 100.0 and int(r.get("trabajadores_activos", 0) or 0) > 0:
            return "PENDIENTE"
        return "OK"

    faenas["semaforo"] = faenas.apply(_semaforo, axis=1)
    return faenas



def build_auto_alerts(*, fetch_df: Callable, fetch_value: Callable, client_key: str, client_name: str, required_doc_types: list[str], worker_required_docs: Callable[[str | None], list[str]], doc_tipo_label: Callable[[str], str]):
    today = date.today().isoformat()
    soon = (date.today() + timedelta(days=30)).isoformat()

    faena_risk = build_faena_risk_table(
        fetch_df=fetch_df,
        client_key=client_key,
        required_doc_types=required_doc_types,
        worker_required_docs=worker_required_docs,
    )

    rows: list[dict] = []

    if faena_risk is not None and not faena_risk.empty:
        crit = faena_risk[faena_risk["semaforo"] == "CRITICO"]
        pend = faena_risk[faena_risk["semaforo"] == "PENDIENTE"]
        for _, row in crit.iterrows():
            rows.append(
                {
                    "severidad": "CRITICA",
                    "categoria": "Documentación operacional",
                    "titulo": f"Faena crítica: {row['faena']}",
                    "detalle": f"Faltantes trabajador: {int(row['faltantes_trabajador'])} · Faltantes empresa mes: {int(row['faltantes_empresa_mes'])}",
                    "responsable": "Supervisor / Prevención",
                    "fecha_limite": today,
                    "origen": "FAENA",
                }
            )
        for _, row in pend.head(10).iterrows():
            rows.append(
                {
                    "severidad": "MEDIA",
                    "categoria": "Seguimiento documental",
                    "titulo": f"Faena con pendientes: {row['faena']}",
                    "detalle": f"Cobertura docs {float(row['cobertura_docs_pct']):.1f}% · Faltantes trabajador: {int(row['faltantes_trabajador'])}",
                    "responsable": "Administrador ERP",
                    "fecha_limite": soon,
                    "origen": "FAENA",
                }
            )

    overdue_prog = fetch_df(
        """
        SELECT id, actividad, fecha_compromiso, responsable
          FROM sgsst_programa_anual
         WHERE COALESCE(cliente_key,'')=?
           AND COALESCE(estado,'PENDIENTE') <> 'CERRADO'
           AND fecha_compromiso IS NOT NULL AND TRIM(fecha_compromiso)<>''
           AND fecha_compromiso < ?
         ORDER BY fecha_compromiso ASC
        """,
        (client_key, today),
    )
    if overdue_prog is not None and not overdue_prog.empty:
        for _, row in overdue_prog.head(10).iterrows():
            rows.append(
                {
                    "severidad": "ALTA",
                    "categoria": "Programa anual",
                    "titulo": f"Actividad vencida: {row['actividad']}",
                    "detalle": f"Compromiso: {row['fecha_compromiso'] or '-'}",
                    "responsable": row.get("responsable") or "Sin responsable",
                    "fecha_limite": row.get("fecha_compromiso") or today,
                    "origen": f"PROGRAMA:{int(row['id'])}",
                }
            )

    cap_venc = fetch_df(
        """
        SELECT id, tipo, tema, vigencia, relator
          FROM sgsst_capacitaciones
         WHERE COALESCE(cliente_key,'')=?
           AND vigencia IS NOT NULL AND TRIM(vigencia)<>''
           AND vigencia <= ?
         ORDER BY vigencia ASC
        """,
        (client_key, soon),
    )
    if cap_venc is not None and not cap_venc.empty:
        for _, row in cap_venc.head(12).iterrows():
            sev = "ALTA" if str(row.get("vigencia") or "") < today else "MEDIA"
            rows.append(
                {
                    "severidad": sev,
                    "categoria": "Capacitaciones / ODI",
                    "titulo": f"{row.get('tipo') or 'Capacitación'} con vigencia próxima: {row.get('tema') or ''}",
                    "detalle": f"Vigencia: {row.get('vigencia') or '-'}",
                    "responsable": row.get("relator") or "Prevención",
                    "fecha_limite": row.get("vigencia") or soon,
                    "origen": f"CAP:{int(row['id'])}",
                }
            )

    matriz_pend = int(
        fetch_value(
            "SELECT COUNT(*) FROM sgsst_matriz_legal WHERE COALESCE(cliente_key,'')=? AND COALESCE(estado,'PENDIENTE') <> 'CERRADO'",
            (client_key,),
            default=0,
        )
        or 0
    )
    if matriz_pend > 0:
        rows.append(
            {
                "severidad": "MEDIA",
                "categoria": "Matriz legal",
                "titulo": f"{matriz_pend} obligaciones abiertas en matriz legal",
                "detalle": f"Empresa activa: {client_name}",
                "responsable": "Gerencia / Prevención",
                "fecha_limite": soon,
                "origen": "MATRIZ",
            }
        )

    # resumen documentos empresa mensual
    monthly_missing = _company_monthly_missing_by_faena(
        fetch_df=fetch_df,
        client_key=client_key,
        required_doc_types=required_doc_types,
        year=date.today().year,
        month=date.today().month,
    )
    for faena_id, docs in list(monthly_missing.items())[:10]:
        faena_name = fetch_value("SELECT nombre FROM faenas WHERE id=?", (int(faena_id),), default=f"Faena {faena_id}")
        docs_txt = ", ".join(doc_tipo_label(d) for d in docs[:3])
        if len(docs) > 3:
            docs_txt += f" +{len(docs) - 3}"
        rows.append(
            {
                "severidad": "ALTA",
                "categoria": "Documentos empresa mensuales",
                "titulo": f"Faltan documentos del mes en {faena_name}",
                "detalle": docs_txt,
                "responsable": "Administración / Prevención",
                "fecha_limite": today,
                "origen": f"DOC_FAENA:{faena_id}",
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out, faena_risk

    severity_order = {"CRITICA": 0, "ALTA": 1, "MEDIA": 2, "BAJA": 3}
    out["_ord"] = out["severidad"].map(severity_order).fillna(9)
    out = out.sort_values(["_ord", "fecha_limite", "titulo"], ascending=[True, True, True]).drop(columns=["_ord"])
    return out.reset_index(drop=True), faena_risk



def _metric_card(label: str, value, help_text: str | None = None):
    st.metric(label, value, help=help_text)



def page_compliance_alerts(
    *,
    DB_BACKEND: str,
    conn: Callable,
    execute: Callable,
    fetch_df: Callable,
    fetch_value: Callable,
    clear_app_caches: Callable,
    current_segav_client_key: Callable[[], str],
    segav_clientes_df: Callable[[], pd.DataFrame],
    get_empresa_monthly_doc_types: Callable[[], list[str]],
    worker_required_docs: Callable[[str | None], list[str]],
    doc_tipo_label: Callable[[str], str],
    sgsst_log: Callable[[str, str, str], None],
):
    current_key = str(current_segav_client_key() or "").strip()
    clientes_df = segav_clientes_df()
    current_client = _safe_current_client(clientes_df, current_key)
    current_key = str(current_client.get("cliente_key") or current_key or "cli_default")

    ensure_multiempresa_compliance_schema(
        db_backend=DB_BACKEND,
        execute=execute,
        conn=conn,
        current_client_key=current_key,
    )

    client_name = str(current_client.get("cliente_nombre") or "Empresa activa")
    st.markdown(
        f"""
        <div class="gyd-card">
            <div style="font-size:1.35rem;font-weight:700;line-height:1.2;">Cumplimiento / Alertas</div>
            <div class="gyd-muted" style="margin-top:6px;">Motor ejecutivo para {client_name}: semáforo, vencimientos, documentos críticos y planes de acción.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    auto_alerts, faena_risk = build_auto_alerts(
        fetch_df=fetch_df,
        fetch_value=fetch_value,
        client_key=current_key,
        client_name=client_name,
        required_doc_types=get_empresa_monthly_doc_types(),
        worker_required_docs=worker_required_docs,
        doc_tipo_label=doc_tipo_label,
    )

    open_actions = fetch_df(
        "SELECT id, categoria, severidad, titulo, detalle, responsable, fecha_limite, estado, origen, created_at, updated_at FROM sgsst_alertas WHERE COALESCE(cliente_key,'')=? ORDER BY CASE WHEN estado='ABIERTA' THEN 0 ELSE 1 END, fecha_limite ASC, id DESC",
        (current_key,),
    )
    open_actions = open_actions if open_actions is not None else pd.DataFrame()

    crit_auto = int((auto_alerts["severidad"].isin(["CRITICA", "ALTA"])).sum()) if auto_alerts is not None and not auto_alerts.empty else 0
    pend_faenas = int((faena_risk["semaforo"] == "PENDIENTE").sum()) if faena_risk is not None and not faena_risk.empty else 0
    crit_faenas = int((faena_risk["semaforo"] == "CRITICO").sum()) if faena_risk is not None and not faena_risk.empty else 0
    open_manual = int((open_actions["estado"].fillna("ABIERTA") == "ABIERTA").sum()) if not open_actions.empty else 0
    overdue_manual = int(((open_actions["estado"].fillna("ABIERTA") == "ABIERTA") & (open_actions["fecha_limite"].fillna("") < date.today().isoformat())).sum()) if not open_actions.empty and "fecha_limite" in open_actions.columns else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        _metric_card("Faenas críticas", crit_faenas)
    with m2:
        _metric_card("Faenas pendientes", pend_faenas)
    with m3:
        _metric_card("Alertas auto altas", crit_auto)
    with m4:
        _metric_card("Planes abiertos", open_manual)
    with m5:
        _metric_card("Planes vencidos", overdue_manual)

    tabs = st.tabs(["🚦 Semáforo ejecutivo", "🔔 Alertas automáticas", "🛠️ Planes de acción", "📋 Riesgo por faena"])

    with tabs[0]:
        left, right = st.columns([1.15, 1])
        with left:
            st.markdown("### Prioridades de hoy")
            if auto_alerts is None or auto_alerts.empty:
                st.success("No hay alertas automáticas críticas para la empresa activa.")
            else:
                top = auto_alerts.head(8).copy()
                st.dataframe(top, use_container_width=True, hide_index=True)
        with right:
            st.markdown("### Estado de semáforo")
            if faena_risk is None or faena_risk.empty:
                st.info("No hay faenas registradas para la empresa activa.")
            else:
                resumen = faena_risk.groupby("semaforo")["faena_id"].count().reset_index().rename(columns={"faena_id": "Faenas"})
                st.dataframe(resumen, use_container_width=True, hide_index=True)
                crit_rows = faena_risk[faena_risk["semaforo"] == "CRITICO"]
                if not crit_rows.empty:
                    st.warning("Faenas que requieren intervención inmediata:")
                    st.dataframe(
                        crit_rows[["mandante", "faena", "faltantes_trabajador", "faltantes_empresa_mes", "cobertura_docs_pct"]],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.success("No hay faenas en estado CRITICO.")

    with tabs[1]:
        st.markdown("### Bandeja automática de cumplimiento")
        if auto_alerts is None or auto_alerts.empty:
            st.info("Sin alertas automáticas para mostrar.")
        else:
            sev_options = ["TODAS"] + list(dict.fromkeys(auto_alerts["severidad"].tolist()))
            sev = st.selectbox("Filtrar severidad", sev_options, key="comp_auto_sev")
            shown = auto_alerts if sev == "TODAS" else auto_alerts[auto_alerts["severidad"] == sev]
            st.dataframe(shown, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.markdown("### Planes de acción manuales")
        with st.form("comp_new_action"):
            c1, c2, c3 = st.columns(3)
            categoria = c1.text_input("Categoría", value="Cumplimiento")
            severidad = c2.selectbox("Severidad", ["CRITICA", "ALTA", "MEDIA", "BAJA"], index=2)
            responsable = c3.text_input("Responsable", value="Prevención")
            titulo = st.text_input("Título de la acción", placeholder="Ej: Regularizar F30-1 faena Los Boldos")
            detalle = st.text_area("Detalle", height=90, placeholder="Describe el hallazgo, la medida y el criterio de cierre.")
            fecha_limite = st.date_input("Fecha compromiso", value=date.today() + timedelta(days=7))
            submit = st.form_submit_button("Crear plan de acción", type="primary", use_container_width=True)
        if submit:
            if not str(titulo).strip():
                st.error("Debes ingresar un título.")
            else:
                now = datetime.now().isoformat(timespec="seconds")
                execute(
                    "INSERT INTO sgsst_alertas(cliente_key, categoria, severidad, titulo, detalle, responsable, fecha_limite, estado, origen, origen_id, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        current_key,
                        str(categoria).strip() or "Cumplimiento",
                        severidad,
                        str(titulo).strip(),
                        str(detalle).strip(),
                        str(responsable).strip(),
                        fecha_limite.isoformat() if fecha_limite else None,
                        "ABIERTA",
                        "MANUAL",
                        None,
                        now,
                        now,
                    ),
                )
                clear_app_caches()
                sgsst_log("Cumplimiento / Alertas", "Crear acción", str(titulo).strip())
                st.success("Plan de acción creado.")
                st.rerun()

        if open_actions.empty:
            st.info("No hay planes de acción manuales registrados.")
        else:
            st.dataframe(open_actions, use_container_width=True, hide_index=True)
            open_only = open_actions[open_actions["estado"].fillna("ABIERTA") == "ABIERTA"]
            if not open_only.empty:
                action_id = st.selectbox(
                    "Acción a gestionar",
                    open_only["id"].tolist(),
                    format_func=lambda x: f"#{x} · {open_only[open_only['id']==x].iloc[0]['titulo']}",
                    key="comp_action_pick",
                )
                a1, a2 = st.columns(2)
                with a1:
                    if st.button("Cerrar acción", use_container_width=True, key="comp_action_close"):
                        now = datetime.now().isoformat(timespec="seconds")
                        execute("UPDATE sgsst_alertas SET estado='CERRADA', updated_at=? WHERE id=?", (now, int(action_id)))
                        clear_app_caches()
                        sgsst_log("Cumplimiento / Alertas", "Cerrar acción", f"Acción {action_id}")
                        st.success("Acción cerrada.")
                        st.rerun()
                with a2:
                    if st.button("Reabrir acción", use_container_width=True, key="comp_action_reopen"):
                        now = datetime.now().isoformat(timespec="seconds")
                        execute("UPDATE sgsst_alertas SET estado='ABIERTA', updated_at=? WHERE id=?", (now, int(action_id)))
                        clear_app_caches()
                        sgsst_log("Cumplimiento / Alertas", "Reabrir acción", f"Acción {action_id}")
                        st.success("Acción reabierta.")
                        st.rerun()

    with tabs[3]:
        st.markdown("### Matriz resumida de riesgo documental por faena")
        if faena_risk is None or faena_risk.empty:
            st.info("No hay faenas para analizar en la empresa activa.")
        else:
            st.dataframe(faena_risk, use_container_width=True, hide_index=True)
