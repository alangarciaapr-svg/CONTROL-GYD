from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from segav_core.ui import ui_header


def page_sgsst(
    *,
    fetch_df,
    fetch_value,
    execute,
    clear_app_caches,
    ensure_sgsst_seed_data,
    segav_erp_config_map,
    segav_clientes_df,
    current_segav_client_key,
    segav_cargos_df,
    get_empresa_required_doc_types,
    clean_rut,
    go,
    segav_templates_df,
    ERP_TEMPLATE_PRESETS,
    apply_segav_template,
    sgsst_log,
    make_erp_key,
    segav_erp_value,
    ERP_CLIENT_PARAM_DEFAULTS,
    set_segav_erp_config_value,
    segav_cliente_params,
    segav_cargo_labels,
    segav_cargo_rules,
    DOC_OBLIGATORIOS,
    DOC_TIPO_LABELS,
    doc_tipo_label,
    segav_empresa_docs_df,
    get_empresa_monthly_doc_types,
    parse_date_maybe,
    SGSST_NORMAS,
    SGSST_ESTADOS,
    SGSST_GRAVEDADES,
    SGSST_RESULTADOS,
    SGSST_TIPOS_EVENTO,
    SGSST_TIPOS_CAP,
    doc_tipo_join,
    current_user,
    segav_template_payload,
):
    ui_header("Mi Empresa / SGSST", "Núcleo comercializable de SEGAV ERP: configurable para cualquier empresa, sin reemplazar lo ya existente.")
    ensure_sgsst_seed_data()
    _u = current_user() or {}
    _is_superadmin = str(_u.get("role") or "").upper() == "SUPERADMIN"

    company_df = fetch_df("SELECT * FROM sgsst_empresa ORDER BY id LIMIT 1")
    company = company_df.iloc[0].to_dict() if not company_df.empty else {}

    stats = {
        "faenas_activas": int(fetch_value("SELECT COUNT(*) FROM faenas WHERE estado='ACTIVA'", default=0) or 0),
        "trabajadores": int(fetch_value("SELECT COUNT(*) FROM trabajadores", default=0) or 0),
        "matriz_pendiente": int(fetch_value("SELECT COUNT(*) FROM sgsst_matriz_legal WHERE COALESCE(estado,'PENDIENTE') <> 'CERRADO'", default=0) or 0),
        "programa_abierto": int(fetch_value("SELECT COUNT(*) FROM sgsst_programa_anual WHERE COALESCE(estado,'PENDIENTE') <> 'CERRADO'", default=0) or 0),
        "miper_criticos": int(fetch_value("SELECT COUNT(*) FROM sgsst_miper WHERE COALESCE(nivel_riesgo,0) >= 15 AND COALESCE(estado,'PENDIENTE') <> 'CERRADO'", default=0) or 0),
        "ds594_abierto": int(fetch_value("SELECT COUNT(*) FROM sgsst_inspecciones WHERE COALESCE(resultado,'OBSERVACIÓN') <> 'CUMPLE'", default=0) or 0),
        "incidentes_abiertos": int(fetch_value("SELECT COUNT(*) FROM sgsst_incidentes WHERE COALESCE(estado,'PENDIENTE') <> 'CERRADO'", default=0) or 0),
        "cap_vencidas": int(fetch_value("SELECT COUNT(*) FROM sgsst_capacitaciones WHERE vigencia IS NOT NULL AND TRIM(vigencia)<>'' AND vigencia < ?", (date.today().isoformat(),), default=0) or 0),
    }

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faenas activas", stats["faenas_activas"])
    c2.metric("Trabajadores", stats["trabajadores"])
    c3.metric("Matriz legal pendiente", stats["matriz_pendiente"])
    c4.metric("Riesgos críticos MIPER", stats["miper_criticos"])
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Programa abierto", stats["programa_abierto"])
    d2.metric("Hallazgos DS 594", stats["ds594_abierto"])
    d3.metric("Incidentes abiertos", stats["incidentes_abiertos"])
    d4.metric("Capacitaciones/ODI vencidas", stats["cap_vencidas"])

    tabs = st.tabs([
        "🏢 Resumen",
        "⚙️ Configuración ERP",
        "🏭 Ficha empresa",
        "🧩 Catálogos",
        "⚖️ Matriz legal",
        "📅 Programa anual",
        "⚠️ MIPER",
        "🧯 Inspecciones DS 594",
        "🩹 Accidentes e Incidentes",
        "🎓 Capacitaciones y ODI",
        "🧾 Auditoría",
    ])

    with tabs[0]:
        cfg = segav_erp_config_map()
        clientes_df = segav_clientes_df()
        current_client_key = current_segav_client_key()
        current_client = {}
        if clientes_df is not None and not clientes_df.empty:
            rowc = clientes_df[clientes_df['cliente_key'].astype(str) == str(current_client_key)]
            if rowc.empty:
                rowc = clientes_df.iloc[[0]]
            current_client = rowc.iloc[0].to_dict()
        faenas_activas = int(fetch_value("SELECT COUNT(*) FROM faenas WHERE COALESCE(estado,'ACTIVA')='ACTIVA'", default=0) or 0)
        total_trab = int(fetch_value("SELECT COUNT(*) FROM trabajadores", default=0) or 0)
        total_clientes = int(len(clientes_df)) if clientes_df is not None else 0
        total_cargos = int(len(segav_cargos_df())) if segav_cargos_df() is not None else 0
        total_docs_empresa = len(get_empresa_required_doc_types())

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Clientes activos", total_clientes)
        k2.metric("Faenas activas", faenas_activas)
        k3.metric("Trabajadores", total_trab)
        k4.metric("Cargos parametrizados", total_cargos)
        k5.metric("Docs empresa base", total_docs_empresa)

        left, right = st.columns([1.2, 1])
        with left:
            st.markdown("### Vista ejecutiva")
            resumen = [
                ("ERP", cfg.get("erp_name", "SEGAV ERP")),
                ("Vertical", cfg.get("erp_vertical", "General")),
                ("Plantilla actual", cfg.get("template_actual", "GENERAL")),
                ("Cliente actual", current_client.get("cliente_nombre") or cfg.get("cliente_actual") or "Sin definir"),
                ("Razón social operativa", company.get("razon_social") or "Sin definir"),
                ("Organismo administrador", company.get("organismo_admin") or "Sin definir"),
                ("Prevencionista", company.get("prevencionista") or "Sin definir"),
                ("Dotación total", company.get("dotacion_total") or 0),
            ]
            for label, value in resumen:
                st.write(f"**{label}:** {value}")
            st.info(
                "SEGAV ERP ya cuenta con capa comercial configurable: catálogos, clientes, plantillas por rubro y parámetros por cliente, sin eliminar la operación actual.",
                icon="🧭",
            )
        with right:
            st.markdown("### Estado general")
            dotacion = int(company.get("dotacion_total") or 0)
            cphs_msg = "CPHS obligatorio" if dotacion > 25 else "CPHS aún no obligatorio (monitorear dotación)"
            estado_rows = pd.DataFrame([
                {"Indicador": "Ley 16.744 / Dotación", "Estado": cphs_msg},
                {"Indicador": "DS 44", "Estado": "Base ERP visible cargada"},
                {"Indicador": "DS 594", "Estado": "Checklist inicial habilitado"},
                {"Indicador": "Multiempresa", "Estado": cfg.get("multiempresa", "SI")},
                {"Indicador": "Implementación", "Estado": cfg.get("modo_implementacion", "CONFIGURABLE")},
            ])
            st.dataframe(estado_rows, use_container_width=True, hide_index=True)
            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("Ir a Docs Empresa", use_container_width=True, key="sgsst_go_docs_emp"):
                    go("Documentos Empresa")
            with b2:
                if st.button("Ir a Docs Faena", use_container_width=True, key="sgsst_go_docs_faena"):
                    go("Documentos Empresa (Faena)")
            with b3:
                if st.button("Ir a Trabajadores", use_container_width=True, key="sgsst_go_trab"):
                    go("Trabajadores")

        st.markdown("### Dashboard comercial / ERP")
        dash_rows = pd.DataFrame([
            {"Bloque": "Producto", "Valor": cfg.get("erp_slogan", "") or "Sin definir"},
            {"Bloque": "Cliente actual", "Valor": current_client.get("cliente_nombre") or cfg.get("cliente_actual") or "Sin definir"},
            {"Bloque": "RUT cliente", "Valor": current_client.get("rut") or clean_rut(company.get("rut") or "") or "Sin definir"},
            {"Bloque": "Vertical actual", "Valor": cfg.get("erp_vertical", "General")},
            {"Bloque": "Modo comercial", "Valor": cfg.get("multiempresa", "SI")},
            {"Bloque": "Plantilla activa", "Valor": cfg.get("template_actual", "GENERAL")},
        ])
        st.dataframe(dash_rows, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.markdown("### Configuración ERP")
        cfg = segav_erp_config_map()
        z1, z2 = st.columns(2)
        with z1:
            erp_name = st.text_input("Nombre comercial", value=cfg.get("erp_name", "SEGAV ERP"), key="segav_cfg_name")
            erp_slogan = st.text_area("Propuesta de valor", value=cfg.get("erp_slogan", "ERP comercializable de cumplimiento, prevención y operación documental"), height=90, key="segav_cfg_slogan")
            erp_vertical = st.text_input("Vertical / rubro base", value=cfg.get("erp_vertical", "General"), key="segav_cfg_vertical")
        with z2:
            multiempresa = st.selectbox("Modo comercial", ["SI", "NO"], index=0 if cfg.get("multiempresa", "SI") == "SI" else 1, key="segav_cfg_multi")
            cliente_actual = st.text_input("Cliente / empresa actual", value=cfg.get("cliente_actual", company.get("razon_social") or "Empresa actual"), key="segav_cfg_cliente")
            impl_opts = ["CONFIGURABLE", "VERTICAL FORESTAL", "CORPORATIVO"]
            modo_impl = st.selectbox("Implementación", impl_opts, index=impl_opts.index(cfg.get("modo_implementacion", "CONFIGURABLE")) if cfg.get("modo_implementacion", "CONFIGURABLE") in impl_opts else 0, key="segav_cfg_impl")
        if st.button("Guardar configuración ERP", key="segav_cfg_save", type="primary"):
            now = datetime.now().isoformat(timespec='seconds')
            payload = {
                "erp_name": erp_name.strip() or "SEGAV ERP",
                "erp_slogan": erp_slogan.strip(),
                "erp_vertical": erp_vertical.strip() or "General",
                "multiempresa": multiempresa,
                "cliente_actual": cliente_actual.strip(),
                "modo_implementacion": modo_impl,
            }
            for k, v in payload.items():
                execute("DELETE FROM segav_erp_config WHERE config_key=?", (k,))
                execute("INSERT INTO segav_erp_config(config_key, config_value, updated_at) VALUES(?,?,?)", (k, str(v), now))
            clear_app_caches()
            sgsst_log("Configuración ERP", "Guardar", f"Configuración comercial actualizada: {payload.get('erp_name')}")
            st.success("Configuración ERP guardada.")
            st.rerun()
        st.markdown("---")
        st.markdown("### Plantillas por rubro")
        tpl_df = segav_templates_df()
        if tpl_df is not None and not tpl_df.empty:
            st.dataframe(tpl_df[["template_key", "template_label", "vertical", "description", "activo"]].rename(columns={"template_key":"Código", "template_label":"Plantilla", "vertical":"Vertical", "description":"Descripción", "activo":"Activa"}), use_container_width=True, hide_index=True)
        tpl_options = tpl_df["template_key"].tolist() if tpl_df is not None and not tpl_df.empty else list(ERP_TEMPLATE_PRESETS.keys())
        current_tpl = cfg.get("template_actual", tpl_options[0] if tpl_options else "GENERAL")
        if current_tpl not in tpl_options and tpl_options:
            current_tpl = tpl_options[0]
        tpl_sel = st.selectbox("Plantilla a visualizar/aplicar", tpl_options, index=tpl_options.index(current_tpl) if tpl_options else 0, key="segav_tpl_sel") if tpl_options else None
        if tpl_sel:
            payload = segav_template_payload(tpl_sel)
            p1, p2 = st.columns([1.1, 1])
            with p1:
                st.write(f"**Descripción:** {payload.get('description') or 'Sin descripción'}")
                st.write(f"**Vertical:** {payload.get('vertical') or 'Sin definir'}")
                st.write(f"**Cargos incluidos:** {', '.join(payload.get('cargos', [])) or 'Sin cargos'}")
            with p2:
                preview_rows = []
                for cargo_name, docs in (payload.get('cargo_rules') or {}).items():
                    preview_rows.append({"Cargo": cargo_name, "Documentos": doc_tipo_join(docs)})
                if preview_rows:
                    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)
            if st.button("Aplicar plantilla al catálogo ERP", key="segav_apply_tpl"):
                ok, msg = apply_segav_template(tpl_sel)
                if ok:
                    sgsst_log("Configuración ERP", "Aplicar plantilla", tpl_sel)
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

        st.markdown("---")
        if _is_superadmin:
            st.info("La administración completa de empresas, alta/baja, edición y designación de administradores está centralizada en **SuperAdmin / Empresas**.", icon="🌐")
        else:
            st.info("La gestión multiempresa y de administradores está disponible solo para **SUPERADMIN** en la sección **SuperAdmin / Empresas**.", icon="🔒")

    with tabs[3]:
        st.markdown("### Catálogos configurables")
        st.write("#### Cargos del ERP")
        cargos_df = segav_cargos_df()
        if cargos_df is not None and not cargos_df.empty:
            st.dataframe(cargos_df.rename(columns={"cargo_key":"Código", "cargo_label":"Cargo", "sort_order":"Orden", "activo":"Activo"}), use_container_width=True, hide_index=True)
        cadd1, cadd2, cadd3 = st.columns([1.4, 0.8, 0.8])
        cargo_new_label = cadd1.text_input("Nuevo cargo", key="segav_new_cargo_label")
        cargo_new_order = cadd2.number_input("Orden", min_value=1, value=int((len(cargos_df) if cargos_df is not None else 0) + 1), step=1, key="segav_new_cargo_order")
        cargo_new_active = cadd3.selectbox("Activo", ["SI", "NO"], key="segav_new_cargo_active")
        if st.button("Agregar cargo al catálogo", key="segav_add_cargo"):
            if not cargo_new_label.strip():
                st.error("Debes indicar el nombre del cargo.")
            else:
                cargo_key = cargo_new_label.strip().upper()
                now = datetime.now().isoformat(timespec='seconds')
                execute("DELETE FROM segav_erp_cargos WHERE cargo_key=?", (cargo_key,))
                execute("INSERT INTO segav_erp_cargos(cargo_key, cargo_label, sort_order, activo, updated_at) VALUES(?,?,?,?,?)", (cargo_key, cargo_key, int(cargo_new_order), 1 if cargo_new_active == "SI" else 0, now))
                clear_app_caches()
                sgsst_log("Catálogos ERP", "Agregar cargo", cargo_key)
                st.success("Cargo agregado al catálogo.")
                st.rerun()

        cargo_labels = segav_cargo_labels(active_only=False)
        cargo_sel = st.selectbox("Cargo a parametrizar", cargo_labels, key="segav_docs_cargo_sel")
        current_docs = segav_cargo_rules().get(cargo_sel, DOC_OBLIGATORIOS)
        docs_selected = st.multiselect("Documentos obligatorios por cargo", list(DOC_TIPO_LABELS.keys()), default=current_docs, key="segav_docs_cargo_multi", format_func=doc_tipo_label)
        if st.button("Guardar documentos por cargo", key="segav_docs_cargo_save"):
            now = datetime.now().isoformat(timespec='seconds')
            execute("DELETE FROM segav_erp_docs_cargo WHERE cargo_key=?", (cargo_sel,))
            for idx, doc_tipo in enumerate(docs_selected, start=1):
                execute("INSERT INTO segav_erp_docs_cargo(cargo_key, doc_tipo, sort_order, updated_at) VALUES(?,?,?,?)", (cargo_sel, doc_tipo, idx, now))
            clear_app_caches()
            sgsst_log("Catálogos ERP", "Guardar docs cargo", f"{cargo_sel}: {', '.join(docs_selected)}")
            st.success("Documentación obligatoria por cargo actualizada.")
            st.rerun()

        st.write("#### Documentos empresa / mandante / faena")
        emp_df = segav_empresa_docs_df()
        if emp_df is not None and not emp_df.empty:
            st.dataframe(emp_df.rename(columns={"doc_tipo":"Tipo", "obligatorio":"Obligatorio", "mensual":"Mensual", "por_mandante":"Por mandante", "por_faena":"Por faena", "sort_order":"Orden"}), use_container_width=True, hide_index=True)
        emp_docs_selected = st.multiselect("Documentos requeridos empresa/faena", list(DOC_TIPO_LABELS.keys()), default=get_empresa_monthly_doc_types(), key="segav_docs_empresa_multi", format_func=doc_tipo_label)
        if st.button("Guardar documentos empresa/faena", key="segav_docs_empresa_save"):
            now = datetime.now().isoformat(timespec='seconds')
            execute("DELETE FROM segav_erp_docs_empresa", ())
            for idx, doc_tipo in enumerate(emp_docs_selected, start=1):
                execute("INSERT INTO segav_erp_docs_empresa(doc_tipo, obligatorio, mensual, por_mandante, por_faena, sort_order, updated_at) VALUES(?,?,?,?,?,?,?)", (doc_tipo, 1, 1, 1, 1, idx, now))
            clear_app_caches()
            sgsst_log("Catálogos ERP", "Guardar docs empresa", ', '.join(emp_docs_selected))
            st.success("Documentos empresa/faena actualizados.")
            st.rerun()

    with tabs[2]:
        st.markdown("### Ficha empresa")
        e1, e2 = st.columns(2)
        with e1:
            razon_social = st.text_input("Razón social", value=str(company.get("razon_social") or ""), key="sgsst_empresa_razon")
            rut = st.text_input("RUT empresa", value=clean_rut(company.get("rut") or ""), key="sgsst_empresa_rut")
            direccion = st.text_input("Dirección", value=str(company.get("direccion") or ""), key="sgsst_empresa_direccion")
            actividad = st.text_input("Actividad / rubro", value=str(company.get("actividad") or ""), key="sgsst_empresa_actividad")
            organismo_admin = st.text_input("Organismo administrador", value=str(company.get("organismo_admin") or ""), key="sgsst_empresa_oa")
            dotacion_total = st.number_input("Dotación total", min_value=0, value=int(company.get("dotacion_total") or 0), step=1, key="sgsst_empresa_dotacion")
        with e2:
            representantes = st.text_area("Representantes legales", value=str(company.get("representantes") or ""), height=90, key="sgsst_empresa_repr")
            prevencionista = st.text_input("Prevencionista de riesgos", value=str(company.get("prevencionista") or ""), key="sgsst_empresa_prev")
            canal = st.text_input("Canal de denuncias", value=str(company.get("canal_denuncias") or ""), key="sgsst_empresa_canal")
            politica_version = st.text_input("Versión política SST", value=str(company.get("politica_version") or "1.0"), key="sgsst_empresa_politica_v")
            politica_fecha = st.date_input("Fecha política SST", value=parse_date_maybe(company.get("politica_fecha")) or date.today(), key="sgsst_empresa_politica_f")
            observaciones = st.text_area("Observaciones", value=str(company.get("observaciones") or ""), height=120, key="sgsst_empresa_obs")
        if st.button("Guardar ficha empresa", type="primary", key="sgsst_save_empresa"):
            now = datetime.now().isoformat(timespec='seconds')
            if company:
                execute(
                    """
                    UPDATE sgsst_empresa
                       SET razon_social=?, rut=?, direccion=?, actividad=?, organismo_admin=?, representantes=?, prevencionista=?, canal_denuncias=?,
                           dotacion_total=?, politica_version=?, politica_fecha=?, observaciones=?, updated_at=?
                     WHERE id=?
                    """,
                    (razon_social.strip(), clean_rut(rut), direccion.strip(), actividad.strip(), organismo_admin.strip(), representantes.strip(), prevencionista.strip(), canal.strip(), int(dotacion_total), politica_version.strip(), politica_fecha.isoformat(), observaciones.strip(), now, int(company.get("id"))),
                )
            else:
                execute(
                    """
                    INSERT INTO sgsst_empresa(razon_social, rut, direccion, actividad, organismo_admin, representantes, prevencionista, canal_denuncias, dotacion_total, politica_version, politica_fecha, observaciones, created_at, updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (razon_social.strip(), clean_rut(rut), direccion.strip(), actividad.strip(), organismo_admin.strip(), representantes.strip(), prevencionista.strip(), canal.strip(), int(dotacion_total), politica_version.strip(), politica_fecha.isoformat(), observaciones.strip(), now, now),
                )
            sgsst_log("Ficha empresa", "Guardar", f"Ficha empresa actualizada: {razon_social.strip()}")
            st.success("Ficha empresa guardada.")
            st.rerun()

    with tabs[4]:
        st.markdown("### Matriz legal")
        f1, f2 = st.columns([1, 1])
        norma_sel = f1.selectbox("Norma", ["(Todas)"] + SGSST_NORMAS, key="sgsst_matriz_norma")
        estado_sel = f2.selectbox("Estado", ["(Todos)"] + SGSST_ESTADOS, key="sgsst_matriz_estado")
        q = "SELECT id, norma, articulo, tema, obligacion, aplica_a, periodicidad, responsable, evidencia, estado, updated_at FROM sgsst_matriz_legal WHERE 1=1"
        params = []
        if norma_sel != "(Todas)":
            q += " AND norma=?"
            params.append(norma_sel)
        if estado_sel != "(Todos)":
            q += " AND estado=?"
            params.append(estado_sel)
        q += " ORDER BY norma, tema, id"
        df_matriz = fetch_df(q, tuple(params))
        st.dataframe(df_matriz, use_container_width=True, hide_index=True)

        a1, a2 = st.columns(2)
        with a1:
            st.markdown("#### Agregar obligación")
            m_norma = st.selectbox("Norma nueva", SGSST_NORMAS, key="sgsst_add_norma")
            m_art = st.text_input("Artículo / capítulo", key="sgsst_add_art")
            m_tema = st.text_input("Tema", key="sgsst_add_tema")
            m_ob = st.text_area("Obligación", key="sgsst_add_ob", height=90)
            m_ap = st.text_input("Aplica a", key="sgsst_add_ap")
            m_per = st.text_input("Periodicidad", key="sgsst_add_per")
            m_resp = st.text_input("Responsable", key="sgsst_add_resp")
            m_evi = st.text_input("Evidencia", key="sgsst_add_evi")
            m_estado = st.selectbox("Estado inicial", SGSST_ESTADOS, key="sgsst_add_estado")
            if st.button("Agregar a matriz legal", key="sgsst_add_matriz"):
                if not m_tema.strip() or not m_ob.strip():
                    st.error("Tema y obligación son obligatorios.")
                else:
                    now = datetime.now().isoformat(timespec='seconds')
                    execute(
                        "INSERT INTO sgsst_matriz_legal(norma, articulo, tema, obligacion, aplica_a, periodicidad, responsable, evidencia, estado, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (m_norma, m_art.strip(), m_tema.strip(), m_ob.strip(), m_ap.strip(), m_per.strip(), m_resp.strip(), m_evi.strip(), m_estado, now, now),
                    )
                    sgsst_log("Matriz legal", "Agregar", f"{m_norma} · {m_tema.strip()}")
                    st.success("Obligación agregada.")
                    st.rerun()
        with a2:
            st.markdown("#### Actualizar estado")
            if df_matriz.empty:
                st.caption("Sin filas para actualizar.")
            else:
                matriz_ids = df_matriz["id"].tolist()
                mid = st.selectbox("Fila", matriz_ids, format_func=lambda x: f"#{int(x)} · {df_matriz[df_matriz['id']==x].iloc[0]['norma']} / {df_matriz[df_matriz['id']==x].iloc[0]['tema']}", key="sgsst_edit_matriz_id")
                row = df_matriz[df_matriz["id"] == mid].iloc[0]
                estado_actual = str(row["estado"]) if str(row["estado"]) in SGSST_ESTADOS else SGSST_ESTADOS[0]
                u_estado = st.selectbox("Nuevo estado", SGSST_ESTADOS, index=SGSST_ESTADOS.index(estado_actual), key="sgsst_edit_matriz_estado")
                u_resp = st.text_input("Responsable", value=str(row.get("responsable") or ""), key="sgsst_edit_matriz_resp")
                u_evi = st.text_input("Evidencia", value=str(row.get("evidencia") or ""), key="sgsst_edit_matriz_evi")
                if st.button("Guardar estado", key="sgsst_upd_matriz"):
                    execute(
                        "UPDATE sgsst_matriz_legal SET estado=?, responsable=?, evidencia=?, updated_at=? WHERE id=?",
                        (u_estado, u_resp.strip(), u_evi.strip(), datetime.now().isoformat(timespec='seconds'), int(mid)),
                    )
                    sgsst_log("Matriz legal", "Actualizar", f"Fila #{int(mid)} → {u_estado}")
                    st.success("Matriz actualizada.")
                    st.rerun()
                if st.button("Recargar base legal", key="sgsst_seed_matriz"):
                    ensure_sgsst_seed_data()
                    st.success("Base legal verificada/cargada.")
                    st.rerun()

    with tabs[5]:
        st.markdown("### Programa anual preventivo")
        anio_view = st.number_input("Año", min_value=2024, max_value=2100, value=date.today().year, step=1, key="sgsst_prog_anio_view")
        df_prog = fetch_df(
            """
            SELECT p.id, p.anio, COALESCE(f.nombre,'(Empresa)') AS faena, p.objetivo, p.actividad, p.responsable, p.fecha_compromiso, p.estado, p.avance, p.evidencia
            FROM sgsst_programa_anual p
            LEFT JOIN faenas f ON f.id=p.faena_id
            WHERE p.anio=?
            ORDER BY p.fecha_compromiso, p.id
            """,
            (int(anio_view),),
        )
        st.dataframe(df_prog, use_container_width=True, hide_index=True)
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        faena_opts = [None] + faenas_df["id"].tolist() if not faenas_df.empty else [None]
        p1, p2 = st.columns(2)
        with p1:
            objetivo = st.text_input("Objetivo", key="sgsst_prog_obj")
            actividad = st.text_area("Actividad", key="sgsst_prog_act", height=90)
            responsable = st.text_input("Responsable", key="sgsst_prog_resp")
            fecha_comp = st.date_input("Fecha compromiso", value=date.today(), key="sgsst_prog_fecha")
        with p2:
            faena_id = st.selectbox("Faena vinculada", faena_opts, key="sgsst_prog_faena", format_func=lambda x: "(Empresa)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            estado = st.selectbox("Estado", SGSST_ESTADOS, key="sgsst_prog_estado")
            avance = st.slider("Avance %", min_value=0, max_value=100, value=0, step=5, key="sgsst_prog_avance")
            evidencia = st.text_input("Evidencia / entregable", key="sgsst_prog_evidencia")
        if st.button("Agregar actividad al programa", key="sgsst_add_prog"):
            if not objetivo.strip() or not actividad.strip():
                st.error("Objetivo y actividad son obligatorios.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_programa_anual(anio, objetivo, actividad, faena_id, responsable, fecha_compromiso, estado, avance, evidencia, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (int(anio_view), objetivo.strip(), actividad.strip(), faena_id, responsable.strip(), fecha_comp.isoformat(), estado, int(avance), evidencia.strip(), now, now),
                )
                sgsst_log("Programa anual", "Agregar", f"{anio_view} · {objetivo.strip()}")
                st.success("Actividad incorporada al programa anual.")
                st.rerun()

    with tabs[6]:
        st.markdown("### MIPER por faena, proceso y cargo")
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        faena_opts = [None] + faenas_df["id"].tolist() if not faenas_df.empty else [None]
        faena_filter = st.selectbox("Filtrar por faena", faena_opts, key="sgsst_miper_filter", format_func=lambda x: "(Todas)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
        q = """
            SELECT m.id, COALESCE(f.nombre,'(Empresa)') AS faena, m.proceso, m.tarea, m.cargo, m.peligro, m.riesgo, m.consecuencia,
                   m.probabilidad, m.severidad, m.nivel_riesgo, m.responsable, m.plazo, m.estado
            FROM sgsst_miper m
            LEFT JOIN faenas f ON f.id=m.faena_id
        """
        params = ()
        if faena_filter is not None:
            q += " WHERE m.faena_id=?"
            params = (int(faena_filter),)
        q += " ORDER BY m.nivel_riesgo DESC, m.id DESC"
        df_miper = fetch_df(q, params)
        st.dataframe(df_miper, use_container_width=True, hide_index=True)
        m1, m2, m3 = st.columns(3)
        with m1:
            m_faena = st.selectbox("Faena", faena_opts, key="sgsst_miper_faena", format_func=lambda x: "(Empresa)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            proceso = st.text_input("Proceso", key="sgsst_miper_proceso")
            tarea = st.text_input("Tarea", key="sgsst_miper_tarea")
            cargo = st.selectbox("Cargo", segav_cargo_labels(active_only=True), key="sgsst_miper_cargo")
        with m2:
            peligro = st.text_area("Peligro", key="sgsst_miper_peligro", height=80)
            riesgo = st.text_area("Riesgo", key="sgsst_miper_riesgo", height=80)
            consecuencia = st.text_area("Consecuencia", key="sgsst_miper_consecuencia", height=80)
            controles = st.text_area("Controles existentes", key="sgsst_miper_controles", height=80)
        with m3:
            prob = st.slider("Probabilidad", 1, 5, 3, key="sgsst_miper_prob")
            sev = st.slider("Severidad", 1, 5, 3, key="sgsst_miper_sev")
            nivel = int(prob) * int(sev)
            st.metric("Nivel de riesgo", nivel)
            medidas = st.text_area("Medidas / acciones", key="sgsst_miper_medidas", height=80)
            responsable = st.text_input("Responsable", key="sgsst_miper_resp")
            plazo = st.date_input("Plazo", value=date.today(), key="sgsst_miper_plazo")
            estado = st.selectbox("Estado", SGSST_ESTADOS, key="sgsst_miper_estado")
        if st.button("Agregar riesgo a la MIPER", key="sgsst_add_miper"):
            if not peligro.strip() or not riesgo.strip():
                st.error("Peligro y riesgo son obligatorios.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_miper(faena_id, proceso, tarea, cargo, peligro, riesgo, consecuencia, controles_existentes, probabilidad, severidad, nivel_riesgo, medidas, responsable, plazo, estado, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (m_faena, proceso.strip(), tarea.strip(), cargo, peligro.strip(), riesgo.strip(), consecuencia.strip(), controles.strip(), int(prob), int(sev), int(nivel), medidas.strip(), responsable.strip(), plazo.isoformat(), estado, now, now),
                )
                sgsst_log("MIPER", "Agregar", f"{cargo} · riesgo {nivel}")
                st.success("Riesgo incorporado a la MIPER.")
                st.rerun()

    with tabs[7]:
        st.markdown("### Inspecciones DS 594")
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        faena_opts = [None] + faenas_df["id"].tolist() if not faenas_df.empty else [None]
        ins_q = """
            SELECT i.id, COALESCE(f.nombre,'(Planta)') AS faena, i.tipo, i.area, i.item, i.resultado, i.responsable, i.plazo, i.observacion, i.accion_correctiva
            FROM sgsst_inspecciones i
            LEFT JOIN faenas f ON f.id=i.faena_id
            ORDER BY i.id DESC
        """
        st.dataframe(fetch_df(ins_q), use_container_width=True, hide_index=True)
        i1, i2 = st.columns(2)
        with i1:
            ins_faena = st.selectbox("Faena / planta", faena_opts, key="sgsst_ins_faena", format_func=lambda x: "PLANTA" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            ins_tipo = st.selectbox("Tipo inspección", ["DS 594", "Orden y aseo", "Extintores", "Campamento", "Otro"], key="sgsst_ins_tipo")
            ins_area = st.text_input("Área", key="sgsst_ins_area")
            ins_item = st.text_input("Ítem", key="sgsst_ins_item")
            ins_result = st.selectbox("Resultado", SGSST_RESULTADOS, key="sgsst_ins_result")
        with i2:
            ins_obs = st.text_area("Observación", key="sgsst_ins_obs", height=100)
            ins_accion = st.text_area("Acción correctiva", key="sgsst_ins_accion", height=100)
            ins_resp = st.text_input("Responsable", key="sgsst_ins_resp")
            ins_plazo = st.date_input("Plazo", value=date.today(), key="sgsst_ins_plazo")
        if st.button("Registrar inspección", key="sgsst_add_ins"):
            if not ins_item.strip():
                st.error("El ítem inspeccionado es obligatorio.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_inspecciones(faena_id, tipo, area, item, resultado, observacion, accion_correctiva, responsable, plazo, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (ins_faena, ins_tipo, ins_area.strip(), ins_item.strip(), ins_result, ins_obs.strip(), ins_accion.strip(), ins_resp.strip(), ins_plazo.isoformat(), now, now),
                )
                sgsst_log("Inspecciones DS 594", "Registrar", f"{ins_tipo} · {ins_item.strip()}")
                st.success("Inspección registrada.")
                st.rerun()

    with tabs[8]:
        st.markdown("### Accidentes e incidentes")
        trab_df = fetch_df("SELECT id, rut, apellidos, nombres FROM trabajadores ORDER BY apellidos, nombres")
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        trab_opts = [None] + trab_df["id"].tolist() if not trab_df.empty else [None]
        faena_opts = [None] + faenas_df["id"].tolist() if not faenas_df.empty else [None]
        df_inc = fetch_df(
            """
            SELECT i.id, i.fecha, i.tipo, i.gravedad, COALESCE(f.nombre,'(Sin faena)') AS faena,
                   CASE WHEN t.id IS NULL THEN '(Sin trabajador)' ELSE t.rut || ' · ' || t.apellidos || ', ' || t.nombres END AS trabajador,
                   i.estado, i.dias_perdidos, i.descripcion
            FROM sgsst_incidentes i
            LEFT JOIN faenas f ON f.id=i.faena_id
            LEFT JOIN trabajadores t ON t.id=i.trabajador_id
            ORDER BY i.fecha DESC, i.id DESC
            """
        )
        st.dataframe(df_inc, use_container_width=True, hide_index=True)
        x1, x2 = st.columns(2)
        with x1:
            inc_fecha = st.date_input("Fecha", value=date.today(), key="sgsst_inc_fecha")
            inc_tipo = st.selectbox("Tipo", SGSST_TIPOS_EVENTO, key="sgsst_inc_tipo")
            inc_grav = st.selectbox("Gravedad", SGSST_GRAVEDADES, key="sgsst_inc_grav")
            inc_trab = st.selectbox("Trabajador", trab_opts, key="sgsst_inc_trab", format_func=lambda x: "(Sin trabajador)" if x is None else f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}, {trab_df[trab_df['id']==x].iloc[0]['nombres']}")
            inc_faena = st.selectbox("Faena", faena_opts, key="sgsst_inc_faena", format_func=lambda x: "(Sin faena)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
        with x2:
            inc_desc = st.text_area("Descripción", key="sgsst_inc_desc", height=110)
            inc_oa = st.text_input("Organismo administrador", value=str(company.get("organismo_admin") or ""), key="sgsst_inc_oa")
            inc_dias = st.number_input("Días perdidos", min_value=0, value=0, step=1, key="sgsst_inc_dias")
            inc_med = st.text_area("Medidas correctivas", key="sgsst_inc_med", height=90)
            inc_estado = st.selectbox("Estado", SGSST_ESTADOS, key="sgsst_inc_estado")
        if st.button("Registrar evento", key="sgsst_add_inc"):
            if not inc_desc.strip():
                st.error("La descripción del evento es obligatoria.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_incidentes(trabajador_id, faena_id, fecha, tipo, gravedad, descripcion, organismo_admin, dias_perdidos, medidas, estado, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (inc_trab, inc_faena, inc_fecha.isoformat(), inc_tipo, inc_grav, inc_desc.strip(), inc_oa.strip(), int(inc_dias), inc_med.strip(), inc_estado, now, now),
                )
                sgsst_log("Accidentes e incidentes", "Registrar", f"{inc_tipo} · {inc_fecha.isoformat()}")
                st.success("Evento registrado.")
                st.rerun()

    with tabs[9]:
        st.markdown("### Capacitaciones y ODI")
        trab_df = fetch_df("SELECT id, rut, apellidos, nombres FROM trabajadores ORDER BY apellidos, nombres")
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        trab_opts = [None] + trab_df["id"].tolist() if not trab_df.empty else [None]
        faena_opts = [None] + faenas_df["id"].tolist() if not faenas_df.empty else [None]
        df_cap = fetch_df(
            """
            SELECT c.id, c.tipo, c.tema, c.fecha, c.vigencia, c.horas, c.relator, c.estado,
                   COALESCE(f.nombre,'(Sin faena)') AS faena,
                   CASE WHEN t.id IS NULL THEN '(Sin trabajador)' ELSE t.rut || ' · ' || t.apellidos || ', ' || t.nombres END AS trabajador
            FROM sgsst_capacitaciones c
            LEFT JOIN faenas f ON f.id=c.faena_id
            LEFT JOIN trabajadores t ON t.id=c.trabajador_id
            ORDER BY c.fecha DESC, c.id DESC
            """
        )
        st.dataframe(df_cap, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            cap_tipo = st.selectbox("Tipo", SGSST_TIPOS_CAP, key="sgsst_cap_tipo")
            cap_tema = st.text_input("Tema", key="sgsst_cap_tema")
            cap_fecha = st.date_input("Fecha ejecución", value=date.today(), key="sgsst_cap_fecha")
            cap_vig = st.date_input("Vigencia / próxima revisión", value=date.today() + timedelta(days=365), key="sgsst_cap_vig")
            cap_horas = st.number_input("Horas", min_value=0.0, value=1.0, step=0.5, key="sgsst_cap_horas")
        with c2:
            cap_relator = st.text_input("Relator / organismo", key="sgsst_cap_relator")
            cap_trab = st.selectbox("Trabajador", trab_opts, key="sgsst_cap_trab", format_func=lambda x: "(General)" if x is None else f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}, {trab_df[trab_df['id']==x].iloc[0]['nombres']}")
            cap_faena = st.selectbox("Faena", faena_opts, key="sgsst_cap_faena", format_func=lambda x: "(General)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            cap_estado = st.selectbox("Estado", ["VIGENTE", "POR VENCER", "VENCIDA"], key="sgsst_cap_estado")
            cap_evid = st.text_input("Evidencia", key="sgsst_cap_evid")
        if st.button("Registrar capacitación / ODI", key="sgsst_add_cap"):
            if not cap_tema.strip():
                st.error("El tema es obligatorio.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_capacitaciones(trabajador_id, faena_id, tipo, tema, fecha, vigencia, horas, relator, estado, evidencia, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (cap_trab, cap_faena, cap_tipo, cap_tema.strip(), cap_fecha.isoformat(), cap_vig.isoformat(), float(cap_horas), cap_relator.strip(), cap_estado, cap_evid.strip(), now, now),
                )
                sgsst_log("Capacitaciones y ODI", "Registrar", f"{cap_tipo} · {cap_tema.strip()}")
                st.success("Registro guardado.")
                st.rerun()

    with tabs[10]:
        st.markdown("### Bitácora de auditoría")
        aud_df = fetch_df("SELECT id, created_at, modulo, accion, detalle, usuario FROM sgsst_auditoria ORDER BY id DESC LIMIT 200")
        st.dataframe(aud_df, use_container_width=True, hide_index=True)
        st.caption("Esta bitácora registra acciones realizadas dentro del nuevo módulo ERP / SGSST.")
