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
    DS594_CHECKLIST_ITEMS=None,
    EPP_TIPOS=None,
    ROLES_EMPRESA=None,
    is_company_admin_for_active_tenant=None,
    save_company_logo_for_cliente=None,
    get_company_logo_bytes=None,
):
    ui_header("Mi Empresa / SGSST", "Núcleo comercializable de SEGAV ERP: configurable para cualquier empresa, sin reemplazar lo ya existente.")
    ensure_sgsst_seed_data()
    _u = current_user() or {}
    _is_superadmin = str(_u.get("role") or "").upper() == "SUPERADMIN"
    _sgsst_render_nonce = int(st.session_state.get("_sgsst_render_nonce", 0)) + 1
    st.session_state["_sgsst_render_nonce"] = _sgsst_render_nonce
    def K(name: str) -> str:
        return f"sgsst_{_sgsst_render_nonce}_{name}"


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
        "📋 Checklist DS 594",
        "🩹 Accidentes e Incidentes",
        "🎓 Capacitaciones y ODI",
        "🦺 EPP por Trabajador",
        "🧾 Auditoría",
        "👷 CPHS",
        "📝 DIAT / DIEP",
        "🔬 Vigilancia Ocupacional",
        "🏗️ Subcontratistas",
        "📕 RIOHS",
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
                if st.button("Ir a Docs Empresa", use_container_width=True, key=K("sgsst_go_docs_emp")):
                    go("Documentos Empresa")
            with b2:
                if st.button("Ir a Docs Faena", use_container_width=True, key=K("sgsst_go_docs_faena")):
                    go("Documentos Empresa (Faena)")
            with b3:
                if st.button("Ir a Trabajadores", use_container_width=True, key=K("sgsst_go_trab")):
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
            erp_name = st.text_input("Nombre comercial", value=cfg.get("erp_name", "SEGAV ERP"), key=K("segav_cfg_name"))
            erp_slogan = st.text_area("Propuesta de valor", value=cfg.get("erp_slogan", "ERP comercializable de cumplimiento, prevención y operación documental"), height=90, key=K("segav_cfg_slogan"))
            erp_vertical = st.text_input("Vertical / rubro base", value=cfg.get("erp_vertical", "General"), key=K("segav_cfg_vertical"))
        with z2:
            multiempresa = st.selectbox("Modo comercial", ["SI", "NO"], index=0 if cfg.get("multiempresa", "SI") == "SI" else 1, key=K("segav_cfg_multi"))
            cliente_actual = st.text_input("Cliente / empresa actual", value=cfg.get("cliente_actual", company.get("razon_social") or "Empresa actual"), key=K("segav_cfg_cliente"))
            impl_opts = ["CONFIGURABLE", "VERTICAL FORESTAL", "CORPORATIVO"]
            modo_impl = st.selectbox("Implementación", impl_opts, index=impl_opts.index(cfg.get("modo_implementacion", "CONFIGURABLE")) if cfg.get("modo_implementacion", "CONFIGURABLE") in impl_opts else 0, key=K("segav_cfg_impl"))
        if st.button("Guardar configuración ERP", key=K("segav_cfg_save"), type="primary"):
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
        tpl_sel = st.selectbox("Plantilla a visualizar/aplicar", tpl_options, index=tpl_options.index(current_tpl) if tpl_options else 0, key=K("segav_tpl_sel")) if tpl_options else None
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
            if st.button("Aplicar plantilla al catálogo ERP", key=K("segav_apply_tpl")):
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
        cargo_new_label = cadd1.text_input("Nuevo cargo", key=K("segav_new_cargo_label"))
        cargo_new_order = cadd2.number_input("Orden", min_value=1, value=int((len(cargos_df) if cargos_df is not None else 0) + 1), step=1, key=K("segav_new_cargo_order"))
        cargo_new_active = cadd3.selectbox("Activo", ["SI", "NO"], key=K("segav_new_cargo_active"))
        if st.button("Agregar cargo al catálogo", key=K("segav_add_cargo")):
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
        cargo_sel = st.selectbox("Cargo a parametrizar", cargo_labels, key=K("segav_docs_cargo_sel"))
        current_docs = segav_cargo_rules().get(cargo_sel, DOC_OBLIGATORIOS)
        docs_selected = st.multiselect("Documentos obligatorios por cargo", list(DOC_TIPO_LABELS.keys()), default=current_docs, key=K("segav_docs_cargo_multi"), format_func=doc_tipo_label)
        if st.button("Guardar documentos por cargo", key=K("segav_docs_cargo_save")):
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
        emp_docs_selected = st.multiselect("Documentos requeridos empresa/faena", list(DOC_TIPO_LABELS.keys()), default=get_empresa_monthly_doc_types(), key=K("segav_docs_empresa_multi"), format_func=doc_tipo_label)
        if st.button("Guardar documentos empresa/faena", key=K("segav_docs_empresa_save")):
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
            razon_social = st.text_input("Razón social", value=str(company.get("razon_social") or ""), key=K("sgsst_empresa_razon"))
            rut = st.text_input("RUT empresa", value=clean_rut(company.get("rut") or ""), key=K("sgsst_empresa_rut"))
            direccion = st.text_input("Dirección", value=str(company.get("direccion") or ""), key=K("sgsst_empresa_direccion"))
            actividad = st.text_input("Actividad / rubro", value=str(company.get("actividad") or ""), key=K("sgsst_empresa_actividad"))
            organismo_admin = st.text_input("Organismo administrador", value=str(company.get("organismo_admin") or ""), key=K("sgsst_empresa_oa"))
            dotacion_total = st.number_input("Dotación total", min_value=0, value=int(company.get("dotacion_total") or 0), step=1, key=K("sgsst_empresa_dotacion"))
        with e2:
            representantes = st.text_area("Representantes legales", value=str(company.get("representantes") or ""), height=90, key=K("sgsst_empresa_repr"))
            prevencionista = st.text_input("Prevencionista de riesgos", value=str(company.get("prevencionista") or ""), key=K("sgsst_empresa_prev"))
            canal = st.text_input("Canal de denuncias", value=str(company.get("canal_denuncias") or ""), key=K("sgsst_empresa_canal"))
            politica_version = st.text_input("Versión política SST", value=str(company.get("politica_version") or "1.0"), key=K("sgsst_empresa_politica_v"))
            politica_fecha = st.date_input("Fecha política SST", value=parse_date_maybe(company.get("politica_fecha")) or date.today(), key=K("sgsst_empresa_politica_f"))
            observaciones = st.text_area("Observaciones", value=str(company.get("observaciones") or ""), height=120, key=K("sgsst_empresa_obs"))
        if st.button("Guardar ficha empresa", type="primary", key=K("sgsst_save_empresa")):
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
        norma_sel = f1.selectbox("Norma", ["(Todas)"] + SGSST_NORMAS, key=K("sgsst_matriz_norma"))
        estado_sel = f2.selectbox("Estado", ["(Todos)"] + SGSST_ESTADOS, key=K("sgsst_matriz_estado"))
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
            m_norma = st.selectbox("Norma nueva", SGSST_NORMAS, key=K("sgsst_add_norma"))
            m_art = st.text_input("Artículo / capítulo", key=K("sgsst_add_art"))
            m_tema = st.text_input("Tema", key=K("sgsst_add_tema"))
            m_ob = st.text_area("Obligación", key=K("sgsst_add_ob"), height=90)
            m_ap = st.text_input("Aplica a", key=K("sgsst_add_ap"))
            m_per = st.text_input("Periodicidad", key=K("sgsst_add_per"))
            m_resp = st.text_input("Responsable", key=K("sgsst_add_resp"))
            m_evi = st.text_input("Evidencia", key=K("sgsst_add_evi"))
            m_estado = st.selectbox("Estado inicial", SGSST_ESTADOS, key=K("sgsst_add_estado"))
            if st.button("Agregar a matriz legal", key=K("sgsst_add_matriz")):
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
                mid = st.selectbox("Fila", matriz_ids, format_func=lambda x: f"#{int(x)} · {df_matriz[df_matriz['id']==x].iloc[0]['norma']} / {df_matriz[df_matriz['id']==x].iloc[0]['tema']}", key=K("sgsst_edit_matriz_id"))
                row = df_matriz[df_matriz["id"] == mid].iloc[0]
                estado_actual = str(row["estado"]) if str(row["estado"]) in SGSST_ESTADOS else SGSST_ESTADOS[0]
                u_estado = st.selectbox("Nuevo estado", SGSST_ESTADOS, index=SGSST_ESTADOS.index(estado_actual), key=K("sgsst_edit_matriz_estado"))
                u_resp = st.text_input("Responsable", value=str(row.get("responsable") or ""), key=K("sgsst_edit_matriz_resp"))
                u_evi = st.text_input("Evidencia", value=str(row.get("evidencia") or ""), key=K("sgsst_edit_matriz_evi"))
                if st.button("Guardar estado", key=K("sgsst_upd_matriz")):
                    execute(
                        "UPDATE sgsst_matriz_legal SET estado=?, responsable=?, evidencia=?, updated_at=? WHERE id=?",
                        (u_estado, u_resp.strip(), u_evi.strip(), datetime.now().isoformat(timespec='seconds'), int(mid)),
                    )
                    sgsst_log("Matriz legal", "Actualizar", f"Fila #{int(mid)} → {u_estado}")
                    st.success("Matriz actualizada.")
                    st.rerun()
                if st.button("Recargar base legal", key=K("sgsst_seed_matriz")):
                    ensure_sgsst_seed_data()
                    st.success("Base legal verificada/cargada.")
                    st.rerun()

    with tabs[5]:
        st.markdown("### Programa anual preventivo")
        anio_view = st.number_input("Año", min_value=2024, max_value=2100, value=date.today().year, step=1, key=K("sgsst_prog_anio_view"))
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
            objetivo = st.text_input("Objetivo", key=K("sgsst_prog_obj"))
            actividad = st.text_area("Actividad", key=K("sgsst_prog_act"), height=90)
            responsable = st.text_input("Responsable", key=K("sgsst_prog_resp"))
            fecha_comp = st.date_input("Fecha compromiso", value=date.today(), key=K("sgsst_prog_fecha"))
        with p2:
            faena_id = st.selectbox("Faena vinculada", faena_opts, key=K("sgsst_prog_faena"), format_func=lambda x: "(Empresa)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            estado = st.selectbox("Estado", SGSST_ESTADOS, key=K("sgsst_prog_estado"))
            avance = st.slider("Avance %", min_value=0, max_value=100, value=0, step=5, key=K("sgsst_prog_avance"))
            evidencia = st.text_input("Evidencia / entregable", key=K("sgsst_prog_evidencia"))
        if st.button("Agregar actividad al programa", key=K("sgsst_add_prog")):
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
        faena_filter = st.selectbox("Filtrar por faena", faena_opts, key=K("sgsst_miper_filter"), format_func=lambda x: "(Todas)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
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
            m_faena = st.selectbox("Faena", faena_opts, key=K("sgsst_miper_faena"), format_func=lambda x: "(Empresa)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            proceso = st.text_input("Proceso", key=K("sgsst_miper_proceso"))
            tarea = st.text_input("Tarea", key=K("sgsst_miper_tarea"))
            cargo = st.selectbox("Cargo", segav_cargo_labels(active_only=True), key=K("sgsst_miper_cargo"))
        with m2:
            peligro = st.text_area("Peligro", key=K("sgsst_miper_peligro"), height=80)
            riesgo = st.text_area("Riesgo", key=K("sgsst_miper_riesgo"), height=80)
            consecuencia = st.text_area("Consecuencia", key=K("sgsst_miper_consecuencia"), height=80)
            controles = st.text_area("Controles existentes", key=K("sgsst_miper_controles"), height=80)
        with m3:
            prob = st.slider("Probabilidad", 1, 5, 3, key=K("sgsst_miper_prob"))
            sev = st.slider("Severidad", 1, 5, 3, key=K("sgsst_miper_sev"))
            nivel = int(prob) * int(sev)
            st.metric("Nivel de riesgo", nivel)
            medidas = st.text_area("Medidas / acciones", key=K("sgsst_miper_medidas"), height=80)
            responsable = st.text_input("Responsable", key=K("sgsst_miper_resp"))
            plazo = st.date_input("Plazo", value=date.today(), key=K("sgsst_miper_plazo"))
            estado = st.selectbox("Estado", SGSST_ESTADOS, key=K("sgsst_miper_estado"))
        if st.button("Agregar riesgo a la MIPER", key=K("sgsst_add_miper")):
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
            ins_faena = st.selectbox("Faena / planta", faena_opts, key=K("sgsst_ins_faena"), format_func=lambda x: "PLANTA" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            ins_tipo = st.selectbox("Tipo inspección", ["DS 594", "Orden y aseo", "Extintores", "Campamento", "Otro"], key=K("sgsst_ins_tipo"))
            ins_area = st.text_input("Área", key=K("sgsst_ins_area"))
            ins_item = st.text_input("Ítem", key=K("sgsst_ins_item"))
            ins_result = st.selectbox("Resultado", SGSST_RESULTADOS, key=K("sgsst_ins_result"))
        with i2:
            ins_obs = st.text_area("Observación", key=K("sgsst_ins_obs"), height=100)
            ins_accion = st.text_area("Acción correctiva", key=K("sgsst_ins_accion"), height=100)
            ins_resp = st.text_input("Responsable", key=K("sgsst_ins_resp"))
            ins_plazo = st.date_input("Plazo", value=date.today(), key=K("sgsst_ins_plazo"))
        if st.button("Registrar inspección", key=K("sgsst_add_ins")):
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

    # ── TAB 8: CHECKLIST DS 594 ───────────────────────────────────────────
    with tabs[8]:
        st.markdown("### 📋 Checklist DS 594 — Inspección digital")
        st.caption("Checklist estandarizado para verificar cumplimiento de condiciones sanitarias y ambientales según DS 594.")
        _checklist_items = DS594_CHECKLIST_ITEMS or {}
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        faena_opts_ck = faenas_df["id"].tolist() if not faenas_df.empty else []
        if not faena_opts_ck:
            st.info("Crea una faena primero para realizar inspecciones.")
        else:
            ck_faena = st.selectbox("Faena a inspeccionar", faena_opts_ck, key=K("ck594_faena"),
                format_func=lambda x: str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            ck_inspector = st.text_input("Inspector", key=K("ck594_inspector"), placeholder="Nombre del inspector")
            ck_fecha = st.date_input("Fecha inspección", value=date.today(), key=K("ck594_fecha"))

            st.divider()
            results = {}
            obs_dict = {}
            for cat, items in _checklist_items.items():
                st.markdown(f"**{cat}**")
                for idx_item, item in enumerate(items):
                    c1, c2 = st.columns([3, 1])
                    with c1:
                        st.caption(item)
                    with c2:
                        key_ck = K(f"ck594_{cat[:8]}_{idx_item}")
                        results[(cat, item)] = st.checkbox("Cumple", value=True, key=key_ck)

            if st.button("💾 Guardar checklist completo", type="primary", use_container_width=True, key=K("ck594_save")):
                now = datetime.now().isoformat(timespec='seconds')
                saved = 0
                for (cat, item), cumple in results.items():
                    try:
                        execute(
                            "INSERT INTO sgsst_checklist_ds594(faena_id, fecha_inspeccion, inspector, categoria, item, cumple, created_at) VALUES(?,?,?,?,?,?,?)",
                            (int(ck_faena), ck_fecha.isoformat(), ck_inspector.strip(), cat, item, 1 if cumple else 0, now),
                        )
                        saved += 1
                    except Exception:
                        pass
                sgsst_log("Checklist DS 594", "Guardar", f"Faena {ck_faena} · {saved} ítems")
                st.success(f"Checklist guardado: {saved} ítems registrados.")
                st.rerun()

            st.divider()
            st.markdown("#### Historial de inspecciones")
            hist = fetch_df("""
                SELECT c.fecha_inspeccion, COALESCE(f.nombre,'?') AS faena, c.inspector,
                       c.categoria, c.item, CASE WHEN c.cumple THEN '✅' ELSE '❌' END AS resultado
                FROM sgsst_checklist_ds594 c
                LEFT JOIN faenas f ON f.id=c.faena_id
                ORDER BY c.id DESC LIMIT 100
            """)
            if hist is not None and not hist.empty:
                st.dataframe(hist.rename(columns={
                    "fecha_inspeccion":"Fecha","faena":"Faena","inspector":"Inspector",
                    "categoria":"Categoría","item":"Ítem","resultado":"Cumple"
                }), use_container_width=True, hide_index=True)
            else:
                st.info("Aún no hay inspecciones registradas.")

    with tabs[9]:
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
            inc_fecha = st.date_input("Fecha", value=date.today(), key=K("sgsst_inc_fecha"))
            inc_tipo = st.selectbox("Tipo", SGSST_TIPOS_EVENTO, key=K("sgsst_inc_tipo"))
            inc_grav = st.selectbox("Gravedad", SGSST_GRAVEDADES, key=K("sgsst_inc_grav"))
            inc_trab = st.selectbox("Trabajador", trab_opts, key=K("sgsst_inc_trab"), format_func=lambda x: "(Sin trabajador)" if x is None else f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}, {trab_df[trab_df['id']==x].iloc[0]['nombres']}")
            inc_faena = st.selectbox("Faena", faena_opts, key=K("sgsst_inc_faena"), format_func=lambda x: "(Sin faena)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
        with x2:
            inc_desc = st.text_area("Descripción", key=K("sgsst_inc_desc"), height=110)
            inc_oa = st.text_input("Organismo administrador", value=str(company.get("organismo_admin") or ""), key=K("sgsst_inc_oa"))
            inc_dias = st.number_input("Días perdidos", min_value=0, value=0, step=1, key=K("sgsst_inc_dias"))
            inc_med = st.text_area("Medidas correctivas", key=K("sgsst_inc_med"), height=90)
            inc_estado = st.selectbox("Estado", SGSST_ESTADOS, key=K("sgsst_inc_estado"))
        if st.button("Registrar evento", key=K("sgsst_add_inc")):
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

    with tabs[10]:
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
            cap_tipo = st.selectbox("Tipo", SGSST_TIPOS_CAP, key=K("sgsst_cap_tipo"))
            cap_tema = st.text_input("Tema", key=K("sgsst_cap_tema"))
            cap_fecha = st.date_input("Fecha ejecución", value=date.today(), key=K("sgsst_cap_fecha"))
            cap_vig = st.date_input("Vigencia / próxima revisión", value=date.today() + timedelta(days=365), key=K("sgsst_cap_vig"))
            cap_horas = st.number_input("Horas", min_value=0.0, value=1.0, step=0.5, key=K("sgsst_cap_horas"))
        with c2:
            cap_relator = st.text_input("Relator / organismo", key=K("sgsst_cap_relator"))
            cap_trab = st.selectbox("Trabajador", trab_opts, key=K("sgsst_cap_trab"), format_func=lambda x: "(General)" if x is None else f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}, {trab_df[trab_df['id']==x].iloc[0]['nombres']}")
            cap_faena = st.selectbox("Faena", faena_opts, key=K("sgsst_cap_faena"), format_func=lambda x: "(General)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            cap_estado = st.selectbox("Estado", ["VIGENTE", "POR VENCER", "VENCIDA"], key=K("sgsst_cap_estado"))
            cap_evid = st.text_input("Evidencia", key=K("sgsst_cap_evid"))
        if st.button("Registrar capacitación / ODI", key=K("sgsst_add_cap")):
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

    # ── TAB 11: EPP POR TRABAJADOR ────────────────────────────────────────
    with tabs[11]:
        st.markdown("### 🦺 Entrega de EPP por Trabajador")
        st.caption("Registro de entrega de Elementos de Protección Personal según DS 594 Art. 53-55. Trazabilidad completa por trabajador.")

        _epp_tipos = EPP_TIPOS or ["Casco","Guantes","Lentes","Calzado","Otro"]
        trab_df = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")

        if trab_df is None or trab_df.empty:
            st.info("No hay trabajadores registrados.")
        else:
            # ── Historial ─────────────────────────────────────────────────
            epp_hist = fetch_df("""
                SELECT e.id, t.rut, t.apellidos||' '||t.nombres AS trabajador, t.cargo,
                       COALESCE(f.nombre,'PLANTA') AS faena,
                       e.epp_tipo, e.fecha_entrega, e.fecha_vencimiento, e.cantidad, e.talla, e.marca, e.observacion
                FROM sgsst_epp_entrega e
                JOIN trabajadores t ON t.id=e.trabajador_id
                LEFT JOIN faenas f ON f.id=e.faena_id
                ORDER BY e.fecha_entrega DESC, e.id DESC
            """)
            if epp_hist is not None and not epp_hist.empty:
                epp_q = st.text_input("🔍 Filtrar entregas", key=K("epp_q"), placeholder="RUT, nombre, EPP…")
                epp_view = epp_hist.copy()
                if epp_q.strip():
                    qq = epp_q.strip().lower()
                    mask = epp_view.apply(lambda r: any(qq in str(v).lower() for v in r), axis=1)
                    epp_view = epp_view[mask]
                st.dataframe(epp_view.rename(columns={
                    "rut":"RUT","trabajador":"Trabajador","cargo":"Cargo","faena":"Faena",
                    "epp_tipo":"EPP","fecha_entrega":"Entrega","fecha_vencimiento":"Vencimiento",
                    "cantidad":"Cant.","talla":"Talla","marca":"Marca","observacion":"Obs."
                }), use_container_width=True, hide_index=True)
                st.caption(f"{len(epp_view)} registros de entrega")
            else:
                st.info("Aún no hay entregas de EPP registradas.")

            st.divider()
            st.markdown("#### ➕ Registrar entrega de EPP")
            e1, e2 = st.columns(2)
            with e1:
                epp_trab = st.selectbox("Trabajador", trab_df["id"].tolist(), key=K("epp_trab"),
                    format_func=lambda x: f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}, {trab_df[trab_df['id']==x].iloc[0]['nombres']}")
                epp_tipo = st.selectbox("Tipo de EPP", _epp_tipos, key=K("epp_tipo"))
                epp_fecha = st.date_input("Fecha entrega", value=date.today(), key=K("epp_fecha"))
                epp_venc = st.date_input("Fecha vencimiento (opcional)", value=None, key=K("epp_venc"))
            with e2:
                faena_opts_epp = [None] + (faenas_df["id"].tolist() if not faenas_df.empty else [])
                epp_faena = st.selectbox("Faena", faena_opts_epp, key=K("epp_faena"),
                    format_func=lambda x: "PLANTA" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
                epp_cant = st.number_input("Cantidad", min_value=1, value=1, key=K("epp_cant"))
                epp_talla = st.text_input("Talla", key=K("epp_talla"))
                epp_marca = st.text_input("Marca", key=K("epp_marca"))
            epp_obs = st.text_input("Observación", key=K("epp_obs"))

            if st.button("💾 Registrar entrega EPP", type="primary", use_container_width=True, key=K("epp_save")):
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_epp_entrega(trabajador_id, faena_id, epp_tipo, fecha_entrega, fecha_vencimiento, cantidad, talla, marca, observacion, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (int(epp_trab), int(epp_faena) if epp_faena else None, epp_tipo,
                     epp_fecha.isoformat(), epp_venc.isoformat() if epp_venc else None,
                     int(epp_cant), epp_talla.strip(), epp_marca.strip(), epp_obs.strip(), now, now),
                )
                sgsst_log("EPP", "Entrega", f"{epp_tipo} a trabajador {epp_trab}")
                st.success("Entrega de EPP registrada.")
                st.rerun()

    with tabs[12]:
        st.markdown("### Bitácora de auditoría")
        aud_df = fetch_df("SELECT id, created_at, modulo, accion, detalle, usuario FROM sgsst_auditoria ORDER BY id DESC LIMIT 200")
        st.dataframe(aud_df, use_container_width=True, hide_index=True)
        st.caption("Esta bitácora registra acciones realizadas dentro del nuevo módulo ERP / SGSST.")

    # ── TAB 13: CPHS ─────────────────────────────────────────────────────
    with tabs[13]:
        st.markdown("### 👷 Comité Paritario de Higiene y Seguridad (CPHS)")
        st.caption("Ley 16.744 Art. 65-71: obligatorio para empresas con ≥25 trabajadores. Gestiona elecciones, miembros y actas de reunión.")

        # Comité vigente
        cphs_df = fetch_df("SELECT * FROM sgsst_cphs ORDER BY id DESC LIMIT 1")
        if cphs_df is not None and not cphs_df.empty:
            cphs = cphs_df.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Estado", str(cphs.get("estado","VIGENTE")))
            c2.metric("Dotación actual", int(cphs.get("dotacion_actual",0) or 0))
            c3.metric("Vigente hasta", str(cphs.get("vigencia_hasta","Sin definir") or "Sin definir"))
            st.markdown(f"**Presidente:** {cphs.get('presidente','-')} · **Secretario:** {cphs.get('secretario','-')}")
            st.markdown(f"**Rep. Empresa:** {cphs.get('representantes_empresa','-')}")
            st.markdown(f"**Rep. Trabajadores:** {cphs.get('representantes_trabajadores','-')}")
        else:
            st.info("No hay CPHS constituido. Registra uno abajo.")

        st.divider()
        with st.expander("➕ Constituir / Actualizar CPHS", expanded=cphs_df is None or cphs_df.empty):
            cp1, cp2 = st.columns(2)
            with cp1:
                cp_pres = st.text_input("Presidente", key=K("cphs_pres"))
                cp_sec = st.text_input("Secretario", key=K("cphs_sec"))
                cp_re = st.text_area("Representantes empresa (uno por línea)", key=K("cphs_re"), height=80)
                cp_dot = st.number_input("Dotación actual", min_value=0, value=0, key=K("cphs_dot"))
            with cp2:
                cp_rt = st.text_area("Representantes trabajadores (uno por línea)", key=K("cphs_rt"), height=80)
                cp_elec = st.date_input("Fecha elección", value=date.today(), key=K("cphs_elec"))
                cp_vig = st.date_input("Vigencia hasta", value=date.today() + timedelta(days=730), key=K("cphs_vig"))
            if st.button("💾 Guardar CPHS", type="primary", key=K("cphs_save")):
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_cphs(fecha_eleccion, vigencia_hasta, representantes_empresa, representantes_trabajadores, presidente, secretario, dotacion_actual, estado, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (cp_elec.isoformat(), cp_vig.isoformat(), cp_re.strip(), cp_rt.strip(), cp_pres.strip(), cp_sec.strip(), int(cp_dot), "VIGENTE", now, now),
                )
                sgsst_log("CPHS", "Constituir", f"Presidente: {cp_pres}")
                st.success("CPHS registrado.")
                st.rerun()

        st.divider()
        st.markdown("#### Actas de reunión")
        actas = fetch_df("SELECT a.id, a.fecha, a.numero_acta, a.temas, a.acuerdos, a.estado FROM sgsst_cphs_actas a ORDER BY a.fecha DESC LIMIT 50")
        if actas is not None and not actas.empty:
            st.dataframe(actas.rename(columns={"fecha":"Fecha","numero_acta":"N° Acta","temas":"Temas","acuerdos":"Acuerdos","estado":"Estado"}), use_container_width=True, hide_index=True)

        with st.expander("➕ Registrar acta de reunión"):
            ac_fecha = st.date_input("Fecha reunión", value=date.today(), key=K("acta_fecha"))
            ac_num = st.text_input("N° de acta", key=K("acta_num"))
            ac_asist = st.text_area("Asistentes", key=K("acta_asist"), height=60)
            ac_temas = st.text_area("Temas tratados", key=K("acta_temas"), height=80)
            ac_acuerdos = st.text_area("Acuerdos", key=K("acta_acuerdos"), height=80)
            if st.button("💾 Guardar acta", key=K("acta_save")):
                cphs_id = int(cphs_df.iloc[0]["id"]) if cphs_df is not None and not cphs_df.empty else None
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_cphs_actas(cphs_id, fecha, numero_acta, asistentes, temas, acuerdos, estado, created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (cphs_id, ac_fecha.isoformat(), ac_num.strip(), ac_asist.strip(), ac_temas.strip(), ac_acuerdos.strip(), "ABIERTA", now),
                )
                sgsst_log("CPHS", "Acta", f"Acta {ac_num}")
                st.success("Acta registrada.")
                st.rerun()

    # ── TAB 14: DIAT / DIEP ──────────────────────────────────────────────
    with tabs[14]:
        st.markdown("### 📝 Denuncia Individual de Accidente del Trabajo (DIAT) / Enfermedad Profesional (DIEP)")
        st.caption("Ley 16.744 Art. 76: toda empresa debe denunciar accidentes dentro de 24 horas al organismo administrador.")

        diat_df = fetch_df("""
            SELECT d.id, d.tipo, d.fecha_accidente, d.fecha_denuncia, d.numero_denuncia,
                   COALESCE(t.rut,'') AS rut, COALESCE(t.apellidos||' '||t.nombres,'Sin asignar') AS trabajador,
                   COALESCE(f.nombre,'PLANTA') AS faena,
                   d.tipo_lesion, d.parte_cuerpo, d.dias_perdidos, d.estado
            FROM sgsst_diat_diep d
            LEFT JOIN trabajadores t ON t.id=d.trabajador_id
            LEFT JOIN faenas f ON f.id=d.faena_id
            ORDER BY d.fecha_accidente DESC, d.id DESC
        """)
        if diat_df is not None and not diat_df.empty:
            st.dataframe(diat_df.rename(columns={
                "tipo":"Tipo","fecha_accidente":"Fecha Acc.","fecha_denuncia":"Fecha Denuncia",
                "numero_denuncia":"N° Denuncia","rut":"RUT","trabajador":"Trabajador",
                "faena":"Faena","tipo_lesion":"Lesión","parte_cuerpo":"Parte cuerpo",
                "dias_perdidos":"Días perdidos","estado":"Estado"
            }), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### ➕ Registrar DIAT / DIEP")
        trab_df = fetch_df("SELECT id, rut, apellidos, nombres FROM trabajadores ORDER BY apellidos")
        faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
        d1, d2 = st.columns(2)
        with d1:
            di_tipo = st.selectbox("Tipo de denuncia", ["DIAT", "DIEP"], key=K("diat_tipo"))
            di_trab = st.selectbox("Trabajador", [None] + (trab_df["id"].tolist() if trab_df is not None and not trab_df.empty else []), key=K("diat_trab"),
                format_func=lambda x: "(Sin asignar)" if x is None else f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}")
            di_faena = st.selectbox("Faena", [None] + (faenas_df["id"].tolist() if faenas_df is not None and not faenas_df.empty else []), key=K("diat_faena"),
                format_func=lambda x: "PLANTA" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            di_fecha_acc = st.date_input("Fecha del accidente", value=date.today(), key=K("diat_fecha_acc"))
            di_hora = st.text_input("Hora del accidente", key=K("diat_hora"), placeholder="HH:MM")
            di_fecha_den = st.date_input("Fecha de denuncia", value=date.today(), key=K("diat_fecha_den"))
            di_num = st.text_input("N° de denuncia", key=K("diat_num"))
        with d2:
            di_lugar = st.text_input("Lugar del accidente", key=K("diat_lugar"))
            di_desc = st.text_area("Descripción del accidente", key=K("diat_desc"), height=80)
            di_lesion = st.text_input("Tipo de lesión", key=K("diat_lesion"), placeholder="Fractura, contusión, etc.")
            di_parte = st.text_input("Parte del cuerpo afectada", key=K("diat_parte"))
            di_dias = st.number_input("Días perdidos", min_value=0, value=0, key=K("diat_dias"))
            di_testigos = st.text_input("Testigos", key=K("diat_testigos"))
            di_org = st.text_input("Organismo administrador", key=K("diat_org"), placeholder="ACHS, IST, Mutual…")

        if st.button("💾 Registrar denuncia", type="primary", use_container_width=True, key=K("diat_save")):
            if not di_desc.strip():
                st.error("La descripción del accidente es obligatoria.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_diat_diep(tipo, trabajador_id, faena_id, fecha_accidente, hora_accidente, fecha_denuncia, numero_denuncia, lugar, descripcion, tipo_lesion, parte_cuerpo, dias_perdidos, testigos, organismo_admin, estado, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (di_tipo, di_trab, di_faena, di_fecha_acc.isoformat(), di_hora.strip(), di_fecha_den.isoformat(), di_num.strip(), di_lugar.strip(), di_desc.strip(), di_lesion.strip(), di_parte.strip(), int(di_dias), di_testigos.strip(), di_org.strip(), "ABIERTO", now, now),
                )
                sgsst_log("DIAT/DIEP", "Registrar", f"{di_tipo} · {di_desc[:40]}")
                st.success("Denuncia registrada.")
                st.rerun()

    # ── TAB 15: VIGILANCIA OCUPACIONAL ────────────────────────────────────
    with tabs[15]:
        st.markdown("### 🔬 Protocolos de Vigilancia Ocupacional")
        st.caption("PREXOR (ruido), PLANESI (sílice), TMERT (trastornos musculoesqueléticos), y otros protocolos según DS 594.")

        PROTOCOLOS = ["PREXOR (Ruido)", "PLANESI (Sílice)", "TMERT (Musculoesquelético)", "Hipobaria intermitente", "Plaguicidas", "Radiación UV", "Psicosocial ISTAS-21", "Otro"]
        vig_df = fetch_df("""
            SELECT v.id, v.protocolo, COALESCE(t.rut,'') AS rut,
                   COALESCE(t.apellidos||' '||t.nombres,'General') AS trabajador,
                   COALESCE(f.nombre,'PLANTA') AS faena,
                   v.agente, v.nivel_exposicion, v.fecha_evaluacion, v.fecha_proxima, v.resultado, v.estado
            FROM sgsst_vigilancia v
            LEFT JOIN trabajadores t ON t.id=v.trabajador_id
            LEFT JOIN faenas f ON f.id=v.faena_id
            ORDER BY v.fecha_evaluacion DESC, v.id DESC
        """)
        if vig_df is not None and not vig_df.empty:
            st.dataframe(vig_df.rename(columns={
                "protocolo":"Protocolo","rut":"RUT","trabajador":"Trabajador","faena":"Faena",
                "agente":"Agente","nivel_exposicion":"Nivel","fecha_evaluacion":"Evaluación",
                "fecha_proxima":"Próxima","resultado":"Resultado","estado":"Estado"
            }), use_container_width=True, hide_index=True)
        else:
            st.info("No hay evaluaciones registradas.")

        st.divider()
        with st.expander("➕ Registrar evaluación de vigilancia"):
            trab_df = fetch_df("SELECT id, rut, apellidos, nombres FROM trabajadores ORDER BY apellidos")
            faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
            v1, v2 = st.columns(2)
            with v1:
                vi_prot = st.selectbox("Protocolo", PROTOCOLOS, key=K("vig_prot"))
                vi_trab = st.selectbox("Trabajador", [None] + (trab_df["id"].tolist() if trab_df is not None and not trab_df.empty else []), key=K("vig_trab"),
                    format_func=lambda x: "(General)" if x is None else f"{trab_df[trab_df['id']==x].iloc[0]['rut']} · {trab_df[trab_df['id']==x].iloc[0]['apellidos']}")
                vi_faena = st.selectbox("Faena", [None] + (faenas_df["id"].tolist() if faenas_df is not None and not faenas_df.empty else []), key=K("vig_faena"),
                    format_func=lambda x: "PLANTA" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
                vi_agente = st.text_input("Agente de riesgo", key=K("vig_agente"), placeholder="Ruido, sílice, etc.")
            with v2:
                vi_nivel = st.selectbox("Nivel de exposición", ["BAJO", "MEDIO", "ALTO", "CRÍTICO"], key=K("vig_nivel"))
                vi_fecha = st.date_input("Fecha evaluación", value=date.today(), key=K("vig_fecha"))
                vi_prox = st.date_input("Próxima evaluación", value=date.today() + timedelta(days=365), key=K("vig_prox"))
                vi_result = st.text_input("Resultado", key=K("vig_result"))
                vi_medidas = st.text_area("Medidas de control", key=K("vig_medidas"), height=60)
            if st.button("💾 Registrar evaluación", key=K("vig_save")):
                now = datetime.now().isoformat(timespec='seconds')
                execute(
                    "INSERT INTO sgsst_vigilancia(protocolo, trabajador_id, faena_id, agente, nivel_exposicion, fecha_evaluacion, fecha_proxima, resultado, medidas, estado, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (vi_prot, vi_trab, vi_faena, vi_agente.strip(), vi_nivel, vi_fecha.isoformat(), vi_prox.isoformat(), vi_result.strip(), vi_medidas.strip(), "VIGENTE", now, now),
                )
                sgsst_log("Vigilancia", "Evaluación", f"{vi_prot} · {vi_agente}")
                st.success("Evaluación registrada.")
                st.rerun()

    # ── TAB 16: SUBCONTRATISTAS ───────────────────────────────────────────
    with tabs[16]:
        st.markdown("### 🏗️ Coordinación de Subcontratistas")
        st.caption("Ley 20.123 / Art. 66 bis Ley 16.744: coordinación obligatoria con empresas contratistas y subcontratistas.")

        sub_df = fetch_df("""
            SELECT s.id, s.rut_empresa, s.razon_social,
                   COALESCE(m.nombre,'Sin mandante') AS mandante,
                   COALESCE(f.nombre,'Sin faena') AS faena,
                   s.contacto, s.estado, s.docs_al_dia, s.fecha_inicio, s.fecha_termino
            FROM sgsst_subcontratistas s
            LEFT JOIN mandantes m ON m.id=s.mandante_id
            LEFT JOIN faenas f ON f.id=s.faena_id
            ORDER BY s.id DESC
        """)
        if sub_df is not None and not sub_df.empty:
            view_sub = sub_df.copy()
            view_sub["docs_al_dia"] = view_sub["docs_al_dia"].apply(lambda x: "✅" if x else "❌")
            st.dataframe(view_sub.rename(columns={
                "rut_empresa":"RUT","razon_social":"Empresa","mandante":"Mandante","faena":"Faena",
                "contacto":"Contacto","estado":"Estado","docs_al_dia":"Docs OK",
                "fecha_inicio":"Inicio","fecha_termino":"Término"
            }), use_container_width=True, hide_index=True)
        else:
            st.info("No hay subcontratistas registrados.")

        st.divider()
        with st.expander("➕ Registrar subcontratista"):
            mand_df = fetch_df("SELECT id, nombre FROM mandantes ORDER BY nombre")
            faenas_df = fetch_df("SELECT id, nombre FROM faenas ORDER BY nombre")
            s1, s2 = st.columns(2)
            with s1:
                su_rut = st.text_input("RUT empresa subcontratista", key=K("sub_rut"))
                su_razon = st.text_input("Razón social", key=K("sub_razon"))
                su_mand = st.selectbox("Mandante", [None] + (mand_df["id"].tolist() if mand_df is not None and not mand_df.empty else []), key=K("sub_mand"),
                    format_func=lambda x: "(Sin mandante)" if x is None else str(mand_df[mand_df['id']==x].iloc[0]['nombre']))
                su_faena = st.selectbox("Faena", [None] + (faenas_df["id"].tolist() if faenas_df is not None and not faenas_df.empty else []), key=K("sub_faena"),
                    format_func=lambda x: "(Sin faena)" if x is None else str(faenas_df[faenas_df['id']==x].iloc[0]['nombre']))
            with s2:
                su_contacto = st.text_input("Contacto", key=K("sub_contacto"))
                su_email = st.text_input("Email", key=K("sub_email"))
                su_tel = st.text_input("Teléfono", key=K("sub_tel"))
                su_fi = st.date_input("Fecha inicio contrato", value=date.today(), key=K("sub_fi"))
                su_ft = st.date_input("Fecha término (opcional)", value=None, key=K("sub_ft"))
            if st.button("💾 Registrar subcontratista", key=K("sub_save")):
                if not su_razon.strip():
                    st.error("Razón social es obligatoria.")
                else:
                    now = datetime.now().isoformat(timespec='seconds')
                    execute(
                        "INSERT INTO sgsst_subcontratistas(rut_empresa, razon_social, mandante_id, faena_id, contacto, email, telefono, fecha_inicio, fecha_termino, estado, docs_al_dia, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (su_rut.strip(), su_razon.strip(), su_mand, su_faena, su_contacto.strip(), su_email.strip(), su_tel.strip(), su_fi.isoformat(), su_ft.isoformat() if su_ft else None, "ACTIVO", 0, now, now),
                    )
                    sgsst_log("Subcontratistas", "Registrar", su_razon.strip())
                    st.success("Subcontratista registrado.")
                    st.rerun()

    # ── TAB 17: RIOHS ────────────────────────────────────────────────────
    with tabs[17]:
        st.markdown("### 📕 Reglamento Interno de Orden, Higiene y Seguridad (RIOHS)")
        st.caption("DS 44 Art. 12: toda empresa debe mantener un RIOHS actualizado y entregado a cada trabajador con cargo de recepción.")

        riohs_df = fetch_df("SELECT id, version, fecha_vigencia, aprobado_por, observaciones, created_at FROM sgsst_riohs ORDER BY id DESC")
        if riohs_df is not None and not riohs_df.empty:
            st.dataframe(riohs_df.rename(columns={
                "version":"Versión","fecha_vigencia":"Vigencia desde",
                "aprobado_por":"Aprobado por","observaciones":"Observaciones",
                "created_at":"Fecha registro"
            }), use_container_width=True, hide_index=True)
            latest = riohs_df.iloc[0]
            st.success(f"Versión vigente: **{latest.get('version','?')}** desde {latest.get('fecha_vigencia','?')}")
        else:
            st.info("No hay versiones del RIOHS registradas.")

        st.divider()
        with st.expander("➕ Registrar nueva versión del RIOHS"):
            ri_ver = st.text_input("Versión", key=K("riohs_ver"), placeholder="v2.0 - 2025")
            ri_fecha = st.date_input("Vigente desde", value=date.today(), key=K("riohs_fecha"))
            ri_aprob = st.text_input("Aprobado por", key=K("riohs_aprob"))
            ri_obs = st.text_area("Observaciones / cambios principales", key=K("riohs_obs"), height=80)
            if st.button("💾 Registrar versión RIOHS", key=K("riohs_save")):
                if not ri_ver.strip():
                    st.error("La versión es obligatoria.")
                else:
                    now = datetime.now().isoformat(timespec='seconds')
                    execute(
                        "INSERT INTO sgsst_riohs(version, fecha_vigencia, aprobado_por, observaciones, created_at) VALUES(?,?,?,?,?)",
                        (ri_ver.strip(), ri_fecha.isoformat(), ri_aprob.strip(), ri_obs.strip(), now),
                    )
                    sgsst_log("RIOHS", "Nueva versión", ri_ver.strip())
                    st.success("Versión RIOHS registrada.")
                    st.rerun()
