from __future__ import annotations

import streamlit as st

from segav_core.ui import ui_header


def page_documentos_empresa(
    *,
    fetch_df,
    get_empresa_required_doc_types,
    doc_tipo_join,
    doc_tipo_label,
    render_upload_help,
    prepare_upload_payload,
    safe_name,
    save_file_online,
    sha256_bytes,
    execute,
    datetime,
    auto_backup_db,
    load_file_anywhere,
    delete_uploaded_document_record,
):
    ui_header("Documentos Empresa", "Carga documentos corporativos (valen para todas las faenas) y se incluyen en el ZIP de exportación.")
    st.caption("Puedes subir múltiples archivos por tipo. Los tipos requeridos base son liquidaciones de sueldo, F30, F30-1 y certificado de accidentabilidad; además puedes crear tus propios tipos con OTRO.")

    df = fetch_df("SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path, created_at FROM empresa_documentos ORDER BY id DESC")
    tipos_presentes = set(df["doc_tipo"].astype(str).tolist()) if not df.empty else set()
    faltan = [d for d in get_empresa_required_doc_types() if d not in tipos_presentes]

    c1, c2, c3 = st.columns([1, 1, 2])
    c1.metric("Tipos requeridos", len(get_empresa_required_doc_types()))
    c2.metric("Tipos presentes", len(set(tipos_presentes)))
    c3.metric("Faltan requeridos", len(faltan))

    if faltan:
        st.warning("Faltan requeridos: " + doc_tipo_join(faltan))
    else:
        st.success("Requeridos completos (si aplica).")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos requeridos base:")
        st.code("\n".join(doc_tipo_label(d) for d in get_empresa_required_doc_types()))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", get_empresa_required_doc_types() + ["OTRO"], format_func=lambda x: "OTRO" if x == "OTRO" else doc_tipo_label(x))
        with colx2:
            tipo_otro = st.text_input("Si eliges OTRO, escribe el nombre", placeholder="Ej: Política SST, Organigrama, Procedimiento crítico...")

        up = st.file_uploader("Archivo", key="up_doc_empresa", type=None)
        render_upload_help()
        if st.button("Guardar documento empresa", type="primary"):
            if up is None:
                st.error("Debes subir un archivo.")
                st.stop()
            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, 'type', None) or 'application/octet-stream')
            folder = ["empresa", safe_name(doc_tipo)]
            file_path, bucket, object_path = save_file_online(folder, payload["file_name"], payload["file_bytes"], content_type=payload["content_type"])
            sha = sha256_bytes(payload["file_bytes"])
            execute(
                "INSERT INTO empresa_documentos(doc_tipo, nombre_archivo, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?)",
                (doc_tipo, payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])
            st.success("Documento empresa guardado.")
            auto_backup_db("doc_empresa")
            st.rerun()

    with tab2:
        if df.empty:
            st.info("(sin documentos empresa)")
        else:
            docs = df.copy()
            show = (
                docs[["doc_tipo", "nombre_archivo", "created_at"]].copy()
                if all(c in docs.columns for c in ["doc_tipo", "nombre_archivo", "created_at"])
                else docs.copy()
            )
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### 🔎 Gestionar documento")
            pick_id = st.selectbox(
                "Documento",
                docs["id"].tolist(),
                format_func=lambda x: f"{docs[docs['id']==x].iloc[0]['doc_tipo']} — {docs[docs['id']==x].iloc[0]['nombre_archivo']}",
                key="emp_pick_doc",
            )
            row = docs[docs["id"] == pick_id].iloc[0]
            fpath = row.get("file_path", "")
            bucket = row.get("bucket", None)
            object_path = row.get("object_path", None)
            fname = row.get("nombre_archivo", "documento")
            try:
                b = load_file_anywhere(fpath, bucket, object_path)
                st.download_button(
                    "Descargar documento",
                    data=b,
                    file_name=fname,
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="emp_dl_btn",
                )
            except Exception:
                st.warning(
                    "El archivo no está disponible (Storage/disco). "
                    "Verifica configuración de Storage o vuelve a cargar el documento."
                )

            confirm_del = st.checkbox(
                "Confirmo que quiero eliminar este documento cargado.",
                key="emp_del_confirm",
            )
            if st.button("Eliminar documento", type="secondary", use_container_width=True, key="emp_del_btn"):
                if not confirm_del:
                    st.error("Debes confirmar la eliminación.")
                    st.stop()
                result = delete_uploaded_document_record("empresa_documentos", int(pick_id))
                if result["shared_refs"]:
                    st.info("El registro fue eliminado de la base de datos. El archivo físico se conservó porque está referenciado en otro registro.")
                elif result["cleanup_issues"]:
                    st.warning("El registro fue eliminado de la base de datos, pero hubo un problema al limpiar el archivo: " + " | ".join(result["cleanup_issues"]))
                st.success(f"Documento eliminado: {result['file_name']}")
                auto_backup_db("doc_empresa_delete")
                st.rerun()


def page_documentos_empresa_faena(
    *,
    fetch_df,
    ui_tip,
    periodo_label,
    periodo_ym,
    get_empresa_monthly_doc_types,
    doc_tipo_join,
    doc_tipo_label,
    render_upload_help,
    prepare_upload_payload,
    safe_name,
    save_file_online,
    sha256_bytes,
    execute,
    datetime,
    auto_backup_db,
    load_file_anywhere,
    delete_uploaded_document_record,
    MESES_ES,
):
    ui_header(
        "Documentos Empresa (Faena)",
        "Carga documentos de empresa POR FAENA, POR MANDANTE y POR MES. Cada período mensual puede tener varios archivos por tipo.",
    )

    faenas = fetch_df(
        """
        SELECT f.id, f.mandante_id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
        """
    )
    if faenas.empty:
        ui_tip("Crea una faena primero.")
        return

    default_id = st.session_state.get("selected_faena_id", None)
    opts = faenas["id"].tolist()
    idx = opts.index(default_id) if default_id in opts else 0

    faena_id = st.selectbox(
        "Faena",
        opts,
        index=idx,
        format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']} ({faenas[faenas['id']==x].iloc[0]['estado']})",
        key="emp_faena_sel",
    )
    st.session_state["selected_faena_id"] = int(faena_id)
    faena_row = faenas[faenas["id"] == faena_id].iloc[0]
    mandante_id = int(faena_row["mandante_id"])
    mandante_nombre = str(faena_row["mandante"])

    hoy = datetime.now()
    anios = sorted({int(hoy.year) - 1, int(hoy.year), int(hoy.year) + 1})
    meses = list(range(1, 13))
    ctop1, ctop2, ctop3 = st.columns([1.3, 1, 2.2])
    with ctop1:
        anio_sel = st.selectbox("Año del período", anios, index=anios.index(int(hoy.year)), key="emp_faena_periodo_anio")
    with ctop2:
        mes_sel = st.selectbox(
            "Mes del período",
            meses,
            index=max(0, min(11, int(hoy.month) - 1)),
            format_func=lambda x: f"{x:02d} · {MESES_ES.get(int(x), str(x))}",
            key="emp_faena_periodo_mes",
        )
    with ctop3:
        st.info(
            f"Mandante seleccionado: **{mandante_nombre}**\n"
            f"Faena: **{faena_row['nombre']}**\n"
            f"Período mensual: **{periodo_label(anio_sel, mes_sel)}**"
        )

    st.caption(
        "Documentación mensual requerida por mandante/faena: Liquidaciones de sueldo, Certificado de antecedentes laborales F30, "
        "Certificado de cumplimientos laborales y previsionales F30-1, y Certificado de accidentabilidad del período."
    )

    docs_periodo = fetch_df(
        "SELECT id, mandante_id, periodo_anio, periodo_mes, doc_tipo, nombre_archivo, file_path, bucket, object_path, created_at FROM faena_empresa_documentos WHERE faena_id=? AND COALESCE(periodo_anio,0)=? AND COALESCE(periodo_mes,0)=? ORDER BY id DESC",
        (int(faena_id), int(anio_sel), int(mes_sel)),
    )
    tipos_presentes = set(docs_periodo["doc_tipo"].astype(str).tolist()) if not docs_periodo.empty else set()
    faltan = [d for d in get_empresa_monthly_doc_types() if d not in tipos_presentes]

    c1, c2, c3 = st.columns([1, 1, 2])
    c1.metric("Requeridos mensuales", len(get_empresa_monthly_doc_types()))
    c2.metric("Tipos cargados en el período", len([d for d in get_empresa_monthly_doc_types() if d in tipos_presentes]))
    c3.metric("Faltan en el período", len(faltan))

    if faltan:
        st.warning("Faltan en este período: " + doc_tipo_join(faltan))
    else:
        st.success("Período mensual completo para esta faena/mandante.")

    tab1, tab2, tab3 = st.tabs(["📎 Cargar documento mensual", "📋 Documentos del período", "🗂️ Historial de la faena"])

    with tab1:
        st.caption("Tipos mensuales requeridos:")
        st.code("\n".join(doc_tipo_label(d) for d in get_empresa_monthly_doc_types()))
        st.caption("Para LIQUIDACIONES_SUELDO_MES puedes subir uno o varios archivos del mismo período.")

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", get_empresa_monthly_doc_types() + ["OTRO"], key="emp_faena_tipo", format_func=lambda x: "OTRO" if x == "OTRO" else doc_tipo_label(x))
        with colx2:
            tipo_otro = st.text_input(
                "Si eliges OTRO, escribe el nombre",
                placeholder="Ej: respaldo adicional mensual, informe complementario, control interno",
                key="emp_faena_otro",
            )

        up = st.file_uploader("Archivo", key="up_doc_emp_faena", type=None)
        render_upload_help()
        if st.button("Guardar documento mensual (empresa por faena)", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()

            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, 'type', None) or 'application/octet-stream')
            folder = [
                "mandantes",
                mandante_id,
                safe_name(mandante_nombre),
                "faenas",
                faena_id,
                safe_name(str(faena_row['nombre'])),
                periodo_ym(anio_sel, mes_sel),
                "empresa_mensual",
                safe_name(doc_tipo),
            ]
            file_path, bucket, object_path = save_file_online(folder, payload["file_name"], payload["file_bytes"], content_type=payload["content_type"])
            sha = sha256_bytes(payload["file_bytes"])

            execute(
                "INSERT INTO faena_empresa_documentos(faena_id, mandante_id, periodo_anio, periodo_mes, doc_tipo, nombre_archivo, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (int(faena_id), int(mandante_id), int(anio_sel), int(mes_sel), doc_tipo, payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])
            st.success(f"Documento guardado para {mandante_nombre} / {faena_row['nombre']} / {periodo_label(anio_sel, mes_sel)}.")
            auto_backup_db("doc_empresa_faena_mensual")
            st.rerun()

    with tab2:
        if docs_periodo.empty:
            st.info("(sin documentos cargados para este período)")
        else:
            show = docs_periodo[["doc_tipo", "nombre_archivo", "created_at"]].copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### 🔎 Gestionar documento del período")
            pick_id = st.selectbox(
                "Documento",
                docs_periodo["id"].tolist(),
                format_func=lambda x: f"{docs_periodo[docs_periodo['id']==x].iloc[0]['doc_tipo']} — {docs_periodo[docs_periodo['id']==x].iloc[0]['nombre_archivo']}",
                key="empf_pick_doc",
            )
            row = docs_periodo[docs_periodo["id"] == pick_id].iloc[0]
            fpath = row.get("file_path", "")
            bucket = row.get("bucket", None)
            object_path = row.get("object_path", None)
            fname = row.get("nombre_archivo", "documento")
            try:
                b = load_file_anywhere(fpath, bucket, object_path)
                st.download_button(
                    "Descargar documento",
                    data=b,
                    file_name=fname,
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="empf_dl_btn",
                )
            except Exception:
                st.warning(
                    "El archivo no está disponible (Storage/disco). "
                    "Verifica configuración de Storage o vuelve a cargar el documento."
                )

            confirm_del = st.checkbox(
                "Confirmo que quiero eliminar este documento cargado.",
                key="empf_del_confirm",
            )
            if st.button("Eliminar documento", type="secondary", use_container_width=True, key="empf_del_btn"):
                if not confirm_del:
                    st.error("Debes confirmar la eliminación.")
                    st.stop()
                result = delete_uploaded_document_record("faena_empresa_documentos", int(pick_id))
                if result["shared_refs"]:
                    st.info("El registro fue eliminado de la base de datos. El archivo físico se conservó porque está referenciado en otro registro.")
                elif result["cleanup_issues"]:
                    st.warning("El registro fue eliminado de la base de datos, pero hubo un problema al limpiar el archivo: " + " | ".join(result["cleanup_issues"]))
                st.success(f"Documento eliminado: {result['file_name']}")
                auto_backup_db("doc_empresa_faena_delete")
                st.rerun()

    with tab3:
        historial = fetch_df(
            "SELECT id, periodo_anio, periodo_mes, doc_tipo, nombre_archivo, created_at FROM faena_empresa_documentos WHERE faena_id=? ORDER BY COALESCE(periodo_anio,0) DESC, COALESCE(periodo_mes,0) DESC, id DESC",
            (int(faena_id),),
        )
        if historial.empty:
            st.info("(sin historial de documentos empresa por faena)")
        else:
            historial = historial.copy()
            historial["periodo"] = historial.apply(lambda r: periodo_label(r.get("periodo_anio"), r.get("periodo_mes")), axis=1)
            st.dataframe(historial[["periodo", "doc_tipo", "nombre_archivo", "created_at"]], use_container_width=True, hide_index=True)
