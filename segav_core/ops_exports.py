from __future__ import annotations

def page_export_zip(
    *,
    st,
    ui_header,
    ui_tip,
    fetch_df,
    pendientes_obligatorios,
    pendientes_empresa_faena,
    doc_tipo_join,
    export_zip_for_faena,
    persist_export,
    auto_backup_db,
    load_file_anywhere,
    human_file_size,
    export_zip_for_mes,
    persist_export_mes,
    os,
    date,
):
    ui_header("Export (ZIP)", "Genera carpeta por faena con documentos de trabajadores y deja historial.")

    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
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
    )
    st.session_state["selected_faena_id"] = int(faena_id)

    tab1, tab2, tab3, tab4 = st.tabs(["✅ Pendientes", "📦 Generar ZIP", "🗂️ Historial", "📅 Export por mes"])

    with tab1:
        pend = pendientes_obligatorios(int(faena_id))
        miss_emp = pendientes_empresa_faena(int(faena_id))
        st.write("**Pendientes obligatorios (antes de exportar):**")
        if not pend:
            st.info("(sin trabajadores asignados)")
        else:
            for k, missing in pend.items():
                if missing:
                    st.error(f"{k} — faltan: {doc_tipo_join(missing)}")
                else:
                    st.success(f"{k} — OK")

        st.divider()
        st.write("**Documentos empresa (por faena):**")
        if miss_emp:
            st.error("Faltan: " + ", ".join(miss_emp))
        else:
            st.success("OK (requeridos completos).")

    with tab2:
        st.markdown("### 📦 Selecciona qué incluir en el ZIP")

        cA, cB, cC = st.columns(3)
        with cA:
            inc_contrato = st.checkbox("Contrato de faena", value=True, key="exp_inc_contrato")
            inc_anexos = st.checkbox("Anexos de faena", value=True, key="exp_inc_anexos")
        with cB:
            inc_emp_faena = st.checkbox("Docs empresa (por faena)", value=True, key="exp_inc_emp_faena")
            inc_emp_global = st.checkbox("Docs empresa (global)", value=True, key="exp_inc_emp_global")
        with cC:
            inc_trab = st.checkbox("Docs trabajadores", value=True, key="exp_inc_trab")

        st.divider()
        st.markdown("#### (Opcional) Filtrar por tipo de documento")

        emp_global_types = fetch_df("SELECT DISTINCT doc_tipo FROM empresa_documentos ORDER BY doc_tipo")
        emp_global_list = emp_global_types["doc_tipo"].dropna().astype(str).tolist() if not emp_global_types.empty else []

        emp_faena_types = fetch_df("SELECT DISTINCT doc_tipo FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo", (int(faena_id),))
        emp_faena_list = emp_faena_types["doc_tipo"].dropna().astype(str).tolist() if not emp_faena_types.empty else []

        trab_types = fetch_df('''
            SELECT DISTINCT td.doc_tipo AS doc_tipo
            FROM trabajador_documentos td
            JOIN asignaciones a ON a.trabajador_id = td.trabajador_id
            WHERE a.faena_id=?
              AND COALESCE(NULLIF(TRIM(a.estado), ''), 'ACTIVA')='ACTIVA'
            ORDER BY td.doc_tipo
        ''', (int(faena_id),))
        trab_list = trab_types["doc_tipo"].dropna().astype(str).tolist() if not trab_types.empty else []

        colf1, colf2, colf3 = st.columns(3)
        with colf1:
            emp_global_sel = []
            if inc_emp_global and emp_global_list:
                emp_global_sel = st.multiselect("Tipos Empresa Global", emp_global_list, default=emp_global_list, key="exp_types_emp_global")
            elif inc_emp_global and not emp_global_list:
                st.caption("Sin docs empresa global cargados.")
        with colf2:
            emp_faena_sel = []
            if inc_emp_faena and emp_faena_list:
                emp_faena_sel = st.multiselect("Tipos Empresa por Faena", emp_faena_list, default=emp_faena_list, key="exp_types_emp_faena")
            elif inc_emp_faena and not emp_faena_list:
                st.caption("Sin docs empresa por faena cargados.")
        with colf3:
            trab_sel = []
            if inc_trab and trab_list:
                trab_sel = st.multiselect("Tipos Trabajador", trab_list, default=trab_list, key="exp_types_trab")
            elif inc_trab and not trab_list:
                st.caption("Sin docs trabajador cargados para esta faena.")

        st.divider()
        st.markdown("#### 🎯 Selección específica de documentos (opcional)")
        st.caption("Si no activas una selección específica, el ZIP incluirá todos los documentos que cumplan los filtros anteriores.")

        emp_faena_doc_sel_ids = None
        selected_trab_ids = None
        selected_trab_doc_map = None

        if inc_emp_faena:
            emp_docs = fetch_df(
                "SELECT id, doc_tipo, nombre_archivo, file_path, object_path FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo, nombre_archivo, id",
                (int(faena_id),),
            )
            use_specific_emp_docs = st.checkbox(
                "Elegir documentos específicos de empresa para esta faena",
                value=False,
                key="exp_use_specific_emp_docs",
            )
            if use_specific_emp_docs:
                if emp_docs.empty:
                    st.caption("No hay documentos empresa por faena cargados.")
                    emp_faena_doc_sel_ids = []
                else:
                    emp_doc_labels = {}
                    for _, row in emp_docs.iterrows():
                        did = int(row["id"])
                        nombre = str(row.get("nombre_archivo") or row.get("file_path") or row.get("object_path") or f"documento_{did}")
                        nombre = os.path.basename(nombre)
                        venc = ""
                        emp_doc_labels[did] = f"{did} · {row.get('doc_tipo', '-')} · {nombre}{venc}"
                    emp_ids = list(emp_doc_labels.keys())
                    emp_faena_doc_sel_ids = st.multiselect(
                        "Documentos empresa por faena a exportar",
                        emp_ids,
                        default=emp_ids,
                        format_func=lambda x, labels=emp_doc_labels: labels.get(int(x), str(x)),
                        key="exp_emp_faena_doc_ids",
                    )
                    if not emp_faena_doc_sel_ids:
                        st.warning("No hay documentos empresa por faena seleccionados; esa carpeta quedará vacía en el ZIP.")

        if inc_trab:
            asign_docs = fetch_df('''
                SELECT t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
                FROM asignaciones a
                JOIN trabajadores t ON t.id=a.trabajador_id
                WHERE a.faena_id=?
                  AND COALESCE(NULLIF(TRIM(a.estado), ''), 'ACTIVA')='ACTIVA'
                ORDER BY t.apellidos, t.nombres
            ''', (int(faena_id),))
            use_specific_workers = st.checkbox(
                "Elegir trabajadores específicos y sus documentos",
                value=False,
                key="exp_use_specific_workers",
            )
            if use_specific_workers:
                if asign_docs.empty:
                    st.caption("No hay trabajadores asignados a esta faena.")
                    selected_trab_ids = []
                    selected_trab_doc_map = {}
                else:
                    worker_labels = {}
                    for _, row in asign_docs.iterrows():
                        tid = int(row["trabajador_id"])
                        worker_labels[tid] = f"{row['apellidos']}, {row['nombres']} · {row['rut']}"
                    worker_ids = list(worker_labels.keys())
                    selected_trab_ids = st.multiselect(
                        "Trabajadores a incluir en el ZIP",
                        worker_ids,
                        default=worker_ids,
                        format_func=lambda x, labels=worker_labels: labels.get(int(x), str(x)),
                        key="exp_selected_trab_ids",
                    )
                    selected_trab_doc_map = {}
                    for tid in selected_trab_ids:
                        docs_worker = fetch_df(
                            "SELECT id, doc_tipo, nombre_archivo, file_path, object_path FROM trabajador_documentos WHERE trabajador_id=? ORDER BY doc_tipo, nombre_archivo, id",
                            (int(tid),),
                        )
                        with st.expander(f"Documentos de {worker_labels.get(int(tid), tid)}", expanded=False):
                            if docs_worker.empty:
                                st.caption("Este trabajador no tiene documentos cargados.")
                                selected_trab_doc_map[int(tid)] = []
                            else:
                                doc_labels = {}
                                for _, row in docs_worker.iterrows():
                                    did = int(row["id"])
                                    nombre = str(row.get("nombre_archivo") or row.get("file_path") or row.get("object_path") or f"documento_{did}")
                                    nombre = os.path.basename(nombre)
                                    venc = ""
                                    doc_labels[did] = f"{did} · {row.get('doc_tipo', '-')} · {nombre}{venc}"
                                doc_ids = list(doc_labels.keys())
                                selected_trab_doc_map[int(tid)] = st.multiselect(
                                    "Documentos a exportar",
                                    doc_ids,
                                    default=doc_ids,
                                    format_func=lambda x, labels=doc_labels: labels.get(int(x), str(x)),
                                    key=f"exp_trab_doc_ids_{int(faena_id)}_{int(tid)}",
                                )
                                if not selected_trab_doc_map[int(tid)]:
                                    st.warning("No hay documentos seleccionados para este trabajador; no se exportarán archivos de este trabajador.")

        st.divider()
        colx1, colx2 = st.columns([1, 1])
        with colx1:
            if st.button("Generar ZIP y guardar en historial", type="primary", use_container_width=True):
                try:
                    zip_bytes, name = export_zip_for_faena(
                        int(faena_id),
                        include_global_empresa_docs=inc_emp_global,
                        include_contrato=inc_contrato,
                        include_anexos=inc_anexos,
                        include_empresa_faena=inc_emp_faena,
                        include_trabajadores=inc_trab,
                        doc_types_empresa_global=(emp_global_sel or None),
                        doc_types_empresa_faena=(emp_faena_sel or None),
                        doc_types_trabajador=(trab_sel or None),
                        selected_empresa_faena_doc_ids=emp_faena_doc_sel_ids,
                        selected_trabajador_ids=selected_trab_ids,
                        selected_trabajador_doc_ids=selected_trab_doc_map,
                    )
                    path = persist_export(int(faena_id), zip_bytes, name)
                    st.success(f"ZIP generado y guardado: {os.path.basename(path)}")
                    auto_backup_db("export_zip")
                    st.download_button(
                        "Descargar ZIP (recién generado)",
                        data=zip_bytes,
                        file_name=os.path.basename(path),
                        mime="application/zip",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.error(f"No se pudo generar ZIP: {e}")
        with colx2:
            st.caption("Para conservar historial entre reboots, usa Backup / Restore.")

    with tab3:
        hist = fetch_df(
            """
            SELECT eh.id, eh.faena_id, f.nombre AS faena_nombre, eh.file_path, eh.bucket, eh.object_path,
                   eh.size_bytes, eh.created_at
            FROM export_historial eh
            LEFT JOIN faenas f ON f.id = eh.faena_id
            ORDER BY eh.id DESC
            """
        )
        if hist.empty:
            st.info("Aún no hay ZIPs exportados.")
        else:
            view = hist.copy()
            view["archivo"] = view.apply(
                lambda r: os.path.basename(str(r.get("file_path") or r.get("object_path") or f"export_{int(r['id'])}.zip")),
                axis=1,
            )
            view["tamaño"] = view["size_bytes"].apply(human_file_size)
            show_cols = ["id", "faena_id", "faena_nombre", "archivo", "tamaño", "created_at"]
            st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

            hid = st.selectbox(
                "ZIP del historial",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} - {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['created_at']})",
                key="exp_hist_pick",
            )
            row = view[view["id"] == hid].iloc[0]
            try:
                b = load_file_anywhere(row.get("file_path"), row.get("bucket"), row.get("object_path"))
                st.download_button(
                    "Descargar ZIP del historial",
                    data=b,
                    file_name=row["archivo"],
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_hist_dl",
                )
            except Exception as e:
                st.warning(f"No se pudo abrir el ZIP guardado: {e}")

    with tab4:
        st.markdown("### 📅 Export por mes")
        c1, c2 = st.columns(2)
        with c1:
            year = st.number_input("Año", min_value=2020, max_value=2100, value=date.today().year, step=1, key="exp_mes_year")
        with c2:
            month = st.number_input("Mes", min_value=1, max_value=12, value=date.today().month, step=1, key="exp_mes_month")

        inc_mes_emp_global = st.checkbox(
            "Incluir documentos empresa global en export mensual",
            value=True,
            key="exp_mes_inc_emp_global",
        )

        if st.button("Generar ZIP mensual y guardar en historial", type="primary", use_container_width=True, key="exp_mes_btn"):
            try:
                zip_bytes, ym = export_zip_for_mes(int(year), int(month), include_global_empresa_docs=inc_mes_emp_global)
                path_export = persist_export_mes(ym, zip_bytes)
                st.success(f"ZIP mensual generado y guardado: {os.path.basename(path_export)}")
                auto_backup_db("export_zip_mes")
                st.download_button(
                    "Descargar ZIP mensual (recién generado)",
                    data=zip_bytes,
                    file_name=os.path.basename(path_export),
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_mes_dl_now",
                )
            except Exception as e:
                st.error(f"No se pudo generar export mensual: {e}")

        st.divider()
        hist_mes = fetch_df(
            """
            SELECT id, year_month, file_path, bucket, object_path, size_bytes, created_at
            FROM export_historial_mes
            ORDER BY id DESC
            """
        )
        if hist_mes.empty:
            st.caption("Aún no hay exportaciones mensuales guardadas.")
        else:
            view = hist_mes.copy()
            view["archivo"] = view.apply(
                lambda r: os.path.basename(str(r.get("file_path") or r.get("object_path") or f"mes_{r.get('year_month','export')}.zip")),
                axis=1,
            )
            view["tamaño"] = view["size_bytes"].apply(human_file_size)
            st.dataframe(view[["id", "year_month", "archivo", "tamaño", "created_at"]], use_container_width=True, hide_index=True)

            mid = st.selectbox(
                "ZIP mensual del historial",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} - {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['year_month']})",
                key="exp_mes_hist_pick",
            )
            row = view[view["id"] == mid].iloc[0]
            try:
                b = load_file_anywhere(row.get("file_path"), row.get("bucket"), row.get("object_path"))
                st.download_button(
                    "Descargar ZIP mensual del historial",
                    data=b,
                    file_name=row["archivo"],
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_mes_hist_dl",
                )
            except Exception as e:
                st.warning(f"No se pudo abrir el ZIP mensual guardado: {e}")


def page_backup_restore(
    *,
    st,
    ui_header,
    DB_BACKEND,
    DB_PATH,
    STORAGE_BUCKET,
    datetime,
    fetch_df,
    init_db,
    os,
    restore_from_backup_zip,
    storage_admin_enabled,
    storage_enabled,
    storage_upload,
):
    ui_header("Backup / Restore", "Diagnostica el backend activo y gestiona respaldos locales o heredados sin confundirlos con la persistencia real online.")
    st.warning(
        "En Streamlit Community Cloud, los archivos locales (incluyendo SQLite y uploads) pueden perderse en reboots/redeploy. "
        "Si trabajas con Supabase/Postgres, la fuente de verdad está online y este módulo sirve sobre todo para diagnóstico y compatibilidad local heredada."
    )
    if DB_BACKEND == "postgres":
        st.info(
            "Modo actual: **Postgres/Supabase**. La base online es la fuente de verdad; por eso las opciones sobre **app.db** quedan solo como compatibilidad local heredada. "
            "Usa principalmente el diagnóstico de Storage y las exportaciones/documentos online."
        )

    tab1, tab2, tab3 = st.tabs(["🧪 Diagnóstico backend", "🗄️ Base local heredada (app.db)", "📦 Backup completo (ZIP)"])

    with tab1:
        cdiag1, cdiag2, cdiag3 = st.columns(3)
        cdiag1.metric("Backend activo", DB_BACKEND.upper())
        cdiag2.metric("Storage lectura", "Sí" if storage_enabled() else "No")
        cdiag3.metric("Storage admin", "Sí" if storage_admin_enabled() else "No")
        if DB_BACKEND == "postgres":
            st.info("Modo Postgres/Supabase activo. La persistencia real vive online. Los auto-backups/app.db de abajo se mantienen como compatibilidad local heredada.")
        else:
            st.info("Modo SQLite local activo. En este modo app.db sí es la fuente principal de datos.")
        if storage_enabled() and not storage_admin_enabled():
            st.warning("Storage está solo en modo lectura o con key débil. Para subir/eliminar archivos usa una secret/service key real en SUPABASE_SERVICE_ROLE_KEY.")
        st.caption("Auto-backups generados al guardar (solo app.db). Se guardan localmente y conviene descargarlos si sigues usando SQLite local.")
        hist = fetch_df("SELECT id, tag, file_path, size_bytes, created_at FROM auto_backup_historial ORDER BY id DESC")
        if hist.empty:
            st.info("(aún no hay auto-backups)")
        else:
            view = hist.copy()
            view["archivo"] = view["file_path"].apply(lambda p: os.path.basename(p))
            view["size_kb"] = (view["size_bytes"] / 1024).round(1)
            st.dataframe(view[["id", "tag", "archivo", "size_kb", "created_at"]], use_container_width=True, hide_index=True)

            sel = st.selectbox(
                "Elegir auto-backup para descargar",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} - {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['tag']})",
            )
            row = view[view["id"] == sel].iloc[0]
            p = row["file_path"]
            if os.path.exists(p):
                with open(p, "rb") as f:
                    b = f.read()
                st.download_button("Descargar auto-backup (app.db)", data=b, file_name=os.path.basename(p), mime="application/octet-stream", use_container_width=True)
            else:
                st.warning("El archivo no está en disco (posible reboot/redeploy).")

    with tab2:
        if DB_BACKEND == "postgres":
            st.info("Esta pestaña aplica solo a respaldo/restauración de **SQLite local (app.db)**. En Supabase la persistencia real vive en Postgres; úsala solo como compatibilidad o diagnóstico local.")
        coldb1, coldb2 = st.columns([1, 1])

        with coldb1:
            st.markdown("### Descargar app.db")
            if os.path.exists(DB_PATH):
                with open(DB_PATH, "rb") as f:
                    db_bytes = f.read()
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                st.download_button("Descargar app.db", data=db_bytes, file_name=f"app_{ts}.db", mime="application/octet-stream", use_container_width=True)
            else:
                st.info("Aún no existe app.db (no hay datos o no se ha inicializado).")

        with coldb2:
            st.markdown("### Restaurar app.db")
            up_db = st.file_uploader("Sube un archivo .db", type=["db", "sqlite", "sqlite3"], key="up_db_only")
            if st.button("Restaurar app.db", type="primary", use_container_width=True):
                if up_db is None:

                    st.error("Debes subir un archivo .db primero.")

                    st.stop()
                try:
                    with open(DB_PATH, "wb") as f:
                        f.write(up_db.getvalue())
                    init_db()
                    st.success("Base restaurada. La app se reiniciará.")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo restaurar app.db: {e}")

    with tab3:

        st.divider()
        st.markdown("### 🧪 Diagnóstico Storage (solo admin)")
        if not storage_enabled():
            st.info("Storage no está activo. Revisa Secrets: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY y SUPABASE_STORAGE_BUCKET. (SUPABASE_ANON_KEY es solo opcional para lectura)")
        else:
            st.success(f"Storage activo: bucket **{STORAGE_BUCKET}** · admin={'Sí' if storage_admin_enabled() else 'No'}")
            last = st.session_state.get("storage_last_error")
            if last:
                st.warning(f"Último error Storage: HTTP {last.get('status')} · {str(last.get('body',''))[:120]}")
                with st.expander("Ver detalle último error"):
                    st.write(last)
            if st.button("Probar subida Storage (archivo de prueba)", use_container_width=True):
                try:
                    test_path = f"_diagnostico/test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
                    storage_upload(test_path, b"ok", content_type="text/plain", upsert=True)
                    st.success(f"Subida OK: {test_path}")
                except Exception as e:
                    st.error(f"Falló prueba: {e}")
        st.markdown("### 2) Restaurar Backup completo")
        up = st.file_uploader("Sube backup ZIP", type=["zip"], key="up_backup_zip")
        if st.button("Restaurar ahora", type="primary", use_container_width=True):
            if up is None:

                st.error("Debes subir un backup ZIP primero.")

                st.stop()
            try:
                restore_from_backup_zip(up.getvalue())
                st.success("Backup restaurado. La app se reiniciará.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo restaurar: {e}")
