from __future__ import annotations

import pandas as pd

from segav_core.ui import doc_availability_label

def _legal_export_blockers(fetch_df, faena_id: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    latest = fetch_df(
        """
        SELECT id, entity_table, entity_id, doc_tipo, nombre_archivo, criticality, legal_status, renewal_status, expires_at
          FROM legal_doc_approvals
         ORDER BY entity_table, entity_id, version_no DESC, id DESC
        """
    )
    if latest is None or latest.empty:
        return pd.DataFrame(), pd.DataFrame()
    work = latest.copy()
    for c in ["criticality", "legal_status", "renewal_status", "entity_table"]:
        if c in work.columns:
            work[c] = work[c].fillna('').astype(str).str.upper().str.strip()
    work = work.drop_duplicates(subset=["entity_table", "entity_id"], keep="first")
    crit = work[work["criticality"].isin(["ALTA", "CRITICA"])].copy()
    if crit.empty:
        return pd.DataFrame(), pd.DataFrame()
    docs_faena = fetch_df("SELECT id FROM faena_empresa_documentos WHERE faena_id=?", (int(faena_id),))
    ids_emp = {int(v) for v in docs_faena["id"].tolist()} if docs_faena is not None and not docs_faena.empty else set()
    docs_trab = fetch_df(
        """
        SELECT td.id
          FROM trabajador_documentos td
          JOIN asignaciones a ON a.trabajador_id = td.trabajador_id
         WHERE a.faena_id=?
           AND COALESCE(NULLIF(TRIM(a.estado), ''), 'ACTIVA')='ACTIVA'
        """,
        (int(faena_id),),
    )
    ids_trab = {int(v) for v in docs_trab["id"].tolist()} if docs_trab is not None and not docs_trab.empty else set()
    rel = crit[((crit["entity_table"] == "FAENA_EMPRESA_DOCUMENTOS") & crit["entity_id"].isin(ids_emp)) | ((crit["entity_table"] == "TRABAJADOR_DOCUMENTOS") & crit["entity_id"].isin(ids_trab))].copy()
    if rel.empty:
        return pd.DataFrame(), pd.DataFrame()
    blockers = rel[rel["renewal_status"].eq("VENCIDO")].copy()
    warnings = rel[rel["renewal_status"].isin(["POR_VENCER"]) | rel["legal_status"].ne("APROBADO")].copy()
    cols = [c for c in ["doc_tipo", "nombre_archivo", "entity_table", "entity_id", "legal_status", "renewal_status", "expires_at"] if c in rel.columns]
    return blockers[cols].reset_index(drop=True), warnings[cols].reset_index(drop=True)


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
    current_tenant_key,
    current_segav_client_key,
    visible_clientes_df,
    allowed_mandante_ids=None,
    execute=None,
    is_superadmin=None,
    audit_log=None,
):
    ui_header("Exportar (ZIP)", "Genera carpeta por faena con documentos de trabajadores y deja historial.")
    tenant_key = str(current_tenant_key() or current_segav_client_key() or '').strip()
    tenant_name = tenant_key
    try:
        _vdf = visible_clientes_df()
        if _vdf is not None and not _vdf.empty:
            _row = _vdf[_vdf["cliente_key"].astype(str) == tenant_key]
            if not _row.empty:
                tenant_name = str(_row.iloc[0].get("cliente_nombre") or tenant_key)
    except Exception:
        pass
    st.caption(f"Empresa activa para esta exportación: {tenant_name} ({tenant_key})")

    _scope_restricted = allowed_mandante_ids is not None
    _allowed_mands = [int(x) for x in (allowed_mandante_ids or [])]
    if _scope_restricted and not _allowed_mands:
        st.info("Tu usuario lector no tiene mandantes asignados. No hay faenas disponibles para exportar.")
        return
    if _scope_restricted:
        _ph = ','.join(['?'] * len(_allowed_mands))
        faenas = fetch_df(f'''
            SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
            FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
            WHERE f.mandante_id IN ({_ph})
            ORDER BY f.id DESC
        ''', tuple(_allowed_mands))
    else:
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["✅ Pendientes", "📦 Generar ZIP", "🗂️ Historial", "📅 Exportar por mes", "📄 Reporte Cumplimiento"])

    legal_blockers, legal_warnings = _legal_export_blockers(fetch_df, int(faena_id))

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

        st.divider()
        st.write("**Control legal crítico:**")
        if legal_blockers is not None and not legal_blockers.empty:
            st.error("Hay documentos críticos vencidos. La exportación quedará bloqueada hasta renovarlos.")
            st.dataframe(legal_blockers, use_container_width=True, hide_index=True)
        else:
            st.success("No hay bloqueos legales críticos por vencimiento para esta faena.")
        if legal_warnings is not None and not legal_warnings.empty:
            st.warning("Hay documentos críticos por vencer o pendientes de aprobación.")
            st.dataframe(legal_warnings.head(10), use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("### 📦 Selecciona los documentos para el ZIP")
        st.caption("Expande cada sección, revisa los documentos disponibles y desmarca los que no quieras incluir.")

        # ── Collect all documents by category ──────────────────────────────
        _total_selected = 0

        # 1. Contrato de faena
        contrato_row = fetch_df(
            "SELECT id, nombre, file_path, bucket, object_path FROM contratos_faena WHERE id=(SELECT contrato_faena_id FROM faenas WHERE id=?)",
            (int(faena_id),),
        )
        _has_contrato = contrato_row is not None and not contrato_row.empty and (contrato_row.iloc[0].get("file_path") or contrato_row.iloc[0].get("object_path"))
        inc_contrato = False
        if _has_contrato:
            with st.expander("📑 Contrato de faena (1 archivo)", expanded=False):
                _cname = os.path.basename(str(contrato_row.iloc[0].get("file_path") or contrato_row.iloc[0].get("object_path") or "contrato"))
                inc_contrato = st.checkbox(f"Incluir: {_cname}", value=True, key="exp2_contrato")
                if inc_contrato:
                    _total_selected += 1

        # 2. Anexos de faena
        anexos_df = fetch_df("SELECT id, nombre, file_path, object_path FROM faena_anexos WHERE faena_id=? ORDER BY id", (int(faena_id),))
        _has_anexos = anexos_df is not None and not anexos_df.empty
        inc_anexos = False
        sel_anexo_ids = []
        if _has_anexos:
            _n_anexos = len(anexos_df)
            with st.expander(f"📎 Anexos de faena ({_n_anexos} archivo{'s' if _n_anexos != 1 else ''})", expanded=False):
                anexo_labels = {}
                for _, r in anexos_df.iterrows():
                    aid = int(r["id"])
                    aname = os.path.basename(str(r.get("nombre") or r.get("file_path") or r.get("object_path") or f"anexo_{aid}"))
                    anexo_labels[aid] = aname
                a_ids = list(anexo_labels.keys())
                sel_anexo_ids = st.multiselect(
                    "Anexos a incluir",
                    a_ids,
                    default=a_ids,
                    format_func=lambda x, lb=anexo_labels: lb.get(int(x), str(x)),
                    key="exp2_anexos_sel",
                )
                inc_anexos = len(sel_anexo_ids) > 0
                _total_selected += len(sel_anexo_ids)

        # 3. Documentos empresa global
        if _scope_restricted:
            _ph = ','.join(['?'] * len(_allowed_mands))
            emp_global_df = fetch_df(
                f"SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path FROM empresa_documentos WHERE COALESCE(mandante_id,0)=0 OR mandante_id IN ({_ph}) ORDER BY doc_tipo, nombre_archivo, id",
                tuple(_allowed_mands),
            )
        else:
            emp_global_df = fetch_df("SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path FROM empresa_documentos ORDER BY doc_tipo, nombre_archivo, id")
        _has_emp_global = emp_global_df is not None and not emp_global_df.empty
        sel_emp_global_ids = []
        if _has_emp_global:
            _n_eg = len(emp_global_df)
            with st.expander(f"🏢 Documentos empresa global ({_n_eg} archivo{'s' if _n_eg != 1 else ''})", expanded=False):
                eg_labels = {}
                for _, r in emp_global_df.iterrows():
                    did = int(r["id"])
                    nombre = os.path.basename(str(r.get("nombre_archivo") or r.get("file_path") or r.get("object_path") or f"doc_{did}"))
                    _avail = "✅" if (r.get("bucket") and r.get("object_path")) else "💾"
                    eg_labels[did] = f"{_avail} {r.get('doc_tipo', '-')} · {nombre}"
                eg_ids = list(eg_labels.keys())
                sel_emp_global_ids = st.multiselect(
                    "Documentos empresa global a incluir",
                    eg_ids,
                    default=eg_ids,
                    format_func=lambda x, lb=eg_labels: lb.get(int(x), str(x)),
                    key="exp2_emp_global_sel",
                )
                _total_selected += len(sel_emp_global_ids)

        # 4. Documentos empresa por faena
        emp_faena_df = fetch_df(
            "SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo, nombre_archivo, id",
            (int(faena_id),),
        )
        _has_emp_faena = emp_faena_df is not None and not emp_faena_df.empty
        emp_faena_doc_sel_ids = None
        if _has_emp_faena:
            _n_ef = len(emp_faena_df)
            with st.expander(f"🏭 Documentos empresa por faena ({_n_ef} archivo{'s' if _n_ef != 1 else ''})", expanded=False):
                ef_labels = {}
                for _, r in emp_faena_df.iterrows():
                    did = int(r["id"])
                    nombre = os.path.basename(str(r.get("nombre_archivo") or r.get("file_path") or r.get("object_path") or f"doc_{did}"))
                    _avail = "✅" if (r.get("bucket") and r.get("object_path")) else "💾"
                    ef_labels[did] = f"{_avail} {r.get('doc_tipo', '-')} · {nombre}"
                ef_ids = list(ef_labels.keys())
                emp_faena_doc_sel_ids = st.multiselect(
                    "Documentos empresa (faena) a incluir",
                    ef_ids,
                    default=ef_ids,
                    format_func=lambda x, lb=ef_labels: lb.get(int(x), str(x)),
                    key="exp2_emp_faena_sel",
                )
                _total_selected += len(emp_faena_doc_sel_ids)

        # 5. Documentos trabajadores
        asign_df = fetch_df('''
            SELECT t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
            FROM asignaciones a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.faena_id=?
              AND COALESCE(NULLIF(TRIM(a.estado), ''), 'ACTIVA')='ACTIVA'
            ORDER BY t.apellidos, t.nombres
        ''', (int(faena_id),))
        _has_workers = asign_df is not None and not asign_df.empty
        selected_trab_ids = None
        selected_trab_doc_map = None
        if _has_workers:
            _n_workers = len(asign_df)
            # Count total worker docs (use uncached to ensure fresh data)
            _worker_doc_count = 0
            _worker_doc_cache = {}
            for _, wr in asign_df.iterrows():
                _wid = int(wr["trabajador_id"])
                _wdocs = fetch_df(
                    "SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path FROM trabajador_documentos WHERE trabajador_id=? ORDER BY doc_tipo, nombre_archivo, id",
                    (_wid,),
                )
                _worker_doc_cache[_wid] = _wdocs
                if _wdocs is not None and not _wdocs.empty:
                    _worker_doc_count += len(_wdocs)

            with st.expander(f"👷 Documentos de trabajadores ({_n_workers} trabajadores · {_worker_doc_count} archivos)", expanded=False):
                st.markdown("##### 👷 Trabajadores en esta faena")
                # Worker selection
                worker_labels = {}
                for _, wr in asign_df.iterrows():
                    _wid = int(wr["trabajador_id"])
                    _wdocs = _worker_doc_cache.get(_wid)
                    _wdoc_n = len(_wdocs) if _wdocs is not None and not _wdocs.empty else 0
                    worker_labels[_wid] = f"{wr['apellidos']}, {wr['nombres']} · {wr['rut']} ({_wdoc_n} docs)"
                worker_ids = list(worker_labels.keys())
                selected_trab_ids = st.multiselect(
                    "Selecciona los trabajadores que deseas incluir en el ZIP",
                    worker_ids,
                    default=worker_ids,
                    format_func=lambda x, lb=worker_labels: lb.get(int(x), str(x)),
                    key="exp2_trab_sel",
                    label_visibility="collapsed",
                )

                st.caption(f"{len(selected_trab_ids)} de {_n_workers} trabajadores seleccionados")

                # Per-worker document selection
                selected_trab_doc_map = {}
                _total_worker_docs_selected = 0
                for tid in selected_trab_ids:
                    docs_worker = _worker_doc_cache.get(int(tid))
                    wlabel = worker_labels.get(int(tid), str(tid))
                    if docs_worker is None or docs_worker.empty:
                        st.caption(f"⚠️ {wlabel}: sin documentos cargados")
                        selected_trab_doc_map[int(tid)] = []
                        continue
                    doc_labels = {}
                    for _, dr in docs_worker.iterrows():
                        did = int(dr["id"])
                        nombre = os.path.basename(str(dr.get("nombre_archivo") or dr.get("file_path") or dr.get("object_path") or f"doc_{did}"))
                        _avail = "✅" if (dr.get("bucket") and dr.get("object_path")) else "💾"
                        doc_labels[did] = f"{_avail} {dr.get('doc_tipo', '-')} · {nombre}"
                    doc_ids = list(doc_labels.keys())
                    selected_trab_doc_map[int(tid)] = st.multiselect(
                        f"📄 {wlabel}",
                        doc_ids,
                        default=doc_ids,
                        format_func=lambda x, lb=doc_labels: lb.get(int(x), str(x)),
                        key=f"exp2_tdocs_{int(faena_id)}_{int(tid)}",
                    )
                    _total_worker_docs_selected += len(selected_trab_doc_map[int(tid)])
                    _total_selected += len(selected_trab_doc_map[int(tid)])

                st.caption(f"📊 {_total_worker_docs_selected} documentos de trabajadores seleccionados")

        # ── Summary + Generate ─────────────────────────────────────────────
        st.divider()

        # Show what's NOT available
        _missing = []
        if not _has_contrato:
            _missing.append("contrato")
        if not _has_anexos:
            _missing.append("anexos")
        if not _has_emp_global:
            _missing.append("docs empresa global")
        if not _has_emp_faena:
            _missing.append("docs empresa por faena")
        if not _has_workers:
            _missing.append("trabajadores asignados")
        if _missing:
            st.caption(f"No disponibles para esta faena: {', '.join(_missing)}")

        st.markdown(f"**Total documentos seleccionados: {_total_selected}**")

        if _total_selected == 0:
            st.info("Selecciona al menos un documento para generar el ZIP.")

        col_gen, col_info = st.columns([2, 1])
        with col_gen:
            _blocked = legal_blockers is not None and not legal_blockers.empty
            _disabled = _blocked or _total_selected == 0
            if _blocked:
                st.button("🚫 Generar ZIP", type="primary", use_container_width=True, disabled=True)
                st.caption("Exportación bloqueada por documentos críticos vencidos.")
            elif st.button("📦 Generar ZIP y guardar en historial", type="primary", use_container_width=True, disabled=_disabled):
                try:
                    # Build selection flags from user choices
                    _inc_emp_global = len(sel_emp_global_ids) > 0
                    _inc_emp_faena = emp_faena_doc_sel_ids is not None and len(emp_faena_doc_sel_ids) > 0
                    _inc_trab = selected_trab_ids is not None and len(selected_trab_ids) > 0

                    result = export_zip_for_faena(
                        int(faena_id),
                        include_global_empresa_docs=_inc_emp_global,
                        include_contrato=inc_contrato,
                        include_anexos=inc_anexos,
                        include_empresa_faena=_inc_emp_faena,
                        include_trabajadores=_inc_trab,
                        doc_types_empresa_global=None,
                        doc_types_empresa_faena=None,
                        doc_types_trabajador=None,
                        selected_empresa_faena_doc_ids=emp_faena_doc_sel_ids,
                        selected_trabajador_ids=selected_trab_ids,
                        selected_trabajador_doc_ids=selected_trab_doc_map,
                        selected_empresa_global_doc_ids=sel_emp_global_ids if sel_emp_global_ids else None,
                        selected_anexo_ids=sel_anexo_ids if sel_anexo_ids else None,
                    )
                    zip_bytes, name, _inc, _skip, _skip_names = result
                    path = persist_export(int(faena_id), zip_bytes, name)
                    st.success(f"✅ ZIP generado: **{_inc} documentos** incluidos")
                    if _skip > 0:
                        st.warning(f"⚠️ {_skip} documento(s) no pudieron incluirse (archivo no encontrado): {', '.join(_skip_names[:10])}")
                    auto_backup_db("export_zip")
                    st.download_button(
                        "📥 Descargar ZIP",
                        data=zip_bytes,
                        file_name=os.path.basename(path),
                        mime="application/zip",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.error(f"No se pudo generar ZIP: {e}")
        with col_info:
            st.caption("El ZIP se guarda en Supabase Storage para que persista entre reinicios.")

    with tab3:
        _is_sa = is_superadmin() if callable(is_superadmin) else False
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
            _storage_col = view.apply(doc_availability_label, axis=1)
            view["ubicación"] = _storage_col
            show_cols = ["id", "faena_nombre", "archivo", "tamaño", "ubicación", "created_at"]
            st.dataframe(view[show_cols], use_container_width=True, hide_index=True)
            st.caption(f"Historial acotado a la empresa activa: {tenant_name}")

            hid = st.selectbox(
                "ZIP del historial",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} — {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['created_at']})",
                key="exp_hist_pick",
            )
            row = view[view["id"] == hid].iloc[0]

            col_dl, col_del = st.columns([2, 1])
            with col_dl:
                try:
                    b = load_file_anywhere(row.get("file_path"), row.get("bucket"), row.get("object_path"))
                    st.download_button(
                        "📥 Descargar ZIP del historial",
                        data=b,
                        file_name=row["archivo"],
                        mime="application/zip",
                        use_container_width=True,
                        key="exp_hist_dl",
                    )
                except Exception as e:
                    _has_storage = bool(row.get("bucket")) and bool(row.get("object_path"))
                    if _has_storage:
                        st.error(f"No se pudo descargar el ZIP desde Storage: {e}")
                        st.caption("El archivo existe en Supabase Storage pero hubo un error de conexión. Intenta de nuevo en unos segundos.")
                    else:
                        st.warning(
                            "Este ZIP fue guardado solo en disco local y se perdió cuando Streamlit reinició. "
                            "Los ZIPs generados a partir de ahora se guardarán en Supabase Storage para que persistan entre reinicios."
                        )
                        st.caption("Ve a la pestaña **Generar ZIP** para volver a exportar esta faena.")
            with col_del:
                if _is_sa:
                    if st.button("🗑️ Eliminar registro", key=f"exp_hist_del_{int(hid)}", type="secondary", use_container_width=True):
                        st.session_state[f"_confirm_del_hist_{int(hid)}"] = True
                    if st.session_state.get(f"_confirm_del_hist_{int(hid)}"):
                        st.warning(f"¿Eliminar el registro #{int(hid)} del historial?")
                        c_yes, c_no = st.columns(2)
                        with c_yes:
                            if st.button("Sí, eliminar", key=f"exp_hist_del_yes_{int(hid)}", type="primary", use_container_width=True):
                                try:
                                    if callable(execute):
                                        execute(f"DELETE FROM export_historial WHERE id=?", (int(hid),))
                                    if callable(audit_log):
                                        audit_log("ELIMINAR_EXPORT", "export_historial", f"Registro #{int(hid)} eliminado")
                                    st.success("Registro eliminado del historial.")
                                    st.session_state.pop(f"_confirm_del_hist_{int(hid)}", None)
                                    st.rerun()
                                except Exception as _del_exc:
                                    st.error(f"Error al eliminar: {_del_exc}")
                        with c_no:
                            if st.button("Cancelar", key=f"exp_hist_del_no_{int(hid)}", use_container_width=True):
                                st.session_state.pop(f"_confirm_del_hist_{int(hid)}", None)
                                st.rerun()

            # Bulk delete for superadmin
            if _is_sa and len(view) > 1:
                st.divider()
                with st.expander("🗑️ Eliminar registros en lote (superadmin)", expanded=False):
                    _del_ids = st.multiselect(
                        "Selecciona registros a eliminar",
                        view["id"].tolist(),
                        format_func=lambda x: f"#{int(x)} — {view[view['id']==x].iloc[0]['archivo']}",
                        key="exp_hist_bulk_del",
                    )
                    if _del_ids and st.button(f"🗑️ Eliminar {len(_del_ids)} registro(s)", type="primary", use_container_width=True, key="exp_hist_bulk_del_btn"):
                        _deleted = 0
                        for _did in _del_ids:
                            try:
                                if callable(execute):
                                    execute("DELETE FROM export_historial WHERE id=?", (int(_did),))
                                    _deleted += 1
                            except Exception:
                                pass
                        if callable(audit_log):
                            audit_log("ELIMINAR_EXPORT_LOTE", "export_historial", f"{_deleted} registros eliminados")
                        st.success(f"✅ {_deleted} registro(s) eliminados del historial.")
                        st.rerun()

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
            view["ubicación"] = view.apply(doc_availability_label, axis=1)
            st.dataframe(view[["id", "year_month", "archivo", "tamaño", "ubicación", "created_at"]], use_container_width=True, hide_index=True)
            st.caption(f"Exportaciones mensuales visibles solo para: {tenant_name}")

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
                    "📥 Descargar ZIP mensual del historial",
                    data=b,
                    file_name=row["archivo"],
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_mes_hist_dl",
                )
            except Exception as e:
                _has_storage = bool(row.get("bucket")) and bool(row.get("object_path"))
                if _has_storage:
                    st.error(f"No se pudo descargar el ZIP desde Storage: {e}")
                else:
                    st.warning(
                        "Este ZIP mensual fue guardado solo en disco local y se perdió cuando Streamlit reinició. "
                        "Los ZIPs nuevos se guardarán en Supabase Storage automáticamente."
                    )

    with tab5:
        st.markdown("### 📄 Reporte de Cumplimiento por Faena")
        st.caption("Genera un reporte HTML descargable e imprimible con el estado documental completo de la faena seleccionada.")

        if faena_id and int(faena_id) > 0:
            from datetime import date as _date
            try:
                faena_info = fetch_df("SELECT f.nombre, m.nombre AS mandante, f.estado, f.fecha_inicio, f.fecha_termino FROM faenas f JOIN mandantes m ON m.id=f.mandante_id WHERE f.id=?", (int(faena_id),))
                fi = faena_info.iloc[0] if faena_info is not None and not faena_info.empty else {}
            except Exception:
                fi = {}

            pend = pendientes_obligatorios(int(faena_id))
            asig = fetch_df("SELECT t.rut, t.apellidos, t.nombres, t.cargo FROM asignaciones a JOIN trabajadores t ON t.id=a.trabajador_id WHERE a.faena_id=? ORDER BY t.apellidos", (int(faena_id),))

            if st.button("📄 Generar reporte HTML", type="primary", use_container_width=True, key="btn_compliance_report"):
                rows_html = ""
                total_ok = total_falt = 0
                if asig is not None and not asig.empty:
                    for _, tr in asig.iterrows():
                        rut = str(tr.get("rut",""))
                        nombre = f"{tr.get('apellidos','')} {tr.get('nombres','')}"
                        cargo = str(tr.get("cargo",""))
                        faltantes = pend.get(rut, [])
                        estado = "✅ Completo" if not faltantes else f"❌ Faltan {len(faltantes)}"
                        falt_txt = ", ".join(faltantes) if faltantes else "-"
                        color = "#dcfce7" if not faltantes else "#fee2e2"
                        rows_html += f'<tr style="background:{color}"><td>{rut}</td><td>{nombre}</td><td>{cargo}</td><td>{estado}</td><td style="font-size:11px">{falt_txt}</td></tr>\n'
                        if faltantes:
                            total_falt += 1
                        else:
                            total_ok += 1

                total_trab = total_ok + total_falt
                pct = round((total_ok / total_trab) * 100, 1) if total_trab else 0

                html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Reporte Cumplimiento - {fi.get('nombre','Faena')}</title>
<style>
body{{font-family:Arial,sans-serif;margin:30px;color:#1e293b}}
h1{{color:#1e40af;border-bottom:3px solid #1e40af;padding-bottom:8px}}
h2{{color:#334155;margin-top:24px}}
table{{width:100%;border-collapse:collapse;margin:12px 0}}
th{{background:#1e40af;color:#fff;padding:8px;text-align:left;font-size:12px}}
td{{padding:6px 8px;border-bottom:1px solid #e2e8f0;font-size:12px}}
.summary{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:16px;margin:16px 0}}
.metric{{display:inline-block;margin:0 24px 0 0}}
.metric b{{font-size:22px;color:#1e40af}}
@media print{{body{{margin:15px}} .no-print{{display:none}}}}
</style></head><body>
<h1>SEGAV ERP — Reporte de Cumplimiento Documental</h1>
<div class="summary">
<p><b>Empresa:</b> {tenant_name} ({tenant_key}) &nbsp;|&nbsp; <b>Faena:</b> {fi.get('nombre','N/A')} &nbsp;|&nbsp; <b>Mandante:</b> {fi.get('mandante','N/A')} &nbsp;|&nbsp; <b>Estado:</b> {fi.get('estado','N/A')}</p>
<p><b>Período:</b> {fi.get('fecha_inicio','?')} a {fi.get('fecha_termino','vigente')} &nbsp;|&nbsp; <b>Fecha reporte:</b> {_date.today().isoformat()}</p>
<div class="metric">Trabajadores: <b>{total_trab}</b></div>
<div class="metric">Completos: <b style="color:#16a34a">{total_ok}</b></div>
<div class="metric">Con faltantes: <b style="color:#dc2626">{total_falt}</b></div>
<div class="metric">Cobertura: <b>{pct}%</b></div>
</div>
<h2>Detalle por trabajador</h2>
<table>
<tr><th>RUT</th><th>Nombre</th><th>Cargo</th><th>Estado</th><th>Documentos faltantes</th></tr>
{rows_html}
</table>
<p style="margin-top:24px;font-size:10px;color:#94a3b8">Generado por SEGAV ERP · {_date.today().isoformat()} · Este reporte es imprimible (Ctrl+P)</p>
</body></html>"""

                st.download_button(
                    "⬇️ Descargar reporte HTML (imprimible como PDF)",
                    data=html.encode("utf-8"),
                    file_name=f"reporte_cumplimiento_faena_{int(faena_id)}.html",
                    mime="text/html",
                    use_container_width=True,
                    key="dl_compliance_html",
                )
                st.success(f"Reporte generado: {total_trab} trabajadores, cobertura {pct}%")
        else:
            st.info("Selecciona una faena en las pestañas anteriores para generar el reporte.")


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
