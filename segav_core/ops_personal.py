from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import streamlit as st

from segav_core.ui import ui_header, ui_tip

def page_trabajadores(
    *,
    fetch_df,
    conn,
    execute,
    auto_backup_db,
    build_trabajadores_template_xlsx,
    clean_rut,
    split_nombre_completo,
    norm_col,
    rut_input,
    segav_cargo_labels,
    parse_date_maybe,
    fetch_file_refs,
    cleanup_deleted_file_refs,
    trabajador_insert_or_update,
    apply_pending_trabajador_create_reset,
    show_pending_trabajador_create_flash,
):
    ui_header("Trabajadores", "Carga masiva por Excel o gestión manual. Puedes crear, editar o eliminar trabajadores. Luego asigna a faenas y adjunta documentos.")
    tab_list, tab_gestion, tab_import, tab_mass_docs = st.tabs(["📋 Listado", "🧩 Gestión", "📥 Importar Excel", "📦 Importar Docs Masivo"])

    # -------------------------
    # Tab 1: Importación Excel
    # -------------------------
    with tab_import:
        st.write("Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        st.download_button(
            "⬇️ Descargar plantilla Excel de trabajadores",
            data=build_trabajadores_template_xlsx(),
            file_name="plantilla_trabajadores.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_tpl_excel_trabajadores",
        )
        st.caption("Usa esta plantilla para la carga masiva. Mantén los encabezados tal como vienen en el Excel.")
        up = st.file_uploader("Sube Excel (.xlsx)", type=["xlsx"], key="up_excel_trabajadores")
        if up is not None:
            try:
                xls = pd.ExcelFile(up)
                sheet = st.selectbox("Hoja", xls.sheet_names, index=0, key="sheet_excel_trab")
                raw = pd.read_excel(xls, sheet_name=sheet)

                colmap = {c: norm_col(str(c)) for c in raw.columns}
                df = raw.rename(columns=colmap).copy()

                st.caption("Vista previa (primeras 10 filas)")
                st.dataframe(df.head(10), use_container_width=True)

                if "rut" not in df.columns or "nombre" not in df.columns:
                    st.error("El Excel debe tener columnas 'RUT' y 'NOMBRE'.")
                else:
                    overwrite = st.checkbox("Sobrescribir si el RUT ya existe", value=True, key="ow_excel_trab")

                    if st.button("Importar Excel ahora", type="primary", key="btn_import_excel_trab"):
                        existing_set = set(fetch_df("SELECT rut FROM trabajadores")["rut"].astype(str).tolist())

                        rows = inserted = updated = skipped = 0
                        has_cargo = "cargo" in df.columns
                        has_cc = "centro_costo" in df.columns
                        has_email = "email" in df.columns
                        fc_col = "fecha_de_contrato" if "fecha_de_contrato" in df.columns else ("fecha_contrato" if "fecha_contrato" in df.columns else None)
                        has_ve = "vigencia_examen" in df.columns

                        def _to_text_date_import_excel(v):
                            if v is None or pd.isna(v):
                                return None
                            if isinstance(v, datetime):
                                return str(v.date())
                            if isinstance(v, date):
                                return str(v)
                            return str(v)

                        with conn() as c:
                            for _, r in df.iterrows():
                                rows += 1
                                rut = clean_rut(str(r.get("rut", "") or ""))
                                nombre = str(r.get("nombre", "") or "").strip()

                                if not rut or rut.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue
                                if not nombre or nombre.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue

                                nombres, apellidos = split_nombre_completo(nombre)
                                cargo = str(r.get("cargo", "") or "").strip() if has_cargo else ""
                                centro_costo = str(r.get("centro_costo", "") or "").strip() if has_cc else ""
                                email = str(r.get("email", "") or "").strip() if has_email else ""
                                fecha_contrato = _to_text_date_import_excel(r.get(fc_col)) if fc_col else None
                                vigencia_examen = _to_text_date_import_excel(r.get("vigencia_examen")) if has_ve else None

                                action, _tid = trabajador_insert_or_update(
                                    c,
                                    rut=rut,
                                    nombres=nombres,
                                    apellidos=apellidos,
                                    cargo=cargo,
                                    centro_costo=centro_costo,
                                    email=email,
                                    fecha_contrato=fecha_contrato,
                                    vigencia_examen=vigencia_examen,
                                    overwrite=overwrite,
                                    existing_id=None,
                                )
                                if action == "inserted":
                                    inserted += 1
                                elif action == "updated":
                                    updated += 1
                                else:
                                    skipped += 1
                                existing_set.add(rut)

                            c.commit()

                        st.success(f"Importación lista. Filas leídas: {rows} | Insertados: {inserted} | Actualizados: {updated} | Omitidos: {skipped}")
                        auto_backup_db("import_excel")
                        st.rerun()
            except Exception as e:
                st.error(f"No se pudo leer/importar el Excel: {e}")

    # -------------------------
    # Tab 2: Gestión (crear/editar/eliminar)
    # -------------------------
    with tab_gestion:
        t_create, t_edit = st.tabs(["➕ Crear", "✏️ Editar / 🗑️ Eliminar"])

        with t_create:
            apply_pending_trabajador_create_reset()
            show_pending_trabajador_create_flash()
            st.caption("El RUT se formatea automáticamente al estilo chileno: XX.XXX.XXX-X")
            rut = rut_input("RUT", key="trabajador_create_rut", placeholder="12.345.678-9", help="Escribe el RUT sin preocuparte por puntos o guion. La app lo formatea sola.")
            nombres = st.text_input("Nombres", placeholder="Juan", key="trabajador_create_nombres")
            apellidos = st.text_input("Apellidos", placeholder="Pérez", key="trabajador_create_apellidos")
            cargo = st.selectbox("Cargo", segav_cargo_labels(active_only=True), key="trabajador_create_cargo")
            centro_costo = st.text_input("Centro de costo (opcional)", placeholder="FAENA", key="trabajador_create_cc")
            email = st.text_input("Email (opcional)", key="trabajador_create_email")
            fecha_contrato = st.date_input("Fecha de contrato (opcional)", value=None, key="trabajador_create_fc")
            vigencia_examen = st.date_input("Vigencia examen (opcional)", value=None, key="trabajador_create_ve")
            ok = st.button("Guardar trabajador", type="primary", key="trabajador_create_btn")

            if ok:
                rut_norm = clean_rut(rut)
                nombres_v = nombres.strip()
                apellidos_v = apellidos.strip()
                cargo_v = cargo.strip()
                centro_costo_v = centro_costo.strip()
                email_v = email.strip()
                fecha_contrato_v = str(fecha_contrato) if fecha_contrato else None
                vigencia_examen_v = str(vigencia_examen) if vigencia_examen else None

                if not (rut_norm.strip() and nombres_v and apellidos_v):
                    st.error("Debes completar RUT, Nombres y Apellidos.")
                    st.stop()
                try:
                    with conn() as c:
                        _action, _tid = trabajador_insert_or_update(
                            c,
                            rut=rut_norm,
                            nombres=nombres_v,
                            apellidos=apellidos_v,
                            cargo=cargo_v,
                            centro_costo=centro_costo_v,
                            email=email_v,
                            fecha_contrato=fecha_contrato_v,
                            vigencia_examen=vigencia_examen_v,
                            overwrite=True,
                            existing_id=None,
                        )
                        c.commit()
                    st.session_state["_trabajador_create_reset_pending"] = True
                    st.session_state["_trabajador_create_flash"] = "Trabajador guardado."
                    auto_backup_db("trabajador")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo guardar: {e}")

        with t_edit:
            df = fetch_df("SELECT id, rut, apellidos, nombres, cargo, centro_costo, email, fecha_contrato, vigencia_examen FROM trabajadores ORDER BY apellidos, nombres")
            if df.empty:
                st.info("No hay trabajadores aún.")
                return

            def _fmt_trab_edit(x):
                r = df[df["id"] == x].iloc[0]
                return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

            tid = st.selectbox("Selecciona trabajador", df["id"].tolist(), format_func=_fmt_trab_edit, key="trab_edit_sel")
            row = df[df["id"] == tid].iloc[0]

            st.markdown("### ✏️ Editar trabajador")
            edit_prefix = f"trabajador_edit_{int(tid)}"
            if st.session_state.get(f"{edit_prefix}_loaded_id") != int(tid):
                st.session_state[f"{edit_prefix}_loaded_id"] = int(tid)
                st.session_state[f"{edit_prefix}_rut"] = clean_rut(str(row["rut"] or ""))
                st.session_state[f"{edit_prefix}_nombres"] = str(row["nombres"] or "")
                st.session_state[f"{edit_prefix}_apellidos"] = str(row["apellidos"] or "")
                st.session_state[f"{edit_prefix}_cargo"] = str(row["cargo"] or "")
                st.session_state[f"{edit_prefix}_cc"] = str(row["centro_costo"] or "")
                st.session_state[f"{edit_prefix}_email"] = str(row["email"] or "")
                st.session_state[f"{edit_prefix}_fc"] = parse_date_maybe(row["fecha_contrato"])
                st.session_state[f"{edit_prefix}_ve"] = parse_date_maybe(row["vigencia_examen"])

            rut_new = rut_input("RUT", key=f"{edit_prefix}_rut", value=str(row["rut"] or ""), placeholder="12.345.678-9", help="Escribe el RUT sin preocuparte por puntos o guion. La app lo formatea sola.")
            nombres_new = st.text_input("Nombres", key=f"{edit_prefix}_nombres")
            apellidos_new = st.text_input("Apellidos", key=f"{edit_prefix}_apellidos")
            cargo_base_options = segav_cargo_labels(active_only=True)
            cargo_actual = str(st.session_state.get(f"{edit_prefix}_cargo", "") or "").strip()
            cargo_options = cargo_base_options.copy()
            if cargo_actual and cargo_actual not in cargo_options:
                cargo_options = [cargo_actual] + cargo_options
            cargo_default = cargo_options.index(cargo_actual) if cargo_actual in cargo_options else 0
            cargo_new = st.selectbox("Cargo", cargo_options, index=cargo_default, key=f"{edit_prefix}_cargo_select")
            st.session_state[f"{edit_prefix}_cargo"] = cargo_new
            cc_new = st.text_input("Centro de costo (opcional)", key=f"{edit_prefix}_cc")
            email_new = st.text_input("Email (opcional)", key=f"{edit_prefix}_email")
            fc_new = st.date_input("Fecha de contrato (opcional)", key=f"{edit_prefix}_fc")
            ve_new = st.date_input("Vigencia examen (opcional)", key=f"{edit_prefix}_ve")
            ok_upd = st.button("Guardar cambios", type="primary", key=f"{edit_prefix}_save")

            if ok_upd:
                if not (rut_new.strip() and nombres_new.strip() and apellidos_new.strip()):
                    st.error("Debes completar RUT, Nombres y Apellidos.")
                    st.stop()
                try:
                    rut_norm_new = clean_rut(rut_new)
                    execute(
                        "UPDATE trabajadores SET rut=?, nombres=?, apellidos=?, cargo=?, centro_costo=?, email=?, fecha_contrato=?, vigencia_examen=? WHERE id=?",
                        (
                            rut_norm_new,
                            nombres_new.strip(),
                            apellidos_new.strip(),
                            cargo_new.strip(),
                            cc_new.strip(),
                            email_new.strip(),
                            str(fc_new) if fc_new else None,
                            str(ve_new) if ve_new else None,
                            int(tid),
                        ),
                    )
                    st.success("Trabajador actualizado.")
                    auto_backup_db("trabajador_edit")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo actualizar: {e}")

            st.divider()
            st.markdown("### 🗑️ Eliminar trabajador")
            st.caption("Se eliminarán también sus asignaciones a faenas y sus documentos. La app intentará limpiar además los archivos físicos que ya no queden referenciados.")

            dep_asg = fetch_df("SELECT COUNT(*) AS n FROM asignaciones WHERE trabajador_id=?", (int(tid),))
            dep_docs = fetch_df("SELECT COUNT(*) AS n FROM trabajador_documentos WHERE trabajador_id=?", (int(tid),))
            dep_faenas = fetch_df("SELECT COUNT(DISTINCT faena_id) AS n FROM asignaciones WHERE trabajador_id=?", (int(tid),))

            n_asg = int(dep_asg["n"].iloc[0]) if not dep_asg.empty else 0
            n_docs = int(dep_docs["n"].iloc[0]) if not dep_docs.empty else 0
            n_faenas = int(dep_faenas["n"].iloc[0]) if not dep_faenas.empty else 0

            st.warning(f"Dependencias: {n_asg} asignaciones (en {n_faenas} faenas) · {n_docs} documentos")

            confirm = st.checkbox("Confirmo que deseo eliminar este trabajador", key="chk_del_trab")
            if st.button("Eliminar trabajador definitivamente", type="secondary", key="btn_del_trab"):
                if not confirm:
                    st.error("Debes confirmar el checkbox antes de eliminar.")
                    st.stop()
                try:
                    refs = fetch_file_refs("trabajador_documentos", "trabajador_id=?", (int(tid),))
                    execute("DELETE FROM asignaciones WHERE trabajador_id=?", (int(tid),))
                    execute("DELETE FROM trabajador_documentos WHERE trabajador_id=?", (int(tid),))
                    execute("DELETE FROM trabajadores WHERE id=?", (int(tid),))
                    cleanup_issues = cleanup_deleted_file_refs(refs)
                    if cleanup_issues:
                        st.warning("Trabajador eliminado, pero hubo problemas al limpiar archivos asociados: " + " | ".join(cleanup_issues))
                    else:
                        st.success("Trabajador eliminado.")
                    auto_backup_db("trabajador_delete")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo eliminar: {e}")

    # -------------------------
    # Tab 3: Listado
    # -------------------------
    with tab_list:
        df = fetch_df(
            """
            SELECT
                t.id,
                t.rut,
                t.apellidos,
                t.nombres,
                t.cargo,
                COALESCE(
                    (
                        SELECT f.nombre
                        FROM asignaciones a
                        JOIN faenas f ON f.id = a.faena_id
                        WHERE a.trabajador_id = t.id
                          AND COALESCE(NULLIF(TRIM(UPPER(a.estado)), ''), 'ACTIVA') <> 'CERRADA'
                        ORDER BY a.id DESC
                        LIMIT 1
                    ),
                    'PLANTA'
                ) AS faena_actual,
                t.email,
                t.fecha_contrato,
                t.vigencia_examen
            FROM trabajadores t
            ORDER BY t.id DESC
            """
        )
        q = st.text_input("🔍 Buscar", placeholder="RUT, nombre, cargo o faena", key="q_trab_list")
        # ── Filtros avanzados ─────────────────────────────────────────────────
        with st.expander("Filtros avanzados", expanded=False):
            fcol1, fcol2 = st.columns(2)
            with fcol1:
                cargos_uniq = ["(Todos)"] + sorted(df["cargo"].dropna().astype(str).unique().tolist())
                filtro_cargo = st.selectbox("Cargo", cargos_uniq, key="trab_f_cargo")
            with fcol2:
                faenas_uniq = ["(Todas)"] + sorted(df["faena_actual"].dropna().astype(str).unique().tolist())
                filtro_faena = st.selectbox("Faena actual", faenas_uniq, key="trab_f_faena")

        out = df.copy()
        if q.strip():
            qq = q.strip().lower()
            out = out[
                out["rut"].astype(str).str.lower().str.contains(qq, na=False) |
                out["apellidos"].astype(str).str.lower().str.contains(qq, na=False) |
                out["nombres"].astype(str).str.lower().str.contains(qq, na=False) |
                out["cargo"].astype(str).str.lower().str.contains(qq, na=False) |
                out["faena_actual"].astype(str).str.lower().str.contains(qq, na=False)
            ]
        if filtro_cargo != "(Todos)":
            out = out[out["cargo"].astype(str) == filtro_cargo]
        if filtro_faena != "(Todas)":
            out = out[out["faena_actual"].astype(str) == filtro_faena]

        st.caption(f"Mostrando {len(out)} de {len(df)} trabajadores")

        # ── Paginación ────────────────────────────────────────────────────────
        PAGE_SIZE = 50
        total_rows = len(out)
        if total_rows > PAGE_SIZE:
            n_pages = (total_rows - 1) // PAGE_SIZE + 1
            page = st.number_input(f"Página (1–{n_pages})", min_value=1, max_value=n_pages, value=1, step=1, key="trab_pg")
            page_start = (page - 1) * PAGE_SIZE
            out_page = out.iloc[page_start: page_start + PAGE_SIZE]
        else:
            out_page = out

        show = out_page.rename(
            columns={
                "rut": "RUT",
                "apellidos": "Apellidos",
                "nombres": "Nombres",
                "cargo": "Cargo",
                "faena_actual": "Faena actual",
                "email": "Email",
                "fecha_contrato": "Fecha de contrato",
                "vigencia_examen": "Vigencia examen",
            }
        )
        st.dataframe(show, use_container_width=True, hide_index=True)
        st.caption("Para editar/eliminar: ve a la pestaña **Gestión → Editar / Eliminar**.")

        # ── Exportar a Excel ──────────────────────────────────────────────────
        if not out.empty:
            try:
                import io
                buf = io.BytesIO()
                export_df = out.copy()
                export_df = export_df.rename(columns={
                    "rut": "RUT", "apellidos": "Apellidos", "nombres": "Nombres",
                    "cargo": "Cargo", "faena_actual": "Faena actual", "email": "Email",
                    "fecha_contrato": "Fecha de contrato", "vigencia_examen": "Vigencia examen",
                })
                cols_export = [c for c in ["RUT","Apellidos","Nombres","Cargo","Faena actual","Email","Fecha de contrato","Vigencia examen"] if c in export_df.columns]
                export_df[cols_export].to_excel(buf, index=False, sheet_name="Trabajadores", engine="openpyxl")
                st.download_button(
                    "📥 Exportar trabajadores a Excel",
                    data=buf.getvalue(),
                    file_name="trabajadores_segav.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_export_trab_xlsx",
                    use_container_width=True,
                )
            except Exception as _e:
                st.caption(f"⚠️ No se pudo generar Excel: {_e}")

    # ── Tab: Importar documentos masivo ───────────────────────────────────
    with tab_mass_docs:
        st.markdown("### 📦 Importación masiva de documentos")
        st.caption(
            "Sube un archivo ZIP con carpetas nombradas por **RUT** del trabajador. "
            "Dentro de cada carpeta, los archivos se asignarán como documentos del trabajador correspondiente."
        )
        st.markdown("""
**Estructura esperada del ZIP:**
```
documentos/
├── 12345678-9/
│   ├── contrato.pdf
│   ├── ODI.pdf
│   └── examen.pdf
├── 98765432-1/
│   ├── contrato.pdf
│   └── certificado.pdf
```
        """)

        up_zip = st.file_uploader("Sube archivo ZIP", type=["zip"], key="mass_docs_zip")
        if up_zip is not None and st.button("📥 Procesar ZIP", type="primary", use_container_width=True, key="mass_docs_process"):
            import zipfile, io, os as _os
            try:
                zf = zipfile.ZipFile(io.BytesIO(up_zip.getvalue()))
                names = zf.namelist()

                # Group files by RUT folder
                rut_files = {}
                for name in names:
                    parts = name.replace("\\", "/").strip("/").split("/")
                    if len(parts) >= 2 and not name.endswith("/"):
                        rut_candidate = clean_rut(parts[-2]) if len(parts[-2]) > 5 else None
                        if rut_candidate:
                            rut_files.setdefault(rut_candidate, []).append(name)

                if not rut_files:
                    st.error("No se encontraron carpetas con RUT válidos en el ZIP.")
                else:
                    # Match with existing workers
                    existing = fetch_df("SELECT id, rut FROM trabajadores")
                    rut_to_id = {}
                    if existing is not None and not existing.empty:
                        for _, r in existing.iterrows():
                            rut_to_id[clean_rut(str(r["rut"]))] = int(r["id"])

                    imported = skipped = not_found = 0
                    for rut, files in rut_files.items():
                        tid = rut_to_id.get(rut)
                        if tid is None:
                            not_found += len(files)
                            continue
                        for fname in files:
                            try:
                                file_bytes = zf.read(fname)
                                basename = _os.path.basename(fname)
                                doc_tipo = _os.path.splitext(basename)[0].upper().replace(" ", "_")
                                # Save to disk
                                save_dir = _os.path.join("uploads", "trabajadores", rut)
                                _os.makedirs(save_dir, exist_ok=True)
                                save_path = _os.path.join(save_dir, basename)
                                with open(save_path, "wb") as fp:
                                    fp.write(file_bytes)
                                from hashlib import sha256
                                sha = sha256(file_bytes).hexdigest()
                                execute(
                                    "INSERT INTO trabajador_documentos(trabajador_id, doc_tipo, nombre_archivo, file_path, sha256, created_at) VALUES(?,?,?,?,?,?)",
                                    (tid, doc_tipo, basename, save_path, sha,
                                     __import__('datetime').datetime.now().isoformat(timespec='seconds')),
                                )
                                imported += 1
                            except Exception:
                                skipped += 1

                    auto_backup_db("mass_docs_import")
                    st.success(f"✅ Importación completa: {imported} documentos importados, {skipped} omitidos, {not_found} sin RUT coincidente.")
                    if not_found:
                        missing_ruts = [r for r in rut_files if r not in rut_to_id]
                        st.warning(f"RUTs no encontrados: {', '.join(missing_ruts[:10])}")
                    st.rerun()
            except Exception as e:
                st.error(f"Error al procesar ZIP: {e}")


def page_asignar_trabajadores(
    *,
    fetch_df,
    conn,
    cursor_execute,
    ASSIGNACION_INSERT_SQL,
    clear_app_caches,
    auto_backup_db,
    build_trabajadores_template_xlsx,
    clean_rut,
    split_nombre_completo,
    norm_col,
    executemany,
    go,
    trabajador_insert_or_update,
):
    ui_header("Asignar Trabajadores", "Carga e incorpora trabajadores por faena. Si un trabajador se repite en otra faena, mantiene su documentación ya cargada.")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        ui_tip("Crea faenas primero.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        faena_id = st.selectbox(
            "Faena",
            faenas["id"].tolist(),
            format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']}",
        )
    with col2:
        st.session_state["selected_faena_id"] = int(faena_id)

    tab1, tab2, tab3 = st.tabs(["🧩 Asignar existentes", "📥 Importar Excel y asignar", "📋 Asignados"])

    # -------------------------
    # Tab 1: asignar existentes
    # -------------------------
    with tab1:
        trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
        if trab.empty:
            ui_tip("Crea trabajadores primero (o usa 'Importar Excel y asignar').")
            return

        asignados = fetch_df("SELECT trabajador_id FROM asignaciones WHERE faena_id=?", (int(faena_id),))
        asignados_ids = set(asignados["trabajador_id"].tolist()) if not asignados.empty else set()
        disponibles = trab[~trab["id"].isin(asignados_ids)].copy()

        def _fmt_trab(x):
            r = trab[trab["id"] == x].iloc[0]
            return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

        st.markdown("#### Agregar asignaciones")
        if disponibles.empty:
            st.success("Todos los trabajadores ya están asignados.")
        else:
            with st.form("form_asignar"):
                seleccion = st.multiselect("Selecciona trabajadores", disponibles["id"].tolist(), format_func=_fmt_trab)
                fecha_ingreso = st.date_input("Fecha ingreso", value=date.today())
                cargo_faena = st.text_input("Cargo en faena (opcional, aplica a todos)")
                ok = st.form_submit_button("Asignar seleccionados", type="primary")

            if ok:
                if len(seleccion) == 0:
                    st.error("Selecciona al menos un trabajador para asignar.")
                    st.stop()
                inserted_count = 0
                skipped_count = 0
                with conn() as c:
                    for tid in seleccion:
                        cur = cursor_execute(
                            c,
                            ASSIGNACION_INSERT_SQL,
                            (int(faena_id), int(tid), cargo_faena.strip(), str(fecha_ingreso), None, "ACTIVA"),
                        )
                        try:
                            rc = int(cur.rowcount or 0)
                        except Exception:
                            rc = 0
                        if rc > 0:
                            inserted_count += 1
                        else:
                            skipped_count += 1
                    c.commit()
                clear_app_caches()
                st.session_state["docs_scoped_toggle"] = True
                st.session_state.pop("docs_trabajador_pick", None)
                msg = f"Trabajadores asignados: {inserted_count}."
                if skipped_count:
                    msg += f" Omitidos por ya existir: {skipped_count}."
                st.success(msg)
                auto_backup_db("asignacion")
                st.rerun()

    # ---------------------------------
    # Tab 2: importar Excel y asignar
    # ---------------------------------
    with tab2:
        st.write("Sube Excel de trabajadores para **esta faena**. Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        st.download_button(
            "⬇️ Descargar plantilla Excel de trabajadores",
            data=build_trabajadores_template_xlsx(),
            file_name="plantilla_trabajadores.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_tpl_trab_faena",
        )
        st.caption("Puedes usar la misma plantilla para la carga masiva por faena.")
        up = st.file_uploader("Sube Excel (.xlsx)", type=["xlsx"], key="up_excel_trab_por_faena")
        if up is not None:
            try:
                xls = pd.ExcelFile(up)
                sheet = st.selectbox("Hoja", xls.sheet_names, index=0, key="sheet_trab_por_faena")
                raw = pd.read_excel(xls, sheet_name=sheet)

                colmap = {c: norm_col(str(c)) for c in raw.columns}
                df = raw.rename(columns=colmap).copy()

                st.caption("Vista previa (primeras 10 filas)")
                st.dataframe(df.head(10), use_container_width=True)

                if "rut" not in df.columns or "nombre" not in df.columns:
                    st.error("El Excel debe tener columnas 'RUT' y 'NOMBRE'.")
                else:
                    overwrite = st.checkbox("Sobrescribir datos si el RUT ya existe", value=True, key="ow_trab_por_faena")
                    fecha_ingreso = st.date_input("Fecha ingreso para esta faena", value=date.today(), key="fi_trab_por_faena")
                    cargo_faena_all = st.text_input("Cargo en faena (opcional, aplica a todos)", key="cargo_faena_all")

                    if st.button("Importar y asignar a esta faena", type="primary"):
                        existing = fetch_df("SELECT rut, id FROM trabajadores")
                        rut_to_id = {str(r["rut"]): int(r["id"]) for _, r in existing.iterrows()} if not existing.empty else {}

                        rows = inserted = updated = skipped = assigned = 0

                        has_cargo = "cargo" in df.columns
                        has_cc = "centro_costo" in df.columns
                        has_email = "email" in df.columns
                        fc_col = "fecha_de_contrato" if "fecha_de_contrato" in df.columns else ("fecha_contrato" if "fecha_contrato" in df.columns else None)
                        has_ve = "vigencia_examen" in df.columns

                        def _to_text_date_import_faena(v):
                            if v is None or pd.isna(v):
                                return None
                            if isinstance(v, datetime):
                                return str(v.date())
                            if isinstance(v, date):
                                return str(v)
                            return str(v)

                        with conn() as c:
                            for _, r in df.iterrows():
                                rows += 1
                                rut = clean_rut(str(r.get("rut", "") or ""))
                                nombre = str(r.get("nombre", "") or "").strip()

                                if not rut or rut.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue
                                if not nombre or nombre.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue

                                nombres, apellidos = split_nombre_completo(nombre)
                                cargo = str(r.get("cargo", "") or "").strip() if has_cargo else ""
                                centro_costo = str(r.get("centro_costo", "") or "").strip() if has_cc else ""
                                email = str(r.get("email", "") or "").strip() if has_email else ""
                                fecha_contrato = _to_text_date_import_faena(r.get(fc_col)) if fc_col else None
                                vigencia_examen = _to_text_date_import_faena(r.get("vigencia_examen")) if has_ve else None

                                action, tid_saved = trabajador_insert_or_update(
                                    c,
                                    rut=rut,
                                    nombres=nombres,
                                    apellidos=apellidos,
                                    cargo=cargo,
                                    centro_costo=centro_costo,
                                    email=email,
                                    fecha_contrato=fecha_contrato,
                                    vigencia_examen=vigencia_examen,
                                    overwrite=overwrite,
                                    existing_id=rut_to_id.get(rut),
                                )
                                if action == "inserted":
                                    inserted += 1
                                elif action == "updated":
                                    updated += 1
                                else:
                                    skipped += 1
                                    continue

                                # obtener id del trabajador
                                if rut not in rut_to_id:
                                    if tid_saved:
                                        rut_to_id[rut] = int(tid_saved)
                                    else:
                                        rid = cursor_execute(c, "SELECT id FROM trabajadores WHERE rut=?", (rut,)).fetchone()
                                        if rid:
                                            rut_to_id[rut] = int(rid[0])

                                tid = rut_to_id.get(rut)
                                if tid:
                                    cur_asg = cursor_execute(
                                        c,
                                        ASSIGNACION_INSERT_SQL,
                                        (int(faena_id), int(tid), cargo_faena_all.strip(), str(fecha_ingreso), None, "ACTIVA"),
                                    )
                                    try:
                                        assigned += int(cur_asg.rowcount or 0)
                                    except Exception:
                                        pass

                            c.commit()

                        clear_app_caches()
                        st.session_state["docs_scoped_toggle"] = True
                        st.session_state.pop("docs_trabajador_pick", None)
                        st.success(f"Listo. Filas: {rows} | Insertados: {inserted} | Actualizados: {updated} | Omitidos: {skipped} | Asignados: {assigned}")
                        auto_backup_db("import_asignar_faena")
                        # llevar a docs con la faena seleccionada
                        st.session_state["selected_faena_id"] = int(faena_id)
                        go("Documentos Trabajador", faena_id=int(faena_id))
            except Exception as e:
                st.error(f"No se pudo leer/importar el Excel: {e}")

    # -------------------------
    # Tab 3: asignados + quitar
    # -------------------------
    with tab3:
        docs_asg = fetch_df('''
            SELECT a.id AS asignacion_id,
                   t.id AS trabajador_id,
                   t.apellidos || ' ' || t.nombres AS trabajador,
                   t.rut,
                   a.cargo_faena,
                   a.fecha_ingreso,
                   a.estado
            FROM asignaciones a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.faena_id=?
            ORDER BY t.apellidos, t.nombres
        ''', (int(faena_id),))

        if docs_asg.empty:
            st.info("(sin trabajadores asignados)")
        else:
            st.dataframe(
                docs_asg[["trabajador","rut","cargo_faena","fecha_ingreso","estado"]],
                use_container_width=True,
                hide_index=True,
            )

            st.divider()
            st.markdown("#### 🗑️ Quitar trabajadores de esta faena")
            st.caption("Esto **solo elimina la asignación** (no elimina al trabajador ni sus documentos).")

            def _fmt_asg(tid):
                r = docs_asg[docs_asg["trabajador_id"] == tid].iloc[0]
                return f"{r['trabajador']} ({r['rut']})"

            to_remove = st.multiselect(
                "Selecciona trabajadores a quitar",
                docs_asg["trabajador_id"].tolist(),
                format_func=_fmt_asg,
                key="asg_remove_multi",
            )
            confirm = st.checkbox(
                "Confirmo que deseo quitar los seleccionados de esta faena",
                key="asg_remove_confirm",
            )

            cols = st.columns([1, 1, 2])
            with cols[0]:
                if st.button("Quitar seleccionados", type="secondary", use_container_width=True, key="btn_asg_remove"):
                    if not to_remove:
                        st.error("Selecciona al menos un trabajador.")
                        st.stop()
                    if not confirm:
                        st.error("Debes confirmar el checkbox antes de quitar.")
                        st.stop()
                    try:
                        params = [(int(faena_id), int(tid)) for tid in to_remove]
                        executemany("DELETE FROM asignaciones WHERE faena_id=? AND trabajador_id=?", params)
                        st.success(f"Listo. Quitados: {len(to_remove)}")
                        auto_backup_db("asignacion_remove")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo quitar: {e}")

            with cols[1]:
                if st.button("Limpiar selección", use_container_width=True, key="btn_asg_remove_clear"):
                    st.session_state["asg_remove_multi"] = []
                    st.rerun()

def page_documentos_trabajador(
    *,
    DB_BACKEND,
    fetch_df,
    fetch_df_uncached,
    execute,
    execute_rowcount,
    auto_backup_db,
    fetch_assigned_workers,
    prepare_upload_payload,
    render_upload_help,
    save_file_online,
    sha256_bytes,
    load_file_anywhere,
    worker_required_docs_for_record,
    doc_tipo_label,
    doc_tipo_join,
    safe_name,
    canonical_cargo_label,
    cargo_docs_catalog_rows,
    pendientes_obligatorios,
    delete_uploaded_document_record,
):
    ui_header(
        "Documentos Trabajador",
        "Carga documentos obligatorios por trabajador. Puedes trabajar por FAENA: selecciona una faena y verás solo los trabajadores asignados.",
    )

    # Lista de faenas para selector local (en este mismo apartado)
    faenas = fetch_df(
        '''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
        '''
    )

    # Selector de faena dentro del apartado (no genera cajas vacías)
    current = st.session_state.get("selected_faena_id")
    ids = [None] + (faenas["id"].tolist() if not faenas.empty else [])
    default_index = ids.index(current) if (current in ids) else 0

    c1, c2 = st.columns([3, 1])
    with c1:
        faena_pick = st.selectbox(
            "Faena (opcional)",
            ids,
            index=default_index,
            format_func=lambda x: "(sin faena)" if x is None else (
                f"{int(x)} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']} ({faenas[faenas['id']==x].iloc[0]['estado']})"
            ),
            key="docs_faena_pick",
        )
        st.session_state["selected_faena_id"] = None if faena_pick is None else int(faena_pick)

    with c2:
        default_scoped = True if faena_pick is not None else False
        scoped = st.toggle("Solo esta faena", value=default_scoped, key="docs_scoped_toggle")

    st.divider()

    last_scope_key = "_docs_last_scope_signature"
    current_scope_sig = (None if faena_pick is None else int(faena_pick), bool(scoped))
    if st.session_state.get(last_scope_key) != current_scope_sig:
        st.session_state[last_scope_key] = current_scope_sig
        st.session_state.pop("docs_trabajador_pick", None)

    # Fuente de trabajadores: por faena o global
    if scoped:
        if faena_pick is None:
            st.error("Activa 'Solo esta faena' pero no has seleccionado una faena.")
            st.stop()

        trab = fetch_assigned_workers(int(faena_pick), fresh=True)
        assigned_count = len(trab.index)
        st.caption(f"Trabajadores asignados detectados en esta faena: {assigned_count}")
        if trab.empty:
            ui_tip("Esta faena no tiene trabajadores asignados. Ve a 'Asignar Trabajadores' para incorporar personal.")
            return

        # Pendientes por faena (resumen accionable)
        with st.expander("✅ Pendientes de la faena (por trabajador)", expanded=True):
            pend = pendientes_obligatorios(int(faena_pick))
            if not pend:
                st.info("(sin asignaciones)")
            else:
                ok = sum(1 for v in pend.values() if not v)
                total = len(pend)
                st.metric("Trabajadores OK", f"{ok}/{total}")
                for k, missing in pend.items():
                    if missing:
                        st.error(f"{k} — faltan: {doc_tipo_join(missing)}")
                    else:
                        st.success(f"{k} — OK")
    else:
        trab = fetch_df_uncached("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
        if trab.empty:
            ui_tip("Crea trabajadores primero.")
            return

    # Selector de trabajador (solo asignados si scoped)
    def _fmt_trab_docs(x):
        r = trab[trab["id"] == x].iloc[0]
        return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

    tid = st.selectbox("Trabajador", trab["id"].tolist(), format_func=_fmt_trab_docs, key="docs_trabajador_pick")

    # Estado documental del trabajador (global: se reutiliza entre faenas)
    docs = fetch_df(
        "SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path, created_at FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id DESC",
        (int(tid),),
    )
    trabajador_row = trab[trab["id"] == tid].iloc[0]
    req_docs = worker_required_docs_for_record(trabajador_row)
    tipos_presentes = set(docs["doc_tipo"].astype(str).tolist()) if not docs.empty else set()
    faltan = [d for d in req_docs if d not in tipos_presentes]

    col1, col2, col3 = st.columns([1, 1, 2])
    col1.metric("Obligatorios", len(req_docs))
    col2.metric("Cargados", len([d for d in req_docs if d in tipos_presentes]))
    col3.metric("Faltan", len(faltan))

    cargo_label = canonical_cargo_label(trabajador_row.get("cargo"))
    st.caption(f"Cargo del trabajador: **{cargo_label}**")

    with st.expander("Ver documentos obligatorios por cargo", expanded=False):
        st.dataframe(pd.DataFrame(cargo_docs_catalog_rows()), use_container_width=True, hide_index=True)

    if faltan:
        st.warning("Faltan obligatorios: " + doc_tipo_join(faltan))
    else:
        st.success("Trabajador completo (obligatorios OK).")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos obligatorios configurados para este trabajador:")
        st.code("\n".join(doc_tipo_label(d) for d in req_docs))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", req_docs + ["OTRO"], key="doc_tipo_pick", format_func=lambda x: "OTRO" if x == "OTRO" else doc_tipo_label(x))
        with colx2:
            tipo_otro = st.text_input(
                "Si eliges OTRO, escribe el nombre",
                placeholder="Ej: Certificación operador, Licencia, Examen ocupacional",
                key="doc_tipo_otro",
            )

        up = st.file_uploader("Archivo", key="up_doc_trabajador", type=None)
        render_upload_help()
        if st.button("Guardar documento", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()

            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, 'type', None) or 'application/octet-stream')
            folder = ["trabajadores", tid, safe_name(doc_tipo)]
            file_path, bucket, object_path = save_file_online(folder, payload["file_name"], payload["file_bytes"], content_type=payload["content_type"])
            sha = sha256_bytes(payload["file_bytes"])
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])

            try:

                execute(

                    "INSERT INTO trabajador_documentos(trabajador_id, doc_tipo, nombre_archivo, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?,?)",

                    (int(tid), doc_tipo, payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),

                )

            except Exception:

                # Manejo de duplicados (UniqueViolation): actualiza el registro existente sin romper la app

                if DB_BACKEND == "postgres":

                    rc = execute_rowcount(

                        "UPDATE trabajador_documentos SET file_path=?, bucket=?, object_path=?, sha256=?, created_at=? "

                        "WHERE trabajador_id=? AND doc_tipo=? AND nombre_archivo=?",

                        (file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(tid), doc_tipo, payload["file_name"]),

                    )

                    if rc == 0:

                        execute_rowcount(

                            "UPDATE trabajador_documentos SET nombre_archivo=?, file_path=?, bucket=?, object_path=?, sha256=?, created_at=? "

                            "WHERE trabajador_id=? AND doc_tipo=?",

                            (payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(tid), doc_tipo),

                        )

                else:

                    raise
            st.success("Documento guardado.")
            auto_backup_db("doc_trabajador")
            st.rerun()


    with tab2:
        if docs.empty:
            st.info("(sin documentos)")
        else:
            show = docs[["doc_tipo","nombre_archivo","created_at"]].copy() if all(c in docs.columns for c in ["doc_tipo","nombre_archivo","created_at"]) else docs.copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### 🔎 Gestionar documento")

            ids = docs["id"].tolist()
            cur = st.session_state.get("trab_pick_doc", None)
            if cur not in ids:
                st.session_state["trab_pick_doc"] = ids[0]

            def _fmt_doc(x):
                try:
                    r = docs.loc[docs["id"] == x].iloc[0]
                    return f"{r.get('doc_tipo','DOC')} — {r.get('nombre_archivo','archivo')}"
                except Exception:
                    return f"ID {x}"

            pick_id = st.selectbox(
                "Documento",
                ids,
                format_func=_fmt_doc,
                key="trab_pick_doc",
            )

            sel = docs.loc[docs["id"] == pick_id]
            if sel.empty:
                st.warning("El documento seleccionado ya no está disponible en la lista. Vuelve a seleccionar.")
                return

            row = sel.iloc[0]
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
                    key="trab_dl_btn",
                )
            except Exception:
                st.warning(
                    "El archivo no está disponible (Storage/disco). "
                    "Verifica configuración de Storage o vuelve a cargar el documento."
                )

            confirm_del = st.checkbox(
                "Confirmo que quiero eliminar este documento cargado.",
                key="trab_del_confirm",
            )
            if st.button("Eliminar documento", type="secondary", use_container_width=True, key="trab_del_btn"):
                if not confirm_del:
                    st.error("Debes confirmar la eliminación.")
                    st.stop()
                result = delete_uploaded_document_record("trabajador_documentos", int(pick_id))
                if result["shared_refs"]:
                    st.info("El registro fue eliminado de la base de datos. El archivo físico se conservó porque está referenciado en otro registro.")
                elif result["cleanup_issues"]:
                    st.warning("El registro fue eliminado de la base de datos, pero hubo un problema al limpiar el archivo: " + " | ".join(result["cleanup_issues"]))
                st.success(f"Documento eliminado: {result['file_name']}")
                auto_backup_db("doc_trabajador_delete")
                st.rerun()
