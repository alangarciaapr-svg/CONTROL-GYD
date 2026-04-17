from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import streamlit as st

from segav_core.ui import ui_header, ui_tip


def page_mandantes(*, fetch_df, execute, auto_backup_db):
    ui_header("Mandantes", "Registra mandantes. Cada faena se asocia a un mandante. Aquí puedes crear, editar y revisar su avance.")

    k1, k2, k3 = st.columns(3)
    with k1:
        try:
            st.metric("Mandantes", int(fetch_df("SELECT COUNT(*) AS n FROM mandantes")["n"].iloc[0]))
        except Exception:
            st.metric("Mandantes", 0)
    with k2:
        try:
            st.metric("Contratos de faena", int(fetch_df("SELECT COUNT(*) AS n FROM contratos_faena")["n"].iloc[0]))
        except Exception:
            st.metric("Contratos de faena", 0)
    with k3:
        try:
            st.metric("Faenas", int(fetch_df("SELECT COUNT(*) AS n FROM faenas")["n"].iloc[0]))
        except Exception:
            st.metric("Faenas", 0)

    tab_over, tab_create, tab_manage = st.tabs(["📌 Overview", "➕ Crear", "✏️ Editar / 🗑️ Eliminar"])

    with tab_over:
        df = fetch_df(
            """
            SELECT
                m.id,
                m.nombre,
                (SELECT COUNT(*) FROM contratos_faena cf WHERE cf.mandante_id=m.id) AS contratos,
                (SELECT COUNT(*) FROM faenas f WHERE f.mandante_id=m.id) AS faenas_total,
                (SELECT COUNT(*) FROM faenas f WHERE f.mandante_id=m.id AND f.estado='ACTIVA') AS faenas_activas
            FROM mandantes m
            ORDER BY m.id DESC
            """
        )

        q = st.text_input("Buscar mandante", placeholder="Escribe nombre…", key="mand_q")
        out = df.copy()
        if q.strip():
            qq = q.strip().lower()
            out = out[out["nombre"].astype(str).str.lower().str.contains(qq, na=False)]

        c1, c2 = st.columns([2, 1])
        with c1:
            st.dataframe(
                out.rename(columns={
                    "nombre": "Mandante",
                    "contratos": "Contratos",
                    "faenas_total": "Faenas",
                    "faenas_activas": "Activas",
                }),
                use_container_width=True,
                hide_index=True,
            )
        with c2:
            st.markdown("#### Detalle rápido")
            if out.empty:
                st.info("Sin resultados.")
            else:
                mid = st.selectbox(
                    "Mandante",
                    out["id"].tolist(),
                    format_func=lambda x: out[out["id"] == x].iloc[0]["nombre"],
                    key="mand_detail_sel",
                )
                row = df[df["id"] == mid].iloc[0]
                st.metric("Contratos", int(row["contratos"]))
                st.metric("Faenas", int(row["faenas_total"]))
                st.metric("Faenas activas", int(row["faenas_activas"]))
                fa = fetch_df(
                    """
                    SELECT id, nombre, estado, fecha_inicio, fecha_termino
                    FROM faenas
                    WHERE mandante_id=?
                    ORDER BY id DESC
                    LIMIT 10
                    """,
                    (int(mid),),
                )
                if fa.empty:
                    st.caption("Sin faenas asociadas.")
                else:
                    st.caption("Últimas faenas (máx 10)")
                    st.dataframe(
                        fa.rename(columns={
                            "nombre": "Faena",
                            "estado": "Estado",
                            "fecha_inicio": "Inicio",
                            "fecha_termino": "Término",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )

    with tab_create:
        with st.form("form_mandante", clear_on_submit=True):
            nombre = st.text_input("Nombre mandante", placeholder="Bosque Los Lagos", key="mandante_nombre_in")
            ok = st.form_submit_button("Guardar mandante", type="primary")
        if ok:
            nombre_clean = (nombre or "").strip()
            if not nombre_clean:
                st.warning("Ingresa un nombre de mandante.")
            else:
                try:
                    execute("INSERT INTO mandantes(nombre) VALUES(?)", (nombre_clean,))
                    st.success("Mandante creado.")
                    auto_backup_db("mandante")
                    st.rerun()
                except Exception as e:
                    msg = str(e)
                    if "UNIQUE" in msg.upper():
                        st.error("Ya existe un mandante con ese nombre.")
                    else:
                        st.error(f"No se pudo crear: {e}")

    with tab_manage:
        df_all = fetch_df("SELECT id, nombre FROM mandantes ORDER BY id DESC")
        if df_all.empty:
            st.info("No hay mandantes para gestionar.")
        else:
            mid = st.selectbox(
                "Selecciona mandante",
                df_all["id"].tolist(),
                format_func=lambda x: df_all[df_all["id"] == x].iloc[0]["nombre"],
                key="mand_manage_sel",
            )
            row = df_all[df_all["id"] == mid].iloc[0]

            st.markdown("### ✏️ Editar")
            with st.form("form_mand_edit"):
                nombre_new = st.text_input("Nombre", value=str(row["nombre"] or ""), key="mand_name_new")
                ok_upd = st.form_submit_button("Guardar cambios", type="primary")
            if ok_upd:
                nn = (nombre_new or "").strip()
                if not nn:
                    st.error("El nombre no puede estar vacío.")
                else:
                    try:
                        execute("UPDATE mandantes SET nombre=? WHERE id=?", (nn, int(mid)))
                        st.success("Mandante actualizado.")
                        auto_backup_db("mandante_edit")
                        st.rerun()
                    except Exception as e:
                        msg = str(e)
                        if "UNIQUE" in msg.upper():
                            st.error("Ya existe un mandante con ese nombre.")
                        else:
                            st.error(f"No se pudo actualizar: {e}")

            st.divider()
            st.markdown("### 🗑️ Eliminar")
            dep = fetch_df("SELECT COUNT(*) AS n FROM faenas WHERE mandante_id=?", (int(mid),))
            n_faenas = int(dep["n"].iloc[0]) if not dep.empty else 0
            if n_faenas > 0:
                st.warning(f"No se puede eliminar porque tiene {n_faenas} faena(s) asociada(s). Primero reasigna o elimina esas faenas.")
            else:
                confirm = st.checkbox("Confirmo que deseo eliminar este mandante", key="mand_del_confirm")
                if st.button("Eliminar mandante definitivamente", type="secondary", key="mand_del_btn"):
                    if not confirm:
                        st.error("Debes confirmar antes de eliminar.")
                        st.stop()
                    try:
                        execute("DELETE FROM mandantes WHERE id=?", (int(mid),))
                        st.success("Mandante eliminado.")
                        auto_backup_db("mandante_delete")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo eliminar: {e}")


def page_contratos_faena(
    *,
    fetch_df,
    execute,
    auto_backup_db,
    render_upload_help,
    prepare_upload_payload,
    save_file_online,
    sha256_bytes,
    parse_date_maybe,
    fetch_file_refs,
    cleanup_deleted_file_refs,
    load_file_anywhere,
):
    ui_header("Contratos de Faena", "Crea, edita o elimina contratos por mandante. Puedes adjuntar archivo al contrato.")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        ui_tip("Primero crea un mandante.")
        return

    tab1, tab2 = st.tabs(["➕ Crear contrato", "✏️ Editar / Eliminar / Archivo"])

    with tab1:
        with st.form("form_contrato_faena", clear_on_submit=False):
            mandante_id = st.selectbox(
                "Mandante",
                mand["id"].tolist(),
                format_func=lambda x: mand[mand["id"] == x].iloc[0]["nombre"],
            )
            nombre = st.text_input("Nombre contrato de faena", placeholder="Contrato Faena Bellavista")
            fi = st.date_input("Fecha inicio (opcional)", value=None)
            ft = st.date_input("Fecha término (opcional)", value=None)
            archivo = st.file_uploader("Archivo contrato (opcional)", key="up_contrato_faena", type=None)
            render_upload_help()
            ok = st.form_submit_button("Guardar contrato de faena", type="primary")

        if ok:
            if not nombre.strip():
                st.error("Debes ingresar un nombre para el contrato de faena.")
                st.stop()
            try:
                file_path = None
                bucket = None
                object_path = None
                sha = None
                created_at = datetime.utcnow().isoformat(timespec="seconds")
                if archivo is not None:
                    payload = prepare_upload_payload(
                        archivo.name,
                        archivo.getvalue(),
                        getattr(archivo, "type", None) or "application/octet-stream",
                    )
                    file_path, bucket, object_path = save_file_online(
                        ["contratos_faena", mandante_id],
                        payload["file_name"],
                        payload["file_bytes"],
                        content_type=payload["content_type"],
                    )
                    sha = sha256_bytes(payload["file_bytes"])
                    if payload["compressed"] and payload.get("compression_note"):
                        st.info(payload["compression_note"])

                execute(
                    "INSERT INTO contratos_faena(mandante_id, nombre, fecha_inicio, fecha_termino, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        int(mandante_id),
                        nombre.strip(),
                        str(fi) if fi else None,
                        str(ft) if ft else None,
                        file_path,
                        bucket,
                        object_path,
                        sha,
                        created_at,
                    ),
                )
                st.success("Contrato de faena creado.")
                auto_backup_db("contrato_faena")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    with tab2:
        df = fetch_df(
            """
            SELECT cf.id, cf.mandante_id, m.nombre AS mandante, cf.nombre, cf.fecha_inicio, cf.fecha_termino, cf.file_path, cf.bucket, cf.object_path,
                   CASE WHEN cf.file_path IS NULL THEN '(sin archivo)' ELSE 'OK' END AS archivo
            FROM contratos_faena cf
            JOIN mandantes m ON m.id=cf.mandante_id
            ORDER BY cf.id DESC
            """
        )

        if df.empty:
            st.info("No hay contratos.")
            return

        st.markdown("### 📋 Contratos existentes")
        st.dataframe(df.drop(columns=["file_path", "mandante_id"]), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ✏️ Editar datos del contrato")

        contrato_id = st.selectbox(
            "Selecciona contrato",
            df["id"].tolist(),
            format_func=lambda x: f"{x} - {df[df['id'] == x].iloc[0]['mandante']} / {df[df['id'] == x].iloc[0]['nombre']}",
            key="sel_contrato_edit",
        )
        row = df[df["id"] == contrato_id].iloc[0]

        with st.form("form_edit_contrato"):
            mandante_id_new = st.selectbox(
                "Mandante (cambiar)",
                mand["id"].tolist(),
                index=mand["id"].tolist().index(int(row["mandante_id"])) if int(row["mandante_id"]) in mand["id"].tolist() else 0,
                format_func=lambda x: mand[mand["id"] == x].iloc[0]["nombre"],
            )
            nombre_new = st.text_input("Nombre", value=str(row["nombre"]))
            fi_new = st.date_input("Fecha inicio (opcional)", value=parse_date_maybe(row["fecha_inicio"]))
            ft_new = st.date_input("Fecha término (opcional)", value=parse_date_maybe(row["fecha_termino"]))
            upd = st.form_submit_button("Guardar cambios", type="primary")

        if upd:
            if not nombre_new.strip():
                st.error("El nombre no puede estar vacío.")
                st.stop()
            try:
                execute(
                    "UPDATE contratos_faena SET mandante_id=?, nombre=?, fecha_inicio=?, fecha_termino=? WHERE id=?",
                    (int(mandante_id_new), nombre_new.strip(), str(fi_new) if fi_new else None, str(ft_new) if ft_new else None, int(contrato_id)),
                )
                st.success("Contrato actualizado.")
                auto_backup_db("contrato_edit")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar: {e}")

        st.divider()
        st.markdown("### 📎 Archivo del contrato")

        up = st.file_uploader("Subir / reemplazar archivo", key="up_contrato_existente", type=None)
        render_upload_help()
        cfa1, cfa2 = st.columns([1, 1])
        with cfa1:
            if st.button("Guardar archivo", type="primary", use_container_width=True):
                if up is None:
                    st.error("Debes subir un archivo primero.")
                    st.stop()
                payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, "type", None) or "application/octet-stream")
                file_path, bucket, object_path = save_file_online(
                    ["contratos_faena", "id", contrato_id],
                    payload["file_name"],
                    payload["file_bytes"],
                    content_type=payload["content_type"],
                )
                sha = sha256_bytes(payload["file_bytes"])
                if payload["compressed"] and payload.get("compression_note"):
                    st.info(payload["compression_note"])
                execute(
                    "UPDATE contratos_faena SET file_path=?, bucket=?, object_path=?, sha256=?, created_at=? WHERE id=?",
                    (file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(contrato_id)),
                )
                st.success("Archivo actualizado.")
                auto_backup_db("contrato_archivo")
                st.rerun()
        with cfa2:
            current_path = row.get("file_path")
            current_bucket = row.get("bucket", None)
            current_object = row.get("object_path", None)
            try:
                bcur = load_file_anywhere(str(current_path) if current_path else None, current_bucket, current_object)
                st.download_button(
                    "Descargar archivo actual",
                    data=bcur,
                    file_name=(str(current_path).split("/")[-1] if current_path else "contrato"),
                    mime="application/octet-stream",
                    use_container_width=True,
                )
            except Exception:
                st.button("Descargar archivo actual", disabled=True, use_container_width=True)

        st.divider()
        st.markdown("### 🗑️ Eliminar contrato")
        st.caption("Si este contrato está asociado a faenas existentes, al eliminarlo esas faenas quedarán con contrato en blanco (contrato_faena_id = NULL).")

        dep = fetch_df("SELECT COUNT(*) AS n FROM faenas WHERE contrato_faena_id=?", (int(contrato_id),))
        dep_n = int(dep["n"].iloc[0]) if not dep.empty else 0

        st.warning(f"Faenas asociadas a este contrato: {dep_n}")

        confirm = st.checkbox("Confirmo que deseo eliminar este contrato", key="chk_del_contrato")
        if st.button("Eliminar contrato definitivamente", type="secondary"):
            if not confirm:
                st.error("Debes confirmar el checkbox antes de eliminar.")
                st.stop()
            try:
                refs = fetch_file_refs("contratos_faena", "id=?", (int(contrato_id),))
                execute("UPDATE faenas SET contrato_faena_id=NULL WHERE contrato_faena_id=?", (int(contrato_id),))
                execute("DELETE FROM contratos_faena WHERE id=?", (int(contrato_id),))
                cleanup_issues = cleanup_deleted_file_refs(refs)
                if cleanup_issues:
                    st.warning("Contrato eliminado, pero hubo problemas al limpiar el archivo asociado: " + " | ".join(cleanup_issues))
                else:
                    st.success("Contrato eliminado.")
                auto_backup_db("contrato_delete")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo eliminar: {e}")


def page_faenas(
    *,
    fetch_df,
    execute,
    auto_backup_db,
    render_upload_help,
    prepare_upload_payload,
    save_file_online,
    sha256_bytes,
    validate_faena_dates,
    faena_progress_table,
    parse_date_maybe,
    fetch_file_refs,
    cleanup_deleted_file_refs,
    ESTADOS_FAENA=None,
    pendientes_obligatorios=None,
    **_ignored,
):
    if ESTADOS_FAENA is None:
        ESTADOS_FAENA = ["ACTIVA", "TERMINADA"]
    ui_header("Faenas", "Crea, edita y gestiona faenas por mandante. Registra fechas/estado y carga anexos si aplica.")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        ui_tip("Primero crea un mandante.")
        return

    contratos = fetch_df(
        """
        SELECT cf.id, cf.nombre, cf.mandante_id, m.nombre AS mandante
        FROM contratos_faena cf
        JOIN mandantes m ON m.id=cf.mandante_id
        ORDER BY m.nombre, cf.nombre
        """
    )

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["➕ Crear faena", "📋 Listado (semáforo)", "📎 Anexos", "✏️ Editar / Eliminar", "🔒 Faenas Cerradas"])

    with tab1:
        mandante_id = st.selectbox(
            "Mandante",
            mand["id"].tolist(),
            format_func=lambda x: mand[mand["id"] == x].iloc[0]["nombre"],
            key="faena_mandante_sel",
        )

        contratos_m = contratos[contratos["mandante_id"] == mandante_id] if not contratos.empty else pd.DataFrame()
        contrato_opts = [None] + (contratos_m["id"].tolist() if not contratos_m.empty else [])

        def _fmt_contrato(x):
            if x is None:
                return "(sin contrato asociado)"
            row = contratos[contratos["id"] == x]
            if row.empty:
                return str(x)
            return f"{int(x)} - {row.iloc[0]['nombre']}"

        with st.form("form_faena"):
            contrato_id = st.selectbox("Contrato de faena (opcional)", contrato_opts, format_func=_fmt_contrato)
            nombre = st.text_input("Nombre faena", placeholder="Bellavista 3")
            ubicacion = st.text_input("Ubicación", placeholder="Predio / Comuna")
            fi = st.date_input("Fecha inicio", value=date.today())
            ft = st.date_input("Fecha término (opcional)", value=None)
            estado = st.selectbox("Estado", ESTADOS_FAENA, index=0)

            errors = validate_faena_dates(fi, ft, estado)
            if errors:
                st.warning("Revisar: " + " | ".join(errors))

            ok = st.form_submit_button("Guardar faena", type="primary")

        if ok:
            if not nombre.strip():
                st.error("Debes ingresar un nombre para la faena.")
                st.stop()
            if errors:
                st.error("Corrige las fechas/estado antes de guardar la faena.")
                st.stop()
            try:
                execute(
                    "INSERT INTO faenas(mandante_id, contrato_faena_id, nombre, ubicacion, fecha_inicio, fecha_termino, estado) VALUES(?,?,?,?,?,?,?)",
                    (
                        int(mandante_id),
                        int(contrato_id) if contrato_id else None,
                        nombre.strip(),
                        ubicacion.strip(),
                        str(fi),
                        str(ft) if ft else None,
                        estado,
                    ),
                )
                st.success("Faena creada.")
                auto_backup_db("faena")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    with tab2:
        df = faena_progress_table()
        if df.empty:
            st.info("No hay faenas aún.")
        else:
            out = df.copy()

            def _semaforo(r):
                try:
                    tr = int(r.get("trabajadores", 0) or 0)
                    pct = float(r.get("cobertura_docs_pct", 0) or 0)
                    falt = int(r.get("faltantes_total", 0) or 0)
                except Exception:
                    tr, pct, falt = 0, 0, 0

                if tr == 0:
                    return "🔴 CRÍTICO"
                if falt == 0 and pct >= 100:
                    return "🟢 OK"
                if pct >= 70:
                    return "🟡 PENDIENTE"
                return "🔴 CRÍTICO"

            out["estado_docs"] = out.apply(_semaforo, axis=1)
            out["cobertura_%"] = out["cobertura_docs_pct"].round(0).astype(int)

            show = out.rename(columns={"faena_id": "id", "faena": "faena_nombre"})
            show = show[["estado_docs", "id", "mandante", "faena_nombre", "estado", "fecha_inicio", "fecha_termino", "trabajadores", "trab_ok", "cobertura_%", "faltantes_total"]].copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.caption("Regla semáforo: 🔴 sin trabajadores o cobertura <70% | 🟡 ≥70% con faltantes | 🟢 100% sin faltantes.")

            colq1, colq2, colq3 = st.columns([2, 1, 1])
            with colq1:
                fid = st.selectbox(
                    "Acción rápida: seleccionar faena",
                    show["id"].tolist(),
                    format_func=lambda x: f"{int(x)} - {show[show['id'] == x].iloc[0]['mandante']} / {show[show['id'] == x].iloc[0]['faena_nombre']}",
                )
            with colq2:
                if st.button("Ir a Docs", use_container_width=True):
                    st.session_state["selected_faena_id"] = int(fid)
                    st.session_state["nav_page"] = "Documentos Trabajador"
                    st.rerun()
            with colq3:
                if st.button("Ir a Export", type="primary", use_container_width=True):
                    st.session_state["selected_faena_id"] = int(fid)
                    st.session_state["nav_page"] = "Export (ZIP)"
                    st.rerun()

    with tab3:
        base = fetch_df(
            """
            SELECT f.id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio, f.fecha_termino, f.ubicacion,
                   COALESCE(cf.nombre, '') AS contrato_faena
            FROM faenas f
            JOIN mandantes m ON m.id=f.mandante_id
            LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
            ORDER BY f.id DESC
            """
        )

        if base.empty:
            st.info("No hay faenas.")
            return

        faena_id = st.selectbox(
            "Faena",
            base["id"].tolist(),
            format_func=lambda x: f"{x} - {base[base['id'] == x].iloc[0]['mandante']} / {base[base['id'] == x].iloc[0]['nombre']}",
        )
        st.session_state["selected_faena_id"] = int(faena_id)

        st.markdown("### Subir anexo")
        up = st.file_uploader("Archivo anexo", key="up_anexo_faena", type=None)
        render_upload_help()
        if st.button("Guardar anexo", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, "type", None) or "application/octet-stream")
            file_path, bucket, object_path = save_file_online(
                ["faenas", faena_id, "anexos"],
                payload["file_name"],
                payload["file_bytes"],
                content_type=payload["content_type"],
            )
            sha = sha256_bytes(payload["file_bytes"])
            execute(
                "INSERT INTO faena_anexos(faena_id, nombre, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?)",
                (int(faena_id), payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])
            st.success("Anexo guardado.")
            auto_backup_db("anexo_faena")
            st.rerun()

        anexos = fetch_df("SELECT id, nombre, created_at FROM faena_anexos WHERE faena_id=? ORDER BY id DESC", (int(faena_id),))
        st.caption("Anexos cargados")
        st.dataframe(anexos if not anexos.empty else pd.DataFrame([{"info": "(sin anexos)"}]), use_container_width=True)

    with tab4:
        base = fetch_df(
            """
            SELECT f.id, f.mandante_id, m.nombre AS mandante, f.nombre, f.ubicacion, f.fecha_inicio, f.fecha_termino, f.estado,
                   f.contrato_faena_id, COALESCE(cf.nombre,'') AS contrato_nombre
            FROM faenas f
            JOIN mandantes m ON m.id=f.mandante_id
            LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
            ORDER BY f.id DESC
            """
        )
        if base.empty:
            st.info("No hay faenas para editar.")
            return

        fid = st.selectbox(
            "Selecciona faena",
            base["id"].tolist(),
            format_func=lambda x: f"{int(x)} - {base[base['id'] == x].iloc[0]['mandante']} / {base[base['id'] == x].iloc[0]['nombre']} ({base[base['id'] == x].iloc[0]['estado']})",
            key="faena_edit_sel",
        )
        st.session_state["selected_faena_id"] = int(fid)
        row = base[base["id"] == int(fid)].iloc[0]

        contratos_m = contratos[contratos["mandante_id"] == int(row["mandante_id"])] if not contratos.empty else pd.DataFrame()
        contrato_opts = [None] + (contratos_m["id"].tolist() if not contratos_m.empty else [])

        def _fmt_contrato_edit_faena(x):
            if x is None:
                return "(sin contrato asociado)"
            r2 = contratos_m[contratos_m["id"] == x]
            if r2.empty:
                return str(x)
            return f"{int(x)} - {r2.iloc[0]['nombre']}"

        default_c = None if pd.isna(row["contrato_faena_id"]) else int(row["contrato_faena_id"])
        contrato_index = contrato_opts.index(default_c) if default_c in contrato_opts else 0

        st.markdown("### ✏️ Editar faena")
        with st.form("form_edit_faena"):
            nombre_new = st.text_input("Nombre", value=str(row["nombre"] or ""))
            ubic_new = st.text_input("Ubicación", value=str(row["ubicacion"] or ""))
            fi_new = st.date_input("Fecha inicio", value=parse_date_maybe(row["fecha_inicio"]) or date.today())
            ft_new = st.date_input("Fecha término (opcional)", value=parse_date_maybe(row["fecha_termino"]))
            estado_new = st.selectbox("Estado", ESTADOS_FAENA, index=ESTADOS_FAENA.index(str(row["estado"])) if str(row["estado"]) in ESTADOS_FAENA else 0)
            contrato_new = st.selectbox("Contrato de faena (opcional)", contrato_opts, index=contrato_index, format_func=_fmt_contrato_edit_faena)

            errors = validate_faena_dates(fi_new, ft_new, estado_new)
            if errors:
                st.warning("Revisar: " + " | ".join(errors))

            ok_upd = st.form_submit_button("Guardar cambios", type="primary")

        if ok_upd:
            if not nombre_new.strip():
                st.error("El nombre no puede estar vacío.")
                st.stop()
            if errors:
                st.error("Corrige las fechas/estado antes de guardar.")
                st.stop()
            try:
                execute(
                    "UPDATE faenas SET nombre=?, ubicacion=?, fecha_inicio=?, fecha_termino=?, estado=?, contrato_faena_id=? WHERE id=?",
                    (
                        nombre_new.strip(),
                        ubic_new.strip(),
                        str(fi_new),
                        str(ft_new) if ft_new else None,
                        estado_new,
                        int(contrato_new) if contrato_new else None,
                        int(fid),
                    ),
                )
                st.success("Faena actualizada.")
                auto_backup_db("faena_edit")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar: {e}")

        st.divider()
        st.markdown("### 🗑️ Eliminar faena")
        st.caption("Se eliminará la faena y sus anexos/asignaciones asociadas. Los trabajadores NO se eliminan.")

        dep1 = fetch_df("SELECT COUNT(*) AS n FROM asignaciones WHERE faena_id=?", (int(fid),))
        dep2 = fetch_df("SELECT COUNT(*) AS n FROM faena_anexos WHERE faena_id=?", (int(fid),))
        n_asg = int(dep1["n"].iloc[0]) if not dep1.empty else 0
        n_anx = int(dep2["n"].iloc[0]) if not dep2.empty else 0

        st.warning(f"Dependencias: {n_asg} asignaciones · {n_anx} anexos")

        confirm = st.checkbox("Confirmo que deseo eliminar esta faena", key="chk_del_faena")
        if st.button("Eliminar faena definitivamente", type="secondary"):
            if not confirm:
                st.error("Debes confirmar el checkbox antes de eliminar.")
                st.stop()
            try:
                refs = []
                refs.extend(fetch_file_refs("faena_anexos", "faena_id=?", (int(fid),)))
                refs.extend(fetch_file_refs("faena_empresa_documentos", "faena_id=?", (int(fid),)))
                execute("DELETE FROM faena_anexos WHERE faena_id=?", (int(fid),))
                execute("DELETE FROM faena_empresa_documentos WHERE faena_id=?", (int(fid),))
                execute("DELETE FROM asignaciones WHERE faena_id=?", (int(fid),))
                execute("DELETE FROM faenas WHERE id=?", (int(fid),))
                cleanup_issues = cleanup_deleted_file_refs(refs)
                if cleanup_issues:
                    st.warning("Faena eliminada, pero hubo problemas al limpiar archivos asociados: " + " | ".join(cleanup_issues))
                else:
                    st.success("Faena eliminada.")
                auto_backup_db("faena_delete")
                st.session_state["selected_faena_id"] = None
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo eliminar: {e}")

    # ── TAB 5: FAENAS CERRADAS ─────────────────────────────────────────────
    with tab5:
        import re as _re
        st.markdown("### 🔒 Faenas Cerradas — Historial y descarga de documentos")
        st.caption("Aquí aparecen las faenas con estado TERMINADA. Puedes descargar el ZIP completo con todos sus documentos.")

        closed = fetch_df("""
            SELECT f.id, m.nombre AS mandante, f.nombre, f.fecha_inicio, f.fecha_termino, f.ubicacion,
                   (SELECT COUNT(*) FROM asignaciones a WHERE a.faena_id=f.id) AS trabajadores,
                   (SELECT COUNT(*) FROM trabajador_documentos td
                        JOIN asignaciones a2 ON a2.trabajador_id=td.trabajador_id
                        WHERE a2.faena_id=f.id) AS docs_trab,
                   (SELECT COUNT(*) FROM faena_empresa_documentos fed WHERE fed.faena_id=f.id) AS docs_emp,
                   (SELECT COUNT(*) FROM faena_anexos fa WHERE fa.faena_id=f.id) AS anexos
            FROM faenas f
            JOIN mandantes m ON m.id=f.mandante_id
            WHERE f.estado='TERMINADA'
            ORDER BY f.fecha_termino DESC, f.id DESC
        """)

        if closed is None or closed.empty:
            st.info("No hay faenas cerradas aún. Cuando una faena pase a estado TERMINADA aparecerá aquí.")
        else:
            q_c = st.text_input("🔍 Filtrar", key="closed_q", placeholder="Mandante o nombre de faena…")
            view = closed.copy()
            if q_c.strip():
                qq = q_c.strip().lower()
                mask = (view["mandante"].astype(str).str.lower().str.contains(qq, na=False) |
                        view["nombre"].astype(str).str.lower().str.contains(qq, na=False))
                view = view[mask]

            display = view.rename(columns={
                "mandante":"Mandante","nombre":"Faena",
                "fecha_inicio":"Inicio","fecha_termino":"Término",
                "trabajadores":"Trabajadores","docs_trab":"Docs Trab.",
                "docs_emp":"Docs Empresa","anexos":"Anexos",
            })[["id","Mandante","Faena","Inicio","Término","Trabajadores","Docs Trab.","Docs Empresa","Anexos"]]
            st.dataframe(display, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### 📦 Descargar ZIP de faena cerrada")

            fid_opts = view["id"].tolist()
            if fid_opts:
                fid_sel = st.selectbox(
                    "Selecciona faena",
                    fid_opts,
                    format_func=lambda x: (
                        f"{int(x)} — {view[view['id']==x].iloc[0]['mandante']} / "
                        f"{view[view['id']==x].iloc[0]['nombre']} "
                        f"({view[view['id']==x].iloc[0].get('fecha_termino','') or 'sin fecha'})"
                    ),
                    key="closed_fid_sel",
                )
                row_c = view[view["id"] == fid_sel].iloc[0]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Trabajadores", int(row_c["trabajadores"]))
                c2.metric("Docs trabajadores", int(row_c["docs_trab"]))
                c3.metric("Docs empresa", int(row_c["docs_emp"]))
                c4.metric("Anexos", int(row_c["anexos"]))

                if st.button("📦 Generar ZIP", type="primary", use_container_width=True, key="btn_zip_closed"):
                    with st.spinner("Generando ZIP de la faena…"):
                        try:
                            nombre_faena = str(row_c["nombre"])
                            zip_bytes, added = _build_faena_zip(int(fid_sel), nombre_faena, fetch_df)
                            safe = _re.sub(r"[^a-zA-Z0-9_-]", "_", nombre_faena)[:30]
                            zip_name = f"faena_cerrada_{int(fid_sel)}_{safe}.zip"
                            if added == 0:
                                st.warning("Esta faena no tiene documentos locales. Si usas Supabase Storage, los archivos están en la nube.")
                            else:
                                st.success(f"ZIP generado con {added} archivo(s).")
                            st.download_button(
                                f"⬇️ Descargar {zip_name}",
                                data=zip_bytes, file_name=zip_name,
                                mime="application/zip",
                                use_container_width=True, key="dl_closed_zip",
                            )
                        except Exception as e:
                            st.error(f"No se pudo generar el ZIP: {e}")


def _build_faena_zip(faena_id: int, faena_nombre: str, fetch_df) -> bytes:
    """Genera un ZIP con todos los documentos de una faena cerrada."""
    import io, zipfile, os

    mem = io.BytesIO()
    added = 0

    def _read(file_path, bucket, object_path):
        if file_path and os.path.exists(str(file_path)):
            with open(str(file_path), "rb") as f:
                return f.read()
        return None

    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        # Contrato
        cont = fetch_df("SELECT file_path, nombre FROM contratos_faena WHERE id=(SELECT contrato_faena_id FROM faenas WHERE id=?)", (int(faena_id),))
        if cont is not None and not cont.empty:
            r = cont.iloc[0]
            b = _read(r.get("file_path"), None, None)
            if b:
                zf.writestr(f"Contrato/{os.path.basename(str(r.get('file_path','contrato')))}",b)
                added += 1

        # Anexos
        anx = fetch_df("SELECT nombre, file_path FROM faena_anexos WHERE faena_id=? ORDER BY id", (int(faena_id),))
        if anx is not None and not anx.empty:
            for _, r in anx.iterrows():
                b = _read(r.get("file_path"), None, None)
                if b:
                    zf.writestr(f"Anexos/{os.path.basename(str(r.get('file_path','anexo')))}",b)
                    added += 1

        # Docs empresa faena
        emp = fetch_df("SELECT doc_tipo, nombre_archivo, file_path FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo,id", (int(faena_id),))
        if emp is not None and not emp.empty:
            for _, r in emp.iterrows():
                b = _read(r.get("file_path"), None, None)
                if b:
                    fname = str(r.get("nombre_archivo") or r.get("file_path") or "doc")
                    zf.writestr(f"Docs_Empresa/{os.path.basename(fname)}",b)
                    added += 1

        # Docs trabajadores
        trab = fetch_df("""
            SELECT t.rut, t.apellidos||' '||t.nombres AS nombre
            FROM asignaciones a JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.faena_id=? ORDER BY t.apellidos,t.nombres
        """, (int(faena_id),))
        if trab is not None and not trab.empty:
            for _, tr in trab.iterrows():
                docs = fetch_df("SELECT doc_tipo, nombre_archivo, file_path FROM trabajador_documentos WHERE trabajador_id=(SELECT id FROM trabajadores WHERE rut=?) ORDER BY doc_tipo,id", (str(tr["rut"]),))
                if docs is None or docs.empty:
                    continue
                folder = str(tr["nombre"])[:35].replace("/","_")
                for _, dr in docs.iterrows():
                    b = _read(dr.get("file_path"), None, None)
                    if b:
                        fname = str(dr.get("nombre_archivo") or dr.get("file_path") or "doc")
                        zf.writestr(f"Trabajadores/{folder}/{os.path.basename(fname)}",b)
                        added += 1

    return mem.getvalue(), added
