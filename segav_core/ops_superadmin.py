from __future__ import annotations

from datetime import datetime

import pandas as pd


def _dep_count(fetch_value, table: str, cliente_key: str) -> int:
    try:
        return int(fetch_value(f"SELECT COUNT(*) FROM {table} WHERE COALESCE(cliente_key,'')=?", (cliente_key,), default=0) or 0)
    except Exception:
        return 0


def _delete_client_records(fetch_value, execute, fetch_file_refs, cleanup_deleted_file_refs, cliente_key: str):
    file_tables = [
        'contratos_faena', 'faena_anexos', 'trabajador_documentos', 'empresa_documentos',
        'faena_empresa_documentos', 'export_historial', 'export_historial_mes'
    ]
    refs = []
    for table in file_tables:
        try:
            refs.extend(fetch_file_refs(table, "COALESCE(cliente_key,'')=?", (cliente_key,)))
        except Exception:
            pass
    delete_order = [
        'sgsst_alertas', 'sgsst_capacitaciones', 'sgsst_incidentes', 'sgsst_inspecciones', 'sgsst_miper',
        'sgsst_programa_anual', 'sgsst_matriz_legal', 'sgsst_empresa', 'sgsst_auditoria',
        'faena_empresa_documentos', 'trabajador_documentos', 'empresa_documentos', 'faena_anexos',
        'asignaciones', 'trabajadores', 'faenas', 'contratos_faena', 'mandantes', 'export_historial', 'export_historial_mes'
    ]
    deleted = {}
    for table in delete_order:
        try:
            count = int(fetch_value(f"SELECT COUNT(*) FROM {table} WHERE COALESCE(cliente_key,'')=?", (cliente_key,), default=0) or 0)
            execute(f"DELETE FROM {table} WHERE COALESCE(cliente_key,'')=?", (cliente_key,))
        except Exception:
            count = 0
        deleted[table] = count
    cleanup_issues = []
    try:
        cleanup_issues = cleanup_deleted_file_refs(refs)
    except Exception as exc:
        cleanup_issues = [str(exc)]
    return deleted, cleanup_issues


def page_superadmin_empresas(*, st, ui_header, fetch_df, fetch_value, execute, clear_app_caches, segav_clientes_df, visible_clientes_df, current_segav_client_key, make_erp_key, clean_rut, ERP_CLIENT_PARAM_DEFAULTS, set_segav_erp_config_value, sgsst_log, current_user, is_superadmin, ensure_user_client_access_table, fetch_file_refs, cleanup_deleted_file_refs, set_active_cliente_key):
    if not is_superadmin():
        st.error("Esta sección es exclusiva para SUPERADMIN.")
        st.stop()

    ensure_user_client_access_table()
    ui_header("SuperAdmin / Empresas", "Panel exclusivo para controlar todas las empresas del ERP, sus administradores y el estado global de adopción.")

    cli_df = segav_clientes_df()
    cli_df = cli_df.copy() if cli_df is not None else pd.DataFrame()
    users_df = fetch_df("SELECT id, username, role, is_active FROM users ORDER BY is_active DESC, username")
    access_df = fetch_df(
        """
        SELECT a.user_id, a.cliente_key, COALESCE(a.is_company_admin,0) AS is_company_admin,
               u.username, u.role, u.is_active
          FROM user_client_access a
          LEFT JOIN users u ON u.id=a.user_id
         ORDER BY a.cliente_key, COALESCE(a.is_company_admin,0) DESC, u.username
        """
    )

    total_empresas = int(len(cli_df)) if cli_df is not None else 0
    activas = int(cli_df["activo"].fillna(1).astype(int).sum()) if cli_df is not None and not cli_df.empty and "activo" in cli_df.columns else total_empresas
    admins_asignados = int(access_df[access_df["is_company_admin"].fillna(0).astype(int) == 1][["cliente_key", "user_id"]].drop_duplicates().shape[0]) if access_df is not None and not access_df.empty else 0
    usuarios_asignados = int(access_df[["cliente_key", "user_id"]].drop_duplicates().shape[0]) if access_df is not None and not access_df.empty else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Empresas registradas", total_empresas)
    m2.metric("Empresas activas", activas)
    m3.metric("Admins asignados", admins_asignados)
    m4.metric("Usuarios vinculados", usuarios_asignados)

    tabs = st.tabs(["🌐 Dashboard", "🏢 Empresas", "👥 Administradores"])

    with tabs[0]:
        st.markdown("### Vista global de empresas")
        rows = []
        if cli_df is not None and not cli_df.empty:
            for _, row in cli_df.iterrows():
                ckey = str(row.get("cliente_key") or "")
                admins = 0
                if access_df is not None and not access_df.empty:
                    admins = int(access_df[(access_df["cliente_key"].astype(str) == ckey) & (access_df["is_company_admin"].fillna(0).astype(int) == 1)]["user_id"].nunique())
                rows.append({
                    "Código": ckey,
                    "Empresa": str(row.get("cliente_nombre") or ""),
                    "RUT": str(row.get("rut") or ""),
                    "Rubro": str(row.get("vertical") or ""),
                    "Tipo de inicio": str(row.get("modo_implementacion") or ""),
                    "Activa": "SI" if int(row.get("activo") or 0) == 1 else "NO",
                    "Faenas": _dep_count(fetch_value, "faenas", ckey),
                    "Trabajadores": _dep_count(fetch_value, "trabajadores", ckey),
                    "Docs Empresa": _dep_count(fetch_value, "empresa_documentos", ckey),
                    "Admins": admins,
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("Aún no hay empresas registradas en el catálogo multiempresa.")

        st.markdown("### Empresa activa de esta sesión")
        vis_df = visible_clientes_df()
        if vis_df is not None and not vis_df.empty:
            active_key = current_segav_client_key()
            row = vis_df[vis_df["cliente_key"].astype(str) == str(active_key)]
            if row.empty:
                row = vis_df.iloc[[0]]
            active = row.iloc[0].to_dict()
            st.success(f"Empresa activa en esta sesión: {active.get('cliente_nombre')} · {active.get('vertical') or 'Sin vertical'}")
        else:
            st.warning("No hay empresas visibles en esta sesión.")

    with tabs[1]:
        st.markdown("### Crear empresa")
        c1, c2, c3 = st.columns(3)
        new_name = c1.text_input("Nombre empresa", key="sa_new_empresa_nombre")
        new_rut = c2.text_input("RUT empresa", key="sa_new_empresa_rut")
        new_vertical = c3.text_input("Rubro", key="sa_new_empresa_vertical", help="Define el rubro o sector de la empresa, por ejemplo forestal, construcción, transporte o servicios.")
        c4, c5, c6 = st.columns(3)
        new_impl = c4.selectbox("Tipo de inicio", ["Desde cero", "Configuración base", "Demo / Prueba"], key="sa_new_empresa_impl", help="Define cómo comenzará la empresa dentro del ERP. Desde cero crea la empresa vacía; Configuración base agrega parámetros sugeridos sin datos reales; Demo / Prueba deja preparada una empresa para mostrar el sistema.")
        new_contact = c5.text_input("Contacto", key="sa_new_empresa_contacto")
        new_email = c6.text_input("Email", key="sa_new_empresa_email")
        new_obs = st.text_area("Observaciones", key="sa_new_empresa_obs", height=80)
        st.caption("Rubro: sector o giro de la empresa. Tipo de inicio: define si parte vacía, con configuración base o como entorno demo.")
        if st.button("Crear empresa", type="primary", key="sa_btn_crear_empresa"):
            if not new_name.strip():
                st.error("Debes indicar el nombre de la empresa.")
            else:
                now = datetime.now().isoformat(timespec='seconds')
                cliente_key = make_erp_key(new_name, prefix='cli_')
                exists = int(fetch_value("SELECT COUNT(*) FROM segav_erp_clientes WHERE cliente_key=?", (cliente_key,), default=0) or 0)
                if exists > 0:
                    st.error("Ya existe una empresa con ese código. Cambia el nombre o edítala abajo.")
                else:
                    execute(
                        "INSERT INTO segav_erp_clientes(cliente_key, cliente_nombre, rut, vertical, modo_implementacion, activo, contacto, email, observaciones, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (cliente_key, new_name.strip(), clean_rut(new_rut), new_vertical.strip() or "General", new_impl, 1, new_contact.strip(), new_email.strip(), new_obs.strip(), now, now),
                    )
                    for param_key, param_value in ERP_CLIENT_PARAM_DEFAULTS.items():
                        execute(
                            "INSERT INTO segav_erp_parametros_cliente(cliente_key, param_key, param_value, updated_at) VALUES(?,?,?,?)",
                            (cliente_key, param_key, str(param_value), now),
                        )
                    set_active_cliente_key(cliente_key)
                    clear_app_caches()
                    sgsst_log("SuperAdmin / Empresas", "Crear empresa", f"{new_name.strip()} ({cliente_key})")
                    st.success("Empresa creada correctamente. Parte sin registros ni datos cargados.")
                    st.rerun()

        st.markdown("---")
        st.markdown("### Modificar o desactivar empresa")
        cli_keys = cli_df["cliente_key"].astype(str).tolist() if cli_df is not None and not cli_df.empty else []
        if cli_keys:
            edit_key = st.selectbox("Empresa a modificar", cli_keys, key="sa_edit_empresa", format_func=lambda x: str(cli_df[cli_df['cliente_key'].astype(str)==str(x)].iloc[0]['cliente_nombre']))
            row = cli_df[cli_df["cliente_key"].astype(str) == str(edit_key)].iloc[0].to_dict()
            e1, e2, e3 = st.columns(3)
            edit_name = e1.text_input("Nombre empresa", value=str(row.get("cliente_nombre") or ""), key="sa_edit_nombre")
            edit_rut = e2.text_input("RUT", value=str(row.get("rut") or ""), key="sa_edit_rut")
            edit_vertical = e3.text_input("Rubro", value=str(row.get("vertical") or ""), key="sa_edit_vertical", help="Rubro o sector principal de la empresa.")
            e4, e5, e6 = st.columns(3)
            impl_options = ["Desde cero", "Configuración base", "Demo / Prueba"]
            current_impl = str(row.get("modo_implementacion") or "CONFIGURABLE")
            if current_impl not in impl_options:
                impl_options.append(current_impl)
            edit_impl = e4.selectbox("Tipo de inicio", impl_options, index=impl_options.index(current_impl), key="sa_edit_impl", help="Forma en que esta empresa fue o será iniciada dentro del ERP.")
            edit_contact = e5.text_input("Contacto", value=str(row.get("contacto") or ""), key="sa_edit_contacto")
            edit_email = e6.text_input("Email", value=str(row.get("email") or ""), key="sa_edit_email")
            edit_obs = st.text_area("Observaciones", value=str(row.get("observaciones") or ""), key="sa_edit_obs", height=80)
            active_now = int(row.get("activo") or 0) == 1
            active_flag = st.selectbox("Estado", ["ACTIVA", "INACTIVA"], index=0 if active_now else 1, key="sa_edit_activa")
            st.caption("Rubro: sector de la empresa. Tipo de inicio: cómo fue preparada esta empresa dentro del ERP.")
            col_a, col_b = st.columns([1,1])
            with col_a:
                if st.button("Guardar cambios de la empresa", key="sa_btn_guardar_empresa"):
                    now = datetime.now().isoformat(timespec='seconds')
                    execute(
                        "UPDATE segav_erp_clientes SET cliente_nombre=?, rut=?, vertical=?, modo_implementacion=?, activo=?, contacto=?, email=?, observaciones=?, updated_at=? WHERE cliente_key=?",
                        (edit_name.strip() or row.get("cliente_nombre") or edit_key, clean_rut(edit_rut), edit_vertical.strip() or "General", edit_impl, 1 if active_flag == "ACTIVA" else 0, edit_contact.strip(), edit_email.strip(), edit_obs.strip(), now, edit_key),
                    )
                    clear_app_caches()
                    sgsst_log("SuperAdmin / Empresas", "Modificar empresa", edit_key)
                    st.success("Empresa actualizada.")
                    st.rerun()
            with col_b:
                if st.button("Usar como empresa activa de esta sesión", key="sa_btn_activar_sesion"):
                    st.session_state["active_cliente_key"] = edit_key
                    clear_app_caches()
                    st.success("Empresa activa de esta sesión actualizada.")
                    st.rerun()

            st.markdown("### Limpieza de registros")
            st.caption("Usa esta acción para dejar la empresa en blanco sin eliminarla del catálogo. Se borran los registros operativos y SGSST asociados a esa empresa.")
            clear_confirm = st.checkbox("Confirmo que quiero limpiar todos los registros de esta empresa", key="sa_confirm_clear_empresa")
            if st.button("Limpiar registros de esta empresa", key="sa_btn_clear_empresa"):
                if not clear_confirm:
                    st.error("Debes confirmar la limpieza total de registros.")
                else:
                    deleted_counts, cleanup_issues = _delete_client_records(fetch_value, execute, fetch_file_refs, cleanup_deleted_file_refs, edit_key)
                    clear_app_caches()
                    sgsst_log("SuperAdmin / Empresas", "Limpiar registros de esta empresa", edit_key)
                    total_deleted = sum(int(v or 0) for v in deleted_counts.values())
                    if cleanup_issues:
                        st.warning(f"Registros eliminados: {total_deleted}. Hubo observaciones al limpiar archivos: {' | '.join(cleanup_issues)}")
                    else:
                        st.success(f"Empresa limpiada correctamente. Registros eliminados: {total_deleted}. La empresa queda en blanco para comenzar desde cero.")
                    st.rerun()

            st.markdown("### Eliminación controlada")
            dep_tables = [
                "mandantes", "contratos_faena", "faenas", "trabajadores", "asignaciones",
                "trabajador_documentos", "empresa_documentos", "faena_empresa_documentos",
                "export_historial", "export_historial_mes", "sgsst_empresa", "sgsst_matriz_legal",
                "sgsst_programa_anual", "sgsst_miper", "sgsst_inspecciones", "sgsst_incidentes",
                "sgsst_capacitaciones", "sgsst_alertas",
            ]
            dep_total = sum(_dep_count(fetch_value, tbl, edit_key) for tbl in dep_tables)
            st.caption(f"Registros dependientes detectados para esta empresa: {dep_total}")
            confirm_delete = st.checkbox("Confirmo que quiero eliminar la empresa del catálogo", key="sa_confirm_delete_empresa")
            if st.button("Eliminar empresa del catálogo", key="sa_btn_delete_empresa"):
                if not confirm_delete:
                    st.error("Debes confirmar la eliminación.")
                elif dep_total > 0:
                    st.error("No se puede eliminar porque la empresa tiene datos operativos asociados. Primero desactívala o limpia sus registros.")
                else:
                    execute("DELETE FROM user_client_access WHERE cliente_key=?", (edit_key,))
                    execute("DELETE FROM segav_erp_parametros_cliente WHERE cliente_key=?", (edit_key,))
                    execute("DELETE FROM segav_erp_clientes WHERE cliente_key=?", (edit_key,))
                    clear_app_caches()
                    sgsst_log("SuperAdmin / Empresas", "Eliminar empresa", edit_key)
                    st.success("Empresa eliminada del catálogo.")
                    st.rerun()
        else:
            st.info("Aún no hay empresas para modificar.")

    with tabs[2]:
        st.markdown("### Administradores por empresa")
        if cli_df is None or cli_df.empty:
            st.info("Primero crea una empresa.")
        elif users_df is None or users_df.empty:
            st.info("No hay usuarios creados todavía. Usa Admin Usuarios para crearlos.")
        else:
            cli_keys = cli_df["cliente_key"].astype(str).tolist()
            admin_cli_key = st.selectbox("Empresa", cli_keys, key="sa_admin_empresa", format_func=lambda x: str(cli_df[cli_df['cliente_key'].astype(str)==str(x)].iloc[0]['cliente_nombre']))
            company_access = access_df[access_df["cliente_key"].astype(str) == str(admin_cli_key)].copy() if access_df is not None and not access_df.empty else pd.DataFrame(columns=["user_id","cliente_key","is_company_admin","username","role","is_active"])
            st.markdown("#### Asignaciones actuales")
            if company_access is not None and not company_access.empty:
                show = company_access[["username", "role", "is_active", "is_company_admin"]].copy()
                show["is_active"] = show["is_active"].fillna(0).astype(int).map({1:"SI", 0:"NO"})
                show["is_company_admin"] = show["is_company_admin"].fillna(0).astype(int).map({1:"SI", 0:"NO"})
                st.dataframe(show.rename(columns={"username":"Usuario", "role":"Rol global", "is_active":"Activo", "is_company_admin":"Admin empresa"}), use_container_width=True, hide_index=True)
            else:
                st.info("Esta empresa aún no tiene usuarios vinculados.")

            active_users = users_df[users_df["is_active"].fillna(1).astype(int) == 1].copy()
            user_opts = active_users["id"].astype(int).tolist()
            assigned_ids = company_access["user_id"].astype(int).tolist() if company_access is not None and not company_access.empty else []
            admin_ids_now = company_access[company_access["is_company_admin"].fillna(0).astype(int) == 1]["user_id"].astype(int).tolist() if company_access is not None and not company_access.empty else []
            selected_users = st.multiselect(
                "Usuarios con acceso a esta empresa",
                user_opts,
                default=assigned_ids,
                key="sa_company_users",
                format_func=lambda uid: f"{active_users[active_users['id'].astype(int)==int(uid)].iloc[0]['username']} · {active_users[active_users['id'].astype(int)==int(uid)].iloc[0]['role']}",
            )
            admin_candidates = selected_users or []
            selected_admins = st.multiselect(
                "Administradores de esta empresa",
                admin_candidates,
                default=[uid for uid in admin_ids_now if uid in admin_candidates],
                key="sa_company_admins",
                format_func=lambda uid: f"{active_users[active_users['id'].astype(int)==int(uid)].iloc[0]['username']} · {active_users[active_users['id'].astype(int)==int(uid)].iloc[0]['role']}",
            )
            if st.button("Guardar asignaciones de empresa", key="sa_save_company_access", type="primary"):
                now = datetime.now().isoformat(timespec='seconds')
                execute("DELETE FROM user_client_access WHERE cliente_key=?", (admin_cli_key,))
                for uid in selected_users:
                    execute(
                        "INSERT INTO user_client_access(user_id, cliente_key, is_company_admin, created_at, updated_at) VALUES(?,?,?,?,?)",
                        (int(uid), admin_cli_key, 1 if int(uid) in {int(x) for x in selected_admins} else 0, now, now),
                    )
                clear_app_caches()
                sgsst_log("SuperAdmin / Empresas", "Asignar administradores", admin_cli_key)
                st.success("Asignaciones actualizadas.")
                st.rerun()

            st.caption("Tip: usa la sección Admin Usuarios para crear cuentas nuevas y luego vuelve aquí para vincularlas a una empresa.")
