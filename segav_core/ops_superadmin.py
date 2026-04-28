from __future__ import annotations

from datetime import datetime

import pandas as pd

from segav_core.kpi_ui import kpi_card, kpi_grid, kpi_section, tone_for_count, tone_for_percentage

IMPLEMENTATION_OPTIONS = ["DESDE_CERO", "CONFIGURACION_BASE", "DEMO_PRUEBA"]
IMPLEMENTATION_LABELS = {
    "DESDE_CERO": "Desde cero (recomendado)",
    "CONFIGURACION_BASE": "Configuración base",
    "DEMO_PRUEBA": "Demo / Prueba",
    "CONFIGURABLE": "Desde cero (recomendado)",
    "VERTICAL FORESTAL": "Configuración base forestal",
    "CORPORATIVO": "Configuración corporativa",
}

def impl_label(value: str) -> str:
    return IMPLEMENTATION_LABELS.get(str(value or ""), str(value or "Sin definir"))


def _dep_count(fetch_value, table: str, cliente_key: str) -> int:
    try:
        return int(fetch_value(f"SELECT COUNT(*) FROM {table} WHERE COALESCE(cliente_key,'')=?", (cliente_key,), default=0) or 0)
    except Exception:
        return 0


def page_superadmin_empresas(*, st, ui_header, fetch_df, fetch_value, execute, clear_app_caches, segav_clientes_df, visible_clientes_df, current_segav_client_key, make_erp_key, clean_rut, ERP_CLIENT_PARAM_DEFAULTS, set_segav_erp_config_value, sgsst_log, current_user, is_superadmin, ensure_user_client_access_table, save_company_logo_for_cliente, get_company_logo_bytes):
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
               COALESCE(a.role_empresa,'OPERADOR') AS role_empresa,
               u.username, u.role, u.is_active
          FROM user_client_access a
          LEFT JOIN users u ON u.id=a.user_id
         ORDER BY a.cliente_key, COALESCE(a.is_company_admin,0) DESC, u.username
        """
    )

    total_empresas = int(len(cli_df)) if cli_df is not None else 0
    activas = int(cli_df["activo"].fillna(1).astype(int).sum()) if cli_df is not None and not cli_df.empty and "activo" in cli_df.columns else total_empresas
    inactivas = max(0, total_empresas - activas)
    admins_asignados = int(access_df[access_df["is_company_admin"].fillna(0).astype(int) == 1][["cliente_key", "user_id"]].drop_duplicates().shape[0]) if access_df is not None and not access_df.empty else 0
    usuarios_asignados = int(access_df[["cliente_key", "user_id"]].drop_duplicates().shape[0]) if access_df is not None and not access_df.empty else 0
    active_user_ids = set(users_df[users_df["is_active"].fillna(1).astype(int) == 1]["id"].astype(int).tolist()) if users_df is not None and not users_df.empty else set()
    assigned_user_ids = set(access_df["user_id"].dropna().astype(int).tolist()) if access_df is not None and not access_df.empty else set()
    usuarios_sin_empresa = len(active_user_ids - assigned_user_ids)
    empresas_con_admin = set(access_df[access_df["is_company_admin"].fillna(0).astype(int) == 1]["cliente_key"].dropna().astype(str).tolist()) if access_df is not None and not access_df.empty else set()
    empresas_activas_keys = set(cli_df[cli_df["activo"].fillna(1).astype(int) == 1]["cliente_key"].astype(str).tolist()) if cli_df is not None and not cli_df.empty and "activo" in cli_df.columns else set(cli_df["cliente_key"].astype(str).tolist()) if cli_df is not None and not cli_df.empty else set()
    empresas_sin_admin = len(empresas_activas_keys - empresas_con_admin)
    cobertura_admin = round((len(empresas_activas_keys & empresas_con_admin) / max(len(empresas_activas_keys), 1)) * 100.0, 1) if empresas_activas_keys else 0.0

    kpi_section("Centro de control SuperAdmin", "Vista global de empresas, accesos, adopción y riesgos de administración.")
    kpi_grid([
        {"label": "Empresas registradas", "value": total_empresas, "subtitle": f"{activas} activas · {inactivas} inactivas", "icon": "🏢", "tone": "info", "status": "Catálogo"},
        {"label": "Cobertura admin", "value": f"{cobertura_admin:.1f}%", "subtitle": "Empresas activas con administrador asignado", "icon": "🛡️", "tone": tone_for_percentage(cobertura_admin, danger_below=70, warning_below=90), "status": "Control", "progress": cobertura_admin},
        {"label": "Sin administrador", "value": empresas_sin_admin, "subtitle": "Empresas activas sin responsable definido", "icon": "⚠️", "tone": tone_for_count(empresas_sin_admin, danger_at=1), "status": "Revisar" if empresas_sin_admin else "OK"},
        {"label": "Usuarios vinculados", "value": usuarios_asignados, "subtitle": f"{usuarios_sin_empresa} usuarios activos sin empresa", "icon": "👥", "tone": tone_for_count(usuarios_sin_empresa, danger_at=1), "status": "Accesos"},
    ], columns=4)

    tabs = st.tabs(["🌐 Dashboard", "🏢 Empresas", "👥 Administradores", "📋 Audit Log"])

    with tabs[0]:
        st.markdown("### Vista global de empresas")
        rows = []
        if cli_df is not None and not cli_df.empty:
            for _, row in cli_df.iterrows():
                ckey = str(row.get("cliente_key") or "")
                admins = 0
                if access_df is not None and not access_df.empty:
                    admins = int(access_df[(access_df["cliente_key"].astype(str) == ckey) & (access_df["is_company_admin"].fillna(0).astype(int) == 1)]["user_id"].nunique())
                faenas_n = _dep_count(fetch_value, "faenas", ckey)
                trab_n = _dep_count(fetch_value, "trabajadores", ckey)
                docs_emp_n = _dep_count(fetch_value, "empresa_documentos", ckey)
                docs_trab_n = _dep_count(fetch_value, "trabajador_documentos", ckey)
                active_txt = "SI" if int(row.get("activo") or 0) == 1 else "NO"
                estado_admin = "🟢 Con admin" if admins > 0 else "🔴 Sin admin"
                estado_operativo = "🟢 Activa" if active_txt == "SI" else "⚪ Inactiva"
                rows.append({
                    "Código": ckey,
                    "Empresa": str(row.get("cliente_nombre") or ""),
                    "RUT": str(row.get("rut") or ""),
                    "Rubro": str(row.get("vertical") or ""),
                    "Tipo de inicio": impl_label(str(row.get("modo_implementacion") or "")),
                    "Estado": estado_operativo,
                    "Gobernanza": estado_admin,
                    "Faenas": faenas_n,
                    "Trabajadores": trab_n,
                    "Docs Empresa": docs_emp_n,
                    "Docs Trabajador": docs_trab_n,
                    "Admins": admins,
                })
        if rows:
            rows_df = pd.DataFrame(rows)
            total_faenas = int(rows_df["Faenas"].sum())
            total_trab = int(rows_df["Trabajadores"].sum())
            total_docs = int(rows_df["Docs Empresa"].sum() + rows_df["Docs Trabajador"].sum())
            empresas_con_operacion = int(((rows_df["Faenas"] > 0) | (rows_df["Trabajadores"] > 0)).sum())
            adopcion = round((empresas_con_operacion / max(total_empresas, 1)) * 100.0, 1) if total_empresas else 0.0
            kpi_grid([
                {"label": "Adopción operacional", "value": f"{adopcion:.1f}%", "subtitle": f"{empresas_con_operacion}/{total_empresas} empresas con datos", "icon": "📊", "tone": tone_for_percentage(adopcion, danger_below=40, warning_below=70), "status": "Adopción", "progress": adopcion},
                {"label": "Faenas totales", "value": total_faenas, "subtitle": "Operaciones registradas en cartera", "icon": "🌲", "tone": "info", "status": "Operación"},
                {"label": "Trabajadores", "value": total_trab, "subtitle": "Personas registradas globalmente", "icon": "👷", "tone": "purple", "status": "Dotación"},
                {"label": "Documentos", "value": total_docs, "subtitle": "Empresa + trabajador", "icon": "📁", "tone": "success" if total_docs else "neutral", "status": "Repositorio"},
            ], columns=4)
            st.dataframe(rows_df, use_container_width=True, hide_index=True)
            if "Rubro" in rows_df.columns and not rows_df.empty:
                rubro_df = rows_df.groupby("Rubro", dropna=False).size().reset_index(name="Empresas")
                st.markdown("#### Distribución por rubro")
                st.bar_chart(rubro_df.set_index("Rubro"))
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
            st.success(f"Empresa activa en esta sesión: {active.get('cliente_nombre')} · {active.get('vertical') or 'Sin rubro'}")
        else:
            st.warning("No hay empresas visibles en esta sesión.")

    with tabs[1]:
        st.markdown("### Crear empresa")
        c1, c2, c3 = st.columns(3)
        new_name = c1.text_input("Nombre empresa", key="sa_new_empresa_nombre")
        new_rut = c2.text_input("RUT empresa", key="sa_new_empresa_rut")
        new_vertical = c3.text_input("Rubro", key="sa_new_empresa_vertical", help="Define el rubro o tipo de empresa: forestal, construcción, transporte, servicios, etc.")
        c4, c5, c6 = st.columns(3)
        new_impl = c4.selectbox("Tipo de inicio", IMPLEMENTATION_OPTIONS, key="sa_new_empresa_impl", format_func=impl_label, help="Define cómo comenzará la empresa dentro del sistema. Desde cero parte completamente vacía.")
        new_contact = c5.text_input("Contacto", key="sa_new_empresa_contacto")
        new_email = c6.text_input("Email", key="sa_new_empresa_email")
        new_obs = st.text_area("Observaciones", key="sa_new_empresa_obs", height=80)
        new_logo = st.file_uploader("Logo empresa (opcional)", type=['png','jpg','jpeg','webp'], key='sa_new_empresa_logo')
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
                        "INSERT INTO segav_erp_clientes(cliente_key, cliente_nombre, rut, vertical, modo_implementacion, activo, contacto, email, observaciones, logo_local_path, logo_bucket, logo_object_path, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (cliente_key, new_name.strip(), clean_rut(new_rut), new_vertical.strip() or "General", new_impl, 1, new_contact.strip(), new_email.strip(), new_obs.strip(), '', '', '', now, now),
                    )
                    for param_key, param_value in ERP_CLIENT_PARAM_DEFAULTS.items():
                        execute(
                            "INSERT INTO segav_erp_parametros_cliente(cliente_key, param_key, param_value, updated_at) VALUES(?,?,?,?)",
                            (cliente_key, param_key, str(param_value), now),
                        )
                    clear_app_caches()
                    sgsst_log("SuperAdmin / Empresas", "Crear empresa", f"{new_name.strip()} ({cliente_key})")
                    if new_logo is not None:
                        try:
                            save_company_logo_for_cliente(cliente_key, new_logo)
                        except Exception as exc:
                            st.warning(f'La empresa se creó, pero no se pudo guardar el logo: {exc}')
                    st.success("Empresa creada correctamente.")
                    st.rerun()

        st.markdown("---")
        st.markdown("### Modificar o desactivar empresa")
        cli_keys = cli_df["cliente_key"].astype(str).tolist() if cli_df is not None and not cli_df.empty else []
        if cli_keys:
            def _load_selected_company_for_edit():
                _ek = str(st.session_state.get("sa_edit_empresa") or "")
                if not _ek:
                    return
                _hit = cli_df[cli_df["cliente_key"].astype(str) == _ek]
                if _hit is None or _hit.empty:
                    return
                _row = _hit.iloc[0].to_dict()
                st.session_state["sa_edit_loaded_empresa"] = _ek
                st.session_state["sa_edit_nombre"] = str(_row.get("cliente_nombre") or "")
                st.session_state["sa_edit_rut"] = str(_row.get("rut") or "")
                st.session_state["sa_edit_vertical"] = str(_row.get("vertical") or "")
                st.session_state["sa_edit_contacto"] = str(_row.get("contacto") or "")
                st.session_state["sa_edit_email"] = str(_row.get("email") or "")
                st.session_state["sa_edit_obs"] = str(_row.get("observaciones") or "")
                st.session_state["sa_edit_activa"] = "ACTIVA" if int(_row.get("activo") or 0) == 1 else "INACTIVA"
                st.session_state["sa_edit_impl"] = str(_row.get("modo_implementacion") or "CONFIGURABLE")

            edit_key = st.selectbox(
                "Empresa a modificar",
                cli_keys,
                key="sa_edit_empresa",
                format_func=lambda x: str(cli_df[cli_df['cliente_key'].astype(str)==str(x)].iloc[0]['cliente_nombre']),
                on_change=_load_selected_company_for_edit,
            )
            row = cli_df[cli_df["cliente_key"].astype(str) == str(edit_key)].iloc[0].to_dict()
            loaded_key = str(st.session_state.get("sa_edit_loaded_empresa") or "")
            if loaded_key != str(edit_key):
                _load_selected_company_for_edit()
                row = cli_df[cli_df["cliente_key"].astype(str) == str(edit_key)].iloc[0].to_dict()
            e1, e2, e3 = st.columns(3)
            edit_name = e1.text_input("Nombre empresa", key="sa_edit_nombre")
            edit_rut = e2.text_input("RUT", key="sa_edit_rut")
            edit_vertical = e3.text_input("Rubro", key="sa_edit_vertical", help="Rubro o tipo de empresa.")
            e4, e5, e6 = st.columns(3)
            impl_options = IMPLEMENTATION_OPTIONS.copy()
            current_impl = str(st.session_state.get("sa_edit_impl") or row.get("modo_implementacion") or "CONFIGURABLE")
            if current_impl not in impl_options:
                impl_options.append(current_impl)
            edit_impl = e4.selectbox("Tipo de inicio", impl_options, index=impl_options.index(current_impl), key="sa_edit_impl", format_func=impl_label, help="Define si la empresa parte desde cero, con configuración base o en modo demo.")
            edit_contact = e5.text_input("Contacto", key="sa_edit_contacto")
            edit_email = e6.text_input("Email", key="sa_edit_email")
            edit_obs = st.text_area("Observaciones", key="sa_edit_obs", height=80)
            current_logo = None
            try:
                current_logo = get_company_logo_bytes(str(edit_key))
            except Exception:
                current_logo = None
            if current_logo:
                st.image(current_logo, width=180)
            edit_logo = st.file_uploader("Cargar o reemplazar logo", type=['png','jpg','jpeg','webp'], key=f'sa_edit_logo_{edit_key}')
            active_now = str(st.session_state.get("sa_edit_activa") or ("ACTIVA" if int(row.get("activo") or 0) == 1 else "INACTIVA"))
            active_flag = st.selectbox("Estado", ["ACTIVA", "INACTIVA"], index=0 if active_now == "ACTIVA" else 1, key="sa_edit_activa")
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
                    if edit_logo is not None:
                        try:
                            save_company_logo_for_cliente(edit_key, edit_logo)
                        except Exception as exc:
                            st.warning(f'La empresa se actualizó, pero no se pudo guardar el logo: {exc}')
                    st.success("Empresa actualizada.")
                    st.rerun()
            with col_b:
                if st.button("Usar como empresa activa de esta sesión", key="sa_btn_activar_sesion"):
                    st.session_state["active_cliente_key"] = edit_key
                    clear_app_caches()
                    st.success("Empresa activa de esta sesión actualizada.")
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

            # ── Roles por empresa ─────────────────────────────────────────
            st.markdown("##### Roles por empresa")
            st.caption("Asigna un rol específico a cada usuario para esta empresa. El rol de empresa tiene prioridad sobre el rol global.")
            ROLES_EMP = ["ADMIN", "OPERADOR", "LECTOR", "SUPERVISOR"]
            user_roles = {}
            for uid in selected_users:
                uname = active_users[active_users['id'].astype(int)==int(uid)].iloc[0]['username']
                # Get current role_empresa if exists
                current_role = "OPERADOR"
                if access_df is not None and not access_df.empty:
                    match = access_df[(access_df["cliente_key"].astype(str)==admin_cli_key) & (access_df["user_id"].astype(int)==int(uid))]
                    if not match.empty and "role_empresa" in match.columns:
                        current_role = str(match.iloc[0].get("role_empresa") or "OPERADOR")
                default_idx = ROLES_EMP.index(current_role) if current_role in ROLES_EMP else 1
                user_roles[uid] = st.selectbox(
                    f"Rol de {uname}",
                    ROLES_EMP, index=default_idx,
                    key=f"sa_role_{uid}_{admin_cli_key}",
                )

            if st.button("Guardar asignaciones de empresa", key="sa_save_company_access", type="primary"):
                now = datetime.now().isoformat(timespec='seconds')
                execute("DELETE FROM user_client_access WHERE cliente_key=?", (admin_cli_key,))
                for uid in selected_users:
                    role_emp = user_roles.get(uid, "OPERADOR")
                    is_admin = 1 if int(uid) in {int(x) for x in selected_admins} else 0
                    execute(
                        "INSERT INTO user_client_access(user_id, cliente_key, is_company_admin, role_empresa, created_at, updated_at) VALUES(?,?,?,?,?,?)",
                        (int(uid), admin_cli_key, is_admin, role_emp, now, now),
                    )
                clear_app_caches()
                sgsst_log("SuperAdmin / Empresas", "Asignar administradores", admin_cli_key)
                st.success("Asignaciones actualizadas.")
                st.rerun()

            st.caption("Tip: usa la sección Admin Usuarios para crear cuentas nuevas y luego vuelve aquí para vincularlas a una empresa.")

    with tabs[3]:
        st.markdown("### 📋 Registro de Auditoría")
        st.caption("Historial de acciones realizadas por usuarios en el sistema. Se conservan los últimos 2.000 registros.")

        try:
            audit_df = fetch_df(
                "SELECT created_at, username, role_global, role_empresa, accion, entidad, detalle, cliente_key "
                "FROM segav_audit_log ORDER BY id DESC LIMIT 500"
            )
        except Exception:
            audit_df = None

        if audit_df is None or audit_df.empty:
            st.info("Aún no hay registros de auditoría. Las acciones de login y cambios importantes se registrarán automáticamente.")
        else:
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                usuarios_uniq = ["(Todos)"] + sorted(audit_df["username"].dropna().astype(str).unique().tolist())
                filtro_user = st.selectbox("Filtrar por usuario", usuarios_uniq, key="audit_user_f")
            with col_f2:
                acciones_uniq = ["(Todas)"] + sorted(audit_df["accion"].dropna().astype(str).unique().tolist())
                filtro_acc = st.selectbox("Filtrar por acción", acciones_uniq, key="audit_acc_f")
            with col_f3:
                empresas_uniq = ["(Todas)"] + sorted(audit_df["cliente_key"].dropna().astype(str).unique().tolist())
                filtro_emp = st.selectbox("Filtrar por empresa", empresas_uniq, key="audit_emp_f")
            with col_f4:
                filtro_txt = st.text_input("Buscar en detalle", placeholder="Texto libre…", key="audit_txt_f")

            view_a = audit_df.copy()
            if filtro_user != "(Todos)":
                view_a = view_a[view_a["username"] == filtro_user]
            if filtro_acc != "(Todas)":
                view_a = view_a[view_a["accion"] == filtro_acc]
            if filtro_emp != "(Todas)":
                view_a = view_a[view_a["cliente_key"].astype(str) == filtro_emp]
            if filtro_txt.strip():
                qq = filtro_txt.strip().lower()
                view_a = view_a[view_a["detalle"].astype(str).str.lower().str.contains(qq, na=False)]

            kpi_card("Registros mostrados", len(view_a), subtitle="Eventos visibles con filtros aplicados", icon="🧾", tone="info", status="Auditoría")
            st.dataframe(
                view_a.rename(columns={
                    "created_at": "Fecha/Hora", "username": "Usuario",
                    "role_global": "Rol global", "role_empresa": "Rol empresa",
                    "accion": "Acción", "entidad": "Entidad",
                    "detalle": "Detalle", "cliente_key": "Empresa"
                }),
                use_container_width=True,
                hide_index=True,
            )
