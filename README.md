# SEGAV ERP (v8.4.55)

Base operativa reforzada para transición a ERP **vendible, multiempresa y orientado a cumplimiento**.

## Qué incluye esta versión
- ERP **SEGAV ERP** con acceso por usuarios y roles.
- Backend compatible con **SQLite local** o **Supabase/Postgres**.
- Soporte para **Supabase Storage** con fallback local.
- Gestión de **mandantes, contratos, faenas, trabajadores, asignaciones y documentos**.
- Módulo **Mi Empresa / SGSST** con base para **DS 44, Ley 16.744 y DS 594**.
- Selector de **empresa activa** en sidebar para operar el ERP por cliente.
- Nuevo módulo **Cumplimiento / Alertas** con:
  - semáforo ejecutivo por empresa activa,
  - alertas automáticas de documentación y vencimientos,
  - planes de acción manuales,
  - matriz resumida de riesgo documental por faena.
- Base multiempresa reforzada con `cliente_key` en tablas operativas y SGSST para seguir avanzando sin eliminar nada.
- Exportación ZIP, backups y restore.

## Estructura mínima del proyecto
- `streamlit_app.py`
- `core_db.py`
- `requirements.txt`
- `.streamlit/config.toml`
- `.streamlit/secrets.toml.example`
- `segav_core/`
- `run_local.bat`
- `run_local.sh`
- `check_setup.py`

## Inicio rápido local (SQLite)
1. Instala Python 3.11 o superior.
2. Crea y activa un entorno virtual.
3. Instala dependencias:
   - `pip install -r requirements.txt`
4. Opcional: ejecuta validación previa:
   - `python check_setup.py`
5. Inicia la app:
   - `python -m streamlit run streamlit_app.py`

También puedes usar:
- Windows: `run_local.bat`
- Linux/macOS: `bash run_local.sh`

## Acceso inicial
Si la tabla `users` está vacía, la app siembra un SUPERADMIN por defecto con:
- Usuario: `a.garcia`
- Contraseña: `225188`

**Cámbiala inmediatamente después del primer ingreso**.

## Uso con Supabase / Streamlit Cloud
Copia `.streamlit/secrets.toml.example` como base para tu archivo de secretos y completa lo que corresponda.

Variables soportadas:
- `SUPABASE_DB_URL`
- `SUPABASE_DB_HOST`
- `SUPABASE_DB_PORT`
- `SUPABASE_DB_NAME`
- `SUPABASE_DB_USER`
- `SUPABASE_DB_PASSWORD`
- `SUPABASE_URL`
- `SUPABASE_STORAGE_BUCKET`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_ANON_KEY`
- `DEFAULT_ADMIN_USER`
- `DEFAULT_ADMIN_PASS`

## Enfoque de esta entrega
Esta versión no elimina nada funcional. El avance se enfocó en:
- reforzar la transición a **multiempresa real**,
- crear una primera capa visible de **cumplimiento y alertas**,
- dejar una base más vendible para el siguiente bloque: dashboards ejecutivos, permisos por empresa y onboarding comercial.

## Verificación hecha en esta entrega
- Compilación Python correcta con `py_compile` para `streamlit_app.py`, `core_db.py` y módulos de `segav_core`.
- Integración del módulo `segav_core/ops_compliance.py`.
- Actualización del README y base para siguiente fase comercial.
