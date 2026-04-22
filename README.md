# SEGAV ERP (v8.5.00)

Base corregida restaurando el panel SuperAdmin / Empresas y manteniendo los fixes de compatibilidad y arranque.

## Qué incluye esta versión
- **Dashboard ejecutivo comercial** con score gerencial, semáforo por empresa activa, agenda de vencimientos y vista multiempresa para perfil administrativo.
- Nuevo acceso visible a **Cumplimiento / Alertas** desde la navegación lateral.
- ERP **SEGAV ERP** con acceso por usuarios y roles.
- Base de **multiempresa operativa** con cliente activo y aislamiento inicial por `cliente_key` en operación documental y control de faenas/personal.
- Backend compatible con **SQLite local** o **Supabase/Postgres**.
- Soporte para **Supabase Storage** con fallback local y rutas separadas por cliente activo.
- Gestión de **mandantes, contratos, faenas, trabajadores, asignaciones y documentos**.
- Módulo **Mi Empresa / SGSST** con base para DS 44, Ley 16.744 y DS 594.
- Exportación ZIP, backups y restore.
- Nuevo módulo **Arquitectura / Escalabilidad** para visualizar la base técnica de crecimiento del ERP.
- **API REST** (`api_rest.py`) lista para integración externa.
- **Tests automatizados** y **CI/CD** con GitHub Actions.

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
5. Ejecuta tests automáticos:
   - `pytest -q`
6. Inicia la app:
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
- `SEGAV_DB_BACKEND` (`postgres` recomendado, `sqlite` solo fallback local)
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

## Recomendación operativa
- Para pruebas rápidas usa **SQLite local**.
- Para operación real usa **Supabase/Postgres + Storage**.
- En Streamlit Cloud no confíes en el filesystem local como almacenamiento permanente.

## Verificación hecha en esta entrega
- Compilación Python correcta con `py_compile` para `streamlit_app.py`, `core_db.py` y módulos de `segav_core`.
- Se agregaron archivos de arranque, ejemplo de secretos, verificación de entorno y paquete limpio para GitHub/despliegue.

## Nota honesta
Esta entrega queda **lista para instalar, subir a GitHub y desplegar**, pero la validación completa de la interfaz en ejecución depende de correrla en un entorno con Streamlit instalado y tus secretos reales configurados.


Novedades v8.4.63:
- Nuevo panel exclusivo **SuperAdmin / Empresas** para ver todas las empresas del ERP.
- CRUD de empresas desde un panel centralizado.
- Asignación de administradores por empresa mediante vínculos usuario/empresa.
- Selector de empresa activa por sesión para no depender de una configuración global única.


Novedades v8.4.65:
- Se fusiona la portada profesional de inicio con la base estable que mantiene SuperAdmin / Empresas al inicio.
- Se agrega hero visual local para login corporativo con branding SEGAV.
- Se mantiene la base funcional sin eliminar módulos implementados.


Novedades v8.4.69:
- Los datos heredados se reasignan una sola vez a la empresa histórica (Maderas GyD) y ya no se mueven a SEGAV al cambiar la empresa activa.
- Las empresas nuevas parten vacías sin heredar documentación ni registros previos.
- La pantalla de inicio se reordenó: hero azul a la derecha, login a la izquierda y logo bajo el cuadro de inicio de sesión.



Fase de saneamiento v8.4.70:
- Se eliminaron definiciones duplicadas antiguas de páginas en `streamlit_app.py`, conservando solo los wrappers activos hacia `segav_core`.
- Se limpió el sombreado/confusión de `bootstrap_app` y un import no usado.
- Se incorporó diagnóstico liviano para algunos fallos no críticos que antes quedaban totalmente silenciosos.
- Se mantuvo la funcionalidad visible sin quitar módulos.


## API REST
- Ejecuta con: `uvicorn api_rest:app --reload`
- Health: `/health`
- Login: `POST /api/v1/auth/login`
- Endpoints iniciales: clientes, faenas, trabajadores y exportación ZIP por faena.

## CI/CD
- Workflow: `.github/workflows/segav-ci.yml`
- Corre tests, smoke-check de la API, empaqueta ZIP y puede disparar despliegue por `DEPLOY_WEBHOOK_URL`.


Novedades v8.5.00:
- Se agrega base de arquitectura y escalabilidad sin eliminar funciones existentes.
- PostgreSQL queda como backend preferido para producción, manteniendo SQLite como fallback local.
- Se incorporan tests automáticos, API REST inicial y pipeline CI/CD con GitHub Actions.
- Nuevo módulo interno **Arquitectura / Escalabilidad** para supervisar la madurez técnica del ERP.

Novedades v8.7.00:
- Blindaje de tenant en la API con lectura puntual y eliminación segura por empresa activa en faenas y trabajadores.
- Migraciones versionadas con estado consultable vía API y script `scripts/migrate.py`.
- Readiness de producción con chequeos de secreto, backend preferido, utilitarios Postgres y datos mínimos.
- Respaldos con manifiesto SHA256 y fallback JSON si `pg_dump`/`pg_restore` no están disponibles.
- CI/CD reforzado con migraciones dry-run, resumen de readiness y checksum del artefacto.
