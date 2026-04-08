# Revisión técnica y correcciones aplicadas

## Qué revisé
- Compilación Python de `streamlit_app.py`, `core_db.py` y todos los módulos `segav_core`.
- Integridad básica de wrappers `page_*` entre `streamlit_app.py` y módulos `segav_core`.
- Presencia de menú y ruta de `SuperAdmin / Empresas`.
- Duplicidades de funciones top-level en `streamlit_app.py`.
- Riesgos de arranque en código ejecutado al importar el módulo.

## Error real corregido
### 1) Riesgo de `NameError` al iniciar
Había una referencia a `_record_soft_error(...)` dentro de `_get_cfg(...)` y `clear_app_caches(...)` antes de que `_record_soft_error` estuviera definida.

Eso podía romper el arranque en escenarios como:
- error de acceso a `st.secrets`
- fallos tempranos al limpiar caché

### Solución aplicada
- se movió/inyectó una definición liviana de `_record_soft_error(...)` al inicio del archivo
- se eliminó la definición tardía duplicada

## Estado después de la corrección
- `py_compile` OK
- sin funciones top-level duplicadas en `streamlit_app.py`
- wrappers `page_*` compatibles con sus módulos `segav_core`
- la base mantiene `SuperAdmin / Empresas`, multiempresa, cumplimiento y dashboard

## Sugerencias de mejora
1. Centralizar definitivamente `auth_gate_ui` fuera de `streamlit_app.py`.
2. Reducir `except Exception: pass` en flujos críticos y reemplazarlos por mensajes controlados.
3. Agregar una pantalla interna de diagnóstico para `SUPERADMIN` con:
   - backend activo
   - tenant actual
   - últimos soft errors
   - estado de Storage/Supabase
4. Crear pruebas de humo para navegación básica:
   - Dashboard
   - SuperAdmin / Empresas
   - Faenas
   - Documentos
5. Separar la pantalla de inicio/login en un módulo UI dedicado para no volver a romperla al tocar auth.
