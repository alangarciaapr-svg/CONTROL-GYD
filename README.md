# Control Documental de Faenas (v8.4.23)

Estado de esta base:
- Backend compatible con SQLite local o Supabase/Postgres.
- Storage con rutas sanitizadas, carga/borrado administrativo y fallback local.
- Export ZIP, documentos por empresa/faena/trabajador, asignaciones y respaldos heredados.

Cambios consolidados recientes:
- Eliminada la duplicidad principal de pantallas críticas.
- Asignaciones y documentos ajustados para Postgres/Supabase.
- Límite de carga por archivo en 1,5 MB con compresión ZIP automática cuando alcanza.
- Eliminación de documentos con limpieza de BD y archivo físico cuando ya no quedan referencias.
- Diagnóstico de backend/Storage más claro en Backup / Restore.

Notas operativas:
- En Streamlit Community Cloud el filesystem local no es persistente garantizado entre reinicios.
- Si trabajas con Supabase/Postgres, la fuente de verdad es la base online y Storage.
- Para subir o borrar archivos en Storage usa una secret/service key real en `SUPABASE_SERVICE_ROLE_KEY`.
