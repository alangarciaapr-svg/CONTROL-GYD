# Control Documental de Faenas (v8 - Supabase)

Cambios principales:
- ✅ Dashboard mejorado: KPIs, filtros, avance documental por faena, pendientes por trabajador y alertas por vigencia_examen.
- ✅ Guardar lo generado:
  - Historial de exportaciones ZIP (se guardan en uploads/exports + tabla export_historial)
  - Backup/Restore: descarga un ZIP con app.db + uploads/ y restaura desde ese ZIP.
- ✅ Eliminado: apartado/pantalla "Documentos Extra Faena" (y no se incluye en export).

Nota:
- En Streamlit Community Cloud no se garantiza persistencia del filesystem local entre reboots/redeploy.
  Usa Backup/Restore o migra a DB + storage externo.

- ➕ Nuevo en v7.1: Descargar/Restaurar **solo app.db** desde 'Backup / Restore'.
## Supabase (Postgres) en Streamlit Community Cloud

En **Community Cloud** los archivos locales pueden borrarse en reinicios. Si defines `DB_URL` en Secrets, la app usará Supabase Postgres.

Ejemplo Secrets:

DB_URL = "postgres://..."  # Copia desde Supabase → Connect → Pooler session mode (puerto 5432)

