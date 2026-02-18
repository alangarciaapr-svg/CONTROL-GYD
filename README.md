# Control Documental de Faenas (v7.2)

Cambios principales:
- ✅ Dashboard mejorado: KPIs, filtros, avance documental por faena, pendientes por trabajador y alertas por vigencia_examen.
- ✅ Guardar lo generado:
  - Historial de exportaciones ZIP (se guardan en uploads/exports + tabla export_historial)
  - Backup/Restore: descarga un ZIP con app.db + uploads/ y restaura desde ese ZIP.
- ✅ Eliminado: apartado/pantalla "Documentos Extra Faena" (y no se incluye en export).

Nota:
- En Streamlit Community Cloud no se garantiza persistencia del filesystem local entre reboots/redeploy.
  Usa Backup/Restore o migra a DB + storage externo.

- ➕ Nuevo en v7.2: Descargar/Restaurar **solo app.db** desde 'Backup / Restore'.

- ➕ Nuevo en v7.2: **Auto-backup al guardar** (genera automáticamente app.db y deja botón de descarga en el sidebar).
