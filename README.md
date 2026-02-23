# Control Documental de Faenas (v7.3.1)

Cambios principales:
- ✅ Dashboard mejorado: KPIs, filtros, avance documental por faena, pendientes por trabajador y alertas por vigencia_examen.
- ✅ Guardar lo generado:
  - Historial de exportaciones ZIP (se guardan en uploads/exports + tabla export_historial)
  - Backup/Restore: descarga un ZIP con app.db + uploads/ y restaura desde ese ZIP.
- ✅ Eliminado: apartado/pantalla "Documentos Extra Faena" (y no se incluye en export).

Nota:
- En Streamlit Community Cloud no se garantiza persistencia del filesystem local entre reboots/redeploy.
  Usa Backup/Restore o migra a DB + storage externo.

- ➕ Nuevo en v7.3.1: Descargar/Restaurar **solo app.db** desde 'Backup / Restore'.

- ➕ Nuevo en v7.3.1: **Auto-backup al guardar** (genera automáticamente app.db y deja botón de descarga en el sidebar).

- 🛠️ v7.3.1: correcciones de robustez (foreign_keys ON por conexión, normalización de navegación).

- ➕ Nuevo en v7.3.3: opción para **ocultar/mostrar el título** en la página (no afecta datos ni navegación).

- ✨ Nuevo en v7.4: reorganización completa de UI (tabs, sidebar con atajos, estilo más profesional) sin perder funcionalidad.

- v7.5.1: se corrige Guardar Mandante (submit siempre activo + validación post-submit + manejo duplicados).

- v7.5.2: corrección de botones en formularios (sin disabled en forms) y validaciones post-submit; botones de upload/restore operativos con validación.

- v7.6: Nuevo apartado 'Documentos Empresa' + Dashboard más dinámico (KPIs, gráficos, tab Empresa). Export ZIP incluye carpeta 02_Documentos_Empresa.

- v7.7: Contratos de Faena permite editar y eliminar (con confirmación y manejo de faenas asociadas), además de gestionar archivo.

- v7.8: Documentos Empresa sugeridos: CERTIFICADO_CUMPLIMIENTO_LABORAL, CERTIFICADO_ACCIDENTABILIDAD, OTROS. Documentos Trabajador agrega LIQUIDACIONES y FINIQUITO. UI iOS-like (cards, bordes redondeados).

- v7.9: Dashboard 'otro nivel': Inbox de gestión con CTA, Vista Global vs Faena, Acciones rápidas, 2 gráficos mínimos.

- v8.0: Carga/importación de trabajadores por FAENA (Importar Excel y asignar). Documentos Trabajador ahora puede operar por faena (solo asignados) y muestra pendientes por faena. Documentación del trabajador se reutiliza si se repite en otra faena.

- v8.0.2: Documentos Trabajador ahora permite seleccionar faena dentro del apartado (sin depender del Dashboard), manteniendo modo 'Solo esta faena'.

- v8.0.3: Reparado 'Documentos Trabajador' eliminando wrappers HTML que generaban cuadros en blanco; selector de faena + toggle funcionan sin cajas vacías.

- v8.0.4: Restore de Backup ZIP ahora soporta formatos antiguos si traen .db y entrega error claro si el ZIP es solo código. Se agregan migraciones para columnas faltantes en contratos/faenas/asignaciones/documentos.

- v8.1: Faenas ahora permite editar y eliminar (con confirmación y borrado de dependencias: asignaciones/anexos). Se mantiene semáforo y anexos.

- v8.2: Documentos Empresa por FAENA (nuevo módulo), export ZIP por faena ahora incluye 02_Documentos_Empresa_Faena. Export mensual (por mes de inicio) con historial mensual.

- v8.2.2: Fix crítico: bloque 'miss_emp' mal indentado en Export (ZIP) que provocaba NameError al iniciar la app.

- v8.2.3: Fix crítico: bloque 'with tab4' (Export mensual) quedó fuera de page_export_zip. Se reindentó para quedar dentro del módulo Export.

- v8.2.4: Fix crítico: pendientes_obligatorios quedó truncada por indentación y pendientes_empresa_faena contenía código inalcanzable. Se reescribieron ambas funciones.

- v8.2.5: Trabajadores ahora permite crear, editar y eliminar (con validación y confirmación). Eliminar borra asignaciones y documentos asociados.
