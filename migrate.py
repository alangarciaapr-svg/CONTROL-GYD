# SEGAV ERP — Arquitectura y escalabilidad

## Prioridad 4 implementada en esta versión

- **Modularización progresiva**: se extrajeron módulos reutilizables para RUT, tenant scope, compliance, export ZIP y manejo de errores blandos.
- **PostgreSQL como backend preferido**: la app prioriza Postgres para producción y deja SQLite solo como fallback local.
- **Tests automatizados**: hay pruebas base para auth, RUT, pendientes, tenant scope y ZIP.
- **API REST**: FastAPI lista para integraciones con nómina, ERP contable o apps móviles.
- **CI/CD**: GitHub Actions ejecuta tests, smoke checks y empaqueta un ZIP antes de cualquier despliegue por webhook.

## Prioridad 5 preparada como base

- **PWA móvil**: la API y modularización dejan lista la base para inspecciones en terreno.
- **BI**: el ordenamiento de capas facilita conectar dashboards analíticos futuros.
- **Firma electrónica avanzada**: la API y trazabilidad sirven como base para integrar proveedores externos.
- **Integración con mutualidades**: la API abre la puerta a DIAT/DIEP automáticos.
- **Multi-idioma**: modularizar UI permite traducir pantallas por capas.
- **Marketplace de plantillas**: el motor de templates multiempresa ya existe y puede crecer por rubro.
- **Motor de reglas configurable**: la parametrización por cliente es la base para reglas sin código.

## Recomendación operativa

Para producción real, definir estos secretos:

- `SEGAV_DB_BACKEND=postgres`
- `SUPABASE_DB_URL=...`
- `SUPABASE_URL=...`
- `SUPABASE_STORAGE_BUCKET=docs`
- `SUPABASE_SERVICE_ROLE_KEY=...`
- `SEGAV_API_SECRET=...`
- `DEPLOY_WEBHOOK_URL=...` (opcional para despliegue automático)
