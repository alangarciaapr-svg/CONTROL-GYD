# SEGAV ERP v8.8

**Plataforma ERP Multiempresa para Gestión de Seguridad y Salud en el Trabajo (SGSST)**

Sistema integral de cumplimiento normativo chileno: Ley 16.744, DS 594, DS 44 y Ley 20.123.

## Características principales

### Gestión documental
- Control de documentos por trabajador y empresa con vencimientos automáticos
- Exportación ZIP por faena + Reporte HTML de cumplimiento imprimible como PDF
- Importación masiva de documentos por ZIP (carpetas por RUT)

### SGSST completo (18 módulos)
Ficha empresa, Matriz legal (23 obligaciones), MIPER, Programa anual, Inspecciones DS 594, Checklist digital (30 ítems), Accidentes/DIAT/DIEP, Capacitaciones/ODI, EPP por trabajador, CPHS con actas, Vigilancia ocupacional (PREXOR/PLANESI/TMERT), Subcontratistas, RIOHS con versionado.

### Multiempresa
Tenant-scoping automático, roles por empresa (ADMIN/OPERADOR/LECTOR/SUPERVISOR), dashboard ejecutivo con score compuesto.

### Seguridad
Hash SHA-256 + salt, validación RUT con DV, contraseña mínima 8 chars, audit log completo.

## Stack: Streamlit + Python 3.11+ + SQLite/PostgreSQL (Supabase)

## Desarrollo
```bash
make test       # Ejecutar tests
make validate   # Validación completa
make run        # Ejecutar local
make zip        # ZIP de deploy
```

## Normativa: Ley 16.744 · DS 44 · DS 594 · Ley 20.123
