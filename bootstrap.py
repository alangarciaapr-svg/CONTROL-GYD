from __future__ import annotations

from pathlib import Path


def _status_chip(ok: bool) -> str:
    return "🟢 Implementado" if ok else "🟡 Base lista"


def page_architecture(*, st, ui_header, root_dir, db_backend, pg_dsn_available, api_enabled, ci_enabled, tests_count: int):
    ui_header(
        "Arquitectura / Escalabilidad",
        "Centro de control para la evolución técnica del ERP: modularidad, Postgres, API, CI/CD y hoja de ruta comercial.",
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Backend preferido", "PostgreSQL" if db_backend == "postgres" else "SQLite fallback")
    c2.metric("API REST", "Activa" if api_enabled else "Base lista")
    c3.metric("CI/CD", "Activo" if ci_enabled else "Pendiente")
    c4.metric("Tests detectados", int(tests_count or 0))

    tab1, tab2, tab3 = st.tabs(["🏗️ Prioridad 4", "🔮 Prioridad 5", "📁 Artefactos"]) 

    with tab1:
        rows = [
            {"Mejora": "Modularización progresiva", "Estado": _status_chip(True), "Detalle": "Se extrajeron utilidades críticas: RUT, tenant scope, compliance, export ZIP y errores blandos."},
            {"Mejora": "PostgreSQL como default", "Estado": _status_chip(True), "Detalle": "La configuración ahora prioriza Postgres para producción y deja SQLite como fallback controlado."},
            {"Mejora": "Tests automatizados", "Estado": _status_chip(tests_count > 0), "Detalle": "Cobertura base para auth, RUT, pendientes, tenant scope y ZIP."},
            {"Mejora": "API REST", "Estado": _status_chip(api_enabled), "Detalle": "FastAPI con endpoints de health, login, clientes, faenas, trabajadores y exportación."},
            {"Mejora": "CI/CD", "Estado": _status_chip(ci_enabled), "Detalle": "GitHub Actions con tests, smoke checks y artefacto ZIP; deploy por webhook opcional."},
        ]
        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.info(
            "La separación total del streamlit_app.py sigue siendo evolutiva para no romper producción. Esta versión deja la base lista y segura para continuar por bloques."
        )

    with tab2:
        roadmap_rows = [
            {"Línea": "PWA móvil", "Base": "documentación + API lista para crecer", "Impacto": "inspecciones en terreno con cámara/GPS/firma"},
            {"Línea": "BI integrado", "Base": "centro de arquitectura y datos más ordenados", "Impacto": "gerencia y benchmarking por rubro"},
            {"Línea": "Firma electrónica avanzada", "Base": "API y trazabilidad", "Impacto": "ODI, RIOHS y capacitaciones sin papel"},
            {"Línea": "Integración mutualidades", "Base": "API REST", "Impacto": "DIAT/DIEP automatizable"},
            {"Línea": "Multi-idioma", "Base": "modularización de UI", "Impacto": "expansión internacional"},
            {"Línea": "Marketplace de plantillas", "Base": "templates multiempresa ya presentes", "Impacto": "SaaS por rubro"},
            {"Línea": "Motor de reglas configurable", "Base": "parametrización por cliente", "Impacto": "compliance sin tocar código"},
        ]
        st.dataframe(roadmap_rows, use_container_width=True, hide_index=True)
        st.caption("Estas líneas quedan registradas dentro del ERP como plan técnico-comercial para seguir construyendo sin eliminar nada funcional.")

    with tab3:
        root = Path(root_dir)
        files = [
            ".github/workflows/segav-ci.yml",
            "api_rest.py",
            "tests/test_auth_and_api_security.py",
            "tests/test_rut_utils.py",
            "tests/test_compliance_logic.py",
            "tests/test_tenant_scope.py",
            "tests/test_export_utils.py",
            "docs/ARQUITECTURA_ESCALABILIDAD.md",
        ]
        status = []
        for rel in files:
            path = root / rel
            status.append({"Archivo": rel, "Existe": "Sí" if path.exists() else "No"})
        st.dataframe(status, use_container_width=True, hide_index=True)
        if not pg_dsn_available:
            st.warning("No detecté DSN de Postgres en este entorno. La app queda preparada para producción, pero seguirá usando SQLite como fallback hasta configurar secretos reales.")
