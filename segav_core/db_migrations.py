from __future__ import annotations

import json
from datetime import datetime, timezone

from segav_core.migration_registry import builtin_migrations

UTC = timezone.utc
SCHEMA_VERSION = "2026.04-growth-pack"

BUILTIN_TEMPLATES = [
    {
        "template_key": "tpl_forestal_base",
        "rubro": "Forestal",
        "template_name": "Forestal base auditable",
        "description": "Configuración sugerida para forestal con foco en DS44, documentación mensual y control de faenas.",
        "payload": {"erp_vertical": "Forestal", "modo_implementacion": "CONFIGURABLE", "docs_empresa_mensuales": ["F30", "F30-1", "Certificado Accidentabilidad", "Liquidaciones de Sueldo"], "features": ["sgsst", "faenas", "odi", "epp", "alertas"]},
    },
    {
        "template_key": "tpl_transporte_base",
        "rubro": "Transporte",
        "template_name": "Transporte contratista",
        "description": "Plantilla sugerida para transporte y flota con documentación recurrente y control de personal.",
        "payload": {"erp_vertical": "Transporte", "modo_implementacion": "CONFIGURABLE", "docs_empresa_mensuales": ["F30", "F30-1", "Liquidaciones de Sueldo", "Certificado Accidentabilidad"], "features": ["sgsst", "flota", "capacitaciones", "alertas"]},
    },
    {
        "template_key": "tpl_construccion_base",
        "rubro": "Construcción",
        "template_name": "Construcción multiobra",
        "description": "Base para mandantes, contratos, control documental por obra y cumplimiento en terreno.",
        "payload": {"erp_vertical": "Construcción", "modo_implementacion": "CONFIGURABLE", "docs_empresa_mensuales": ["F30", "F30-1", "Liquidaciones de Sueldo", "Previred"], "features": ["sgsst", "mandantes", "contratos", "faenas", "checklists"]},
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def ensure_schema_migrations_table(execute, db_backend: str):
    if str(db_backend or "sqlite").lower() == "postgres":
        execute(
            """
            CREATE TABLE IF NOT EXISTS segav_schema_migrations (
                version_key TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                detail_json TEXT
            );
            """
        )
        return
    execute(
        """
        CREATE TABLE IF NOT EXISTS segav_schema_migrations (
            version_key TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now')),
            detail_json TEXT
        );
        """
    )


def _table_sql(db_backend: str, *, sqlite: str, postgres: str) -> str:
    return postgres if str(db_backend or "sqlite").lower() == "postgres" else sqlite


def ensure_growth_tables(execute, db_backend: str):
    stmts = [
        _table_sql(db_backend, sqlite="""
            CREATE TABLE IF NOT EXISTS segav_integrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_key TEXT NOT NULL,
                integration_type TEXT NOT NULL,
                provider TEXT NOT NULL,
                config_json TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_sync_at TEXT,
                last_status TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """, postgres="""
            CREATE TABLE IF NOT EXISTS segav_integrations (
                id BIGSERIAL PRIMARY KEY,
                cliente_key TEXT NOT NULL,
                integration_type TEXT NOT NULL,
                provider TEXT NOT NULL,
                config_json TEXT,
                is_active BIGINT NOT NULL DEFAULT 1,
                last_sync_at TIMESTAMPTZ,
                last_status TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """),
        _table_sql(db_backend, sqlite="""
            CREATE TABLE IF NOT EXISTS segav_integration_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_key TEXT NOT NULL,
                integration_type TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT,
                status TEXT NOT NULL DEFAULT 'PENDIENTE',
                response_text TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                processed_at TEXT
            );
            """, postgres="""
            CREATE TABLE IF NOT EXISTS segav_integration_events (
                id BIGSERIAL PRIMARY KEY,
                cliente_key TEXT NOT NULL,
                integration_type TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT,
                status TEXT NOT NULL DEFAULT 'PENDIENTE',
                response_text TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                processed_at TIMESTAMPTZ
            );
            """),
        _table_sql(db_backend, sqlite="""
            CREATE TABLE IF NOT EXISTS segav_rule_engine_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_key TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                target_scope TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'MEDIA',
                rule_json TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """, postgres="""
            CREATE TABLE IF NOT EXISTS segav_rule_engine_rules (
                id BIGSERIAL PRIMARY KEY,
                cliente_key TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                target_scope TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'MEDIA',
                rule_json TEXT NOT NULL,
                is_active BIGINT NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """),
        _table_sql(db_backend, sqlite="""
            CREATE TABLE IF NOT EXISTS segav_template_marketplace (
                template_key TEXT PRIMARY KEY,
                rubro TEXT NOT NULL,
                template_name TEXT NOT NULL,
                description TEXT,
                payload_json TEXT NOT NULL,
                is_builtin INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """, postgres="""
            CREATE TABLE IF NOT EXISTS segav_template_marketplace (
                template_key TEXT PRIMARY KEY,
                rubro TEXT NOT NULL,
                template_name TEXT NOT NULL,
                description TEXT,
                payload_json TEXT NOT NULL,
                is_builtin BIGINT NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """),
        _table_sql(db_backend, sqlite="""
            CREATE TABLE IF NOT EXISTS segav_signature_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_key TEXT NOT NULL,
                title TEXT NOT NULL,
                document_type TEXT NOT NULL,
                signer_name TEXT NOT NULL,
                signer_email TEXT,
                signature_level TEXT NOT NULL DEFAULT 'SIMPLE',
                status TEXT NOT NULL DEFAULT 'PENDIENTE',
                metadata_json TEXT,
                requested_at TEXT NOT NULL DEFAULT (datetime('now')),
                signed_at TEXT
            );
            """, postgres="""
            CREATE TABLE IF NOT EXISTS segav_signature_requests (
                id BIGSERIAL PRIMARY KEY,
                cliente_key TEXT NOT NULL,
                title TEXT NOT NULL,
                document_type TEXT NOT NULL,
                signer_name TEXT NOT NULL,
                signer_email TEXT,
                signature_level TEXT NOT NULL DEFAULT 'SIMPLE',
                status TEXT NOT NULL DEFAULT 'PENDIENTE',
                metadata_json TEXT,
                requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                signed_at TIMESTAMPTZ
            );
            """),
        _table_sql(db_backend, sqlite="""
            CREATE TABLE IF NOT EXISTS segav_mobile_inspections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_key TEXT NOT NULL,
                faena_id INTEGER,
                inspection_type TEXT NOT NULL,
                inspector_name TEXT NOT NULL,
                gps_lat REAL,
                gps_lng REAL,
                observations TEXT,
                evidence_json TEXT,
                signature_name TEXT,
                status TEXT NOT NULL DEFAULT 'BORRADOR',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                synced_at TEXT
            );
            """, postgres="""
            CREATE TABLE IF NOT EXISTS segav_mobile_inspections (
                id BIGSERIAL PRIMARY KEY,
                cliente_key TEXT NOT NULL,
                faena_id BIGINT,
                inspection_type TEXT NOT NULL,
                inspector_name TEXT NOT NULL,
                gps_lat DOUBLE PRECISION,
                gps_lng DOUBLE PRECISION,
                observations TEXT,
                evidence_json TEXT,
                signature_name TEXT,
                status TEXT NOT NULL DEFAULT 'BORRADOR',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                synced_at TIMESTAMPTZ
            );
            """),
    ]
    for stmt in stmts:
        execute(stmt)
    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_segav_integrations_cliente ON segav_integrations(cliente_key);",
        "CREATE INDEX IF NOT EXISTS idx_segav_events_cliente ON segav_integration_events(cliente_key);",
        "CREATE INDEX IF NOT EXISTS idx_segav_rules_cliente ON segav_rule_engine_rules(cliente_key);",
        "CREATE INDEX IF NOT EXISTS idx_segav_signatures_cliente ON segav_signature_requests(cliente_key);",
        "CREATE INDEX IF NOT EXISTS idx_segav_mobile_cliente ON segav_mobile_inspections(cliente_key);",
    ]:
        try:
            execute(idx)
        except Exception:
            pass


def seed_builtin_templates(execute, fetch_value):
    now = _now()
    for item in BUILTIN_TEMPLATES:
        exists = int(fetch_value("SELECT COUNT(*) FROM segav_template_marketplace WHERE template_key=?", (item["template_key"],), default=0) or 0)
        if exists:
            continue
        execute(
            "INSERT INTO segav_template_marketplace(template_key, rubro, template_name, description, payload_json, is_builtin, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (item["template_key"], item["rubro"], item["template_name"], item["description"], json.dumps(item["payload"], ensure_ascii=False), 1, now, now),
        )


def known_migrations():
    return builtin_migrations(ensure_growth_tables=ensure_growth_tables, seed_builtin_templates=seed_builtin_templates)


def migration_status(fetch_value):
    rows = []
    for migration in known_migrations():
        applied = int(fetch_value("SELECT COUNT(*) FROM segav_schema_migrations WHERE version_key=?", (migration.version_key,), default=0) or 0) > 0
        rows.append({"version_key": migration.version_key, "description": migration.description, "applied": applied})
    return rows


def apply_runtime_migrations(execute, fetch_value, db_backend: str):
    ensure_schema_migrations_table(execute, db_backend)
    for migration in known_migrations():
        exists = int(fetch_value("SELECT COUNT(*) FROM segav_schema_migrations WHERE version_key=?", (migration.version_key,), default=0) or 0)
        if exists <= 0:
            migration.apply_fn(execute, fetch_value, db_backend)
            payload = json.dumps({"version": migration.version_key, "description": migration.description, "applied_at": _now()}, ensure_ascii=False)
            try:
                execute("INSERT INTO segav_schema_migrations(version_key, detail_json) VALUES(?, ?)", (migration.version_key, payload))
            except Exception:
                pass

    exists = int(fetch_value("SELECT COUNT(*) FROM segav_schema_migrations WHERE version_key=?", (SCHEMA_VERSION,), default=0) or 0)
    if exists <= 0:
        try:
            execute("INSERT INTO segav_schema_migrations(version_key, detail_json) VALUES(?, ?)", (SCHEMA_VERSION, json.dumps({"version": SCHEMA_VERSION, "applied_at": _now()}, ensure_ascii=False)))
        except Exception:
            pass
