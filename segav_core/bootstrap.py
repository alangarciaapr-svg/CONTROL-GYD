"""Bootstrap, migraciones y seed data para SEGAV ERP.

Fase 3: separa arranque, tablas, migraciones y semillas del archivo principal
sin cambiar la funcionalidad visible del ERP.
"""

from __future__ import annotations

import os
import json
import re
from datetime import date, datetime

import streamlit as st

from core_db import (
    DB_BACKEND,
    conn,
    execute,
    fetch_df,
    fetch_value,
    migrate_add_columns_if_missing,
)
from segav_core.app_config import UPLOAD_ROOT
from segav_core.catalogs import (
    CARGO_DOCS_ORDER,
    CARGO_DOCS_RULES,
    DOC_EMPRESA_MENSUALES,
    ERP_CLIENT_PARAM_DEFAULTS,
    ERP_TEMPLATE_PRESETS,
    SGSST_MATRIZ_BASE,
)
from segav_core.formatters import clean_rut, make_erp_key


def ensure_dirs():
    os.makedirs(UPLOAD_ROOT, exist_ok=True)
    os.makedirs(os.path.join(UPLOAD_ROOT, "exports"), exist_ok=True)
    os.makedirs(os.path.join(UPLOAD_ROOT, "auto_backups"), exist_ok=True)


def _bootstrap_config_value(key: str, default: str = "") -> str:
    try:
        df = fetch_df("SELECT config_value FROM segav_erp_config WHERE config_key=? LIMIT 1", (key,))
        if df is None or df.empty:
            return str(default or "")
        return str(df.iloc[0].get("config_value") or default or "")
    except Exception:
        return str(default or "")

def ensure_core_tables_postgres():
    if DB_BACKEND != "postgres":
        return
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS mandantes (
            id BIGSERIAL PRIMARY KEY,
            nombre TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS contratos_faena (
            id BIGSERIAL PRIMARY KEY,
            mandante_id BIGINT NOT NULL REFERENCES mandantes(id) ON DELETE RESTRICT,
            nombre TEXT NOT NULL,
            fecha_inicio TEXT,
            fecha_termino TEXT,
            file_path TEXT,
            sha256 TEXT,
            created_at TEXT,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS faenas (
            id BIGSERIAL PRIMARY KEY,
            mandante_id BIGINT NOT NULL REFERENCES mandantes(id) ON DELETE RESTRICT,
            contrato_faena_id BIGINT REFERENCES contratos_faena(id) ON DELETE SET NULL,
            nombre TEXT NOT NULL,
            ubicacion TEXT DEFAULT '',
            fecha_inicio TEXT NOT NULL,
            fecha_termino TEXT,
            estado TEXT NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA','TERMINADA'))
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS faena_anexos (
            id BIGSERIAL PRIMARY KEY,
            faena_id BIGINT NOT NULL REFERENCES faenas(id) ON DELETE CASCADE,
            nombre TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS trabajadores (
            id BIGSERIAL PRIMARY KEY,
            rut TEXT NOT NULL UNIQUE,
            nombres TEXT NOT NULL,
            apellidos TEXT NOT NULL,
            cargo TEXT DEFAULT '',
            centro_costo TEXT,
            email TEXT,
            fecha_contrato TEXT,
            vigencia_examen TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS asignaciones (
            id BIGSERIAL PRIMARY KEY,
            faena_id BIGINT NOT NULL REFERENCES faenas(id) ON DELETE CASCADE,
            trabajador_id BIGINT NOT NULL REFERENCES trabajadores(id) ON DELETE CASCADE,
            cargo_faena TEXT DEFAULT '',
            fecha_ingreso TEXT NOT NULL,
            fecha_egreso TEXT,
            estado TEXT NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA','CERRADA')),
            UNIQUE(faena_id, trabajador_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS trabajador_documentos (
            id BIGSERIAL PRIMARY KEY,
            trabajador_id BIGINT NOT NULL REFERENCES trabajadores(id) ON DELETE CASCADE,
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS empresa_documentos (
            id BIGSERIAL PRIMARY KEY,
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS faena_empresa_documentos (
            id BIGSERIAL PRIMARY KEY,
            faena_id BIGINT NOT NULL REFERENCES faenas(id) ON DELETE CASCADE,
            mandante_id BIGINT REFERENCES mandantes(id) ON DELETE SET NULL,
            periodo_anio INTEGER,
            periodo_mes INTEGER,
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS export_historial (
            id BIGSERIAL PRIMARY KEY,
            faena_id BIGINT NOT NULL REFERENCES faenas(id) ON DELETE CASCADE,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes BIGINT NOT NULL,
            created_at TEXT NOT NULL,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS export_historial_mes (
            id BIGSERIAL PRIMARY KEY,
            year_month TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT,
            size_bytes BIGINT,
            created_at TEXT NOT NULL,
            bucket TEXT,
            object_path TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS auto_backup_historial (
            id BIGSERIAL PRIMARY KEY,
            tag TEXT,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes BIGINT NOT NULL,
            created_at TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            salt_b64 TEXT NOT NULL,
            pass_hash_b64 TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'OPERADOR',
            perms_json TEXT,
            is_active BIGINT NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ]
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_contratos_faena_mandante_id ON contratos_faena(mandante_id);",
        "CREATE INDEX IF NOT EXISTS idx_faenas_mandante_id ON faenas(mandante_id);",
        "CREATE INDEX IF NOT EXISTS idx_faenas_contrato_id ON faenas(contrato_faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_faena_anexos_faena_id ON faena_anexos(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_asignaciones_faena_id ON asignaciones(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_asignaciones_trabajador_id ON asignaciones(trabajador_id);",
        "CREATE INDEX IF NOT EXISTS idx_trabajador_documentos_trabajador_id ON trabajador_documentos(trabajador_id);",
        "CREATE INDEX IF NOT EXISTS idx_faena_empresa_documentos_faena_id ON faena_empresa_documentos(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_export_historial_faena_id ON export_historial(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);",
    ]
    with conn() as c:
        for s in stmts + indexes:
            c.execute(s)
        c.commit()


def init_db():
    if DB_BACKEND == "postgres":
        ensure_core_tables_postgres()
        ensure_sgsst_tables_postgres()
        ensure_storage_columns_postgres()
        sync_postgres_core_sequences()
        ensure_sgsst_seed_data()
        return
    with conn() as c:
        c.execute("PRAGMA foreign_keys = ON;")

        c.execute('''
        CREATE TABLE IF NOT EXISTS mandantes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS contratos_faena (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandante_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            fecha_inicio TEXT,
            fecha_termino TEXT,
            file_path TEXT,
            sha256 TEXT,
            created_at TEXT,
            FOREIGN KEY(mandante_id) REFERENCES mandantes(id) ON DELETE RESTRICT
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS faenas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandante_id INTEGER NOT NULL,
            contrato_faena_id INTEGER,
            nombre TEXT NOT NULL,
            ubicacion TEXT DEFAULT '',
            fecha_inicio TEXT NOT NULL,
            fecha_termino TEXT,
            estado TEXT NOT NULL CHECK(estado IN ('ACTIVA','TERMINADA')),
            FOREIGN KEY(mandante_id) REFERENCES mandantes(id) ON DELETE RESTRICT,
            FOREIGN KEY(contrato_faena_id) REFERENCES contratos_faena(id) ON DELETE SET NULL
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS faena_anexos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faena_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS trabajadores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rut TEXT NOT NULL UNIQUE,
            nombres TEXT NOT NULL,
            apellidos TEXT NOT NULL,
            cargo TEXT DEFAULT ''
        );
        ''')

        migrate_add_columns_if_missing(c, "trabajadores", {
            "centro_costo": "TEXT",
            "email": "TEXT",
            "fecha_contrato": "TEXT",
            "vigencia_examen": "TEXT",
        })

        c.execute('''
        CREATE TABLE IF NOT EXISTS asignaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faena_id INTEGER NOT NULL,
            trabajador_id INTEGER NOT NULL,
            cargo_faena TEXT DEFAULT '',
            fecha_ingreso TEXT NOT NULL,
            fecha_egreso TEXT,
            estado TEXT NOT NULL DEFAULT 'ACTIVA' CHECK(estado IN ('ACTIVA','CERRADA')),
            UNIQUE(faena_id, trabajador_id),
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE,
            FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS trabajador_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trabajador_id INTEGER NOT NULL,
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
        );
        ''')

        # Eliminado: "Documentos extra faena" (no tabla ni UI en esta versión)


        c.execute('''
        CREATE TABLE IF NOT EXISTS empresa_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        ''')







        c.execute('''
        CREATE TABLE IF NOT EXISTS faena_empresa_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faena_id INTEGER NOT NULL,
            mandante_id INTEGER,
            periodo_anio INTEGER,
            periodo_mes INTEGER,
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE,
            FOREIGN KEY(mandante_id) REFERENCES mandantes(id) ON DELETE SET NULL
        );
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS export_historial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faena_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE
        );
        ''')




        c.execute('''
        CREATE TABLE IF NOT EXISTS export_historial_mes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year_month TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT,
            size_bytes INTEGER,
            created_at TEXT NOT NULL
        );
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS auto_backup_historial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );
        ''')

        ensure_storage_columns_sqlite(c)
        ensure_sgsst_tables_sqlite(c)
        c.commit()
    ensure_sgsst_seed_data()


def ensure_sgsst_tables_postgres():
    if DB_BACKEND != "postgres":
        return
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS sgsst_empresa (
            id BIGSERIAL PRIMARY KEY,
            razon_social TEXT,
            rut TEXT,
            direccion TEXT,
            actividad TEXT,
            organismo_admin TEXT,
            representantes TEXT,
            prevencionista TEXT,
            canal_denuncias TEXT,
            dotacion_total INTEGER DEFAULT 0,
            politica_version TEXT,
            politica_fecha TEXT,
            observaciones TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_matriz_legal (
            id BIGSERIAL PRIMARY KEY,
            norma TEXT NOT NULL,
            articulo TEXT,
            tema TEXT NOT NULL,
            obligacion TEXT NOT NULL,
            aplica_a TEXT,
            periodicidad TEXT,
            responsable TEXT,
            evidencia TEXT,
            estado TEXT NOT NULL DEFAULT 'PENDIENTE',
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_programa_anual (
            id BIGSERIAL PRIMARY KEY,
            anio INTEGER NOT NULL,
            objetivo TEXT NOT NULL,
            actividad TEXT NOT NULL,
            faena_id BIGINT REFERENCES faenas(id) ON DELETE SET NULL,
            responsable TEXT,
            fecha_compromiso TEXT,
            estado TEXT NOT NULL DEFAULT 'PENDIENTE',
            avance INTEGER DEFAULT 0,
            evidencia TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_miper (
            id BIGSERIAL PRIMARY KEY,
            faena_id BIGINT REFERENCES faenas(id) ON DELETE SET NULL,
            proceso TEXT,
            tarea TEXT,
            cargo TEXT,
            peligro TEXT NOT NULL,
            riesgo TEXT NOT NULL,
            consecuencia TEXT,
            controles_existentes TEXT,
            probabilidad INTEGER DEFAULT 1,
            severidad INTEGER DEFAULT 1,
            nivel_riesgo INTEGER DEFAULT 1,
            medidas TEXT,
            responsable TEXT,
            plazo TEXT,
            estado TEXT NOT NULL DEFAULT 'PENDIENTE',
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_inspecciones (
            id BIGSERIAL PRIMARY KEY,
            faena_id BIGINT REFERENCES faenas(id) ON DELETE SET NULL,
            tipo TEXT,
            area TEXT,
            item TEXT NOT NULL,
            resultado TEXT NOT NULL DEFAULT 'OBSERVACIÓN',
            observacion TEXT,
            accion_correctiva TEXT,
            responsable TEXT,
            plazo TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_incidentes (
            id BIGSERIAL PRIMARY KEY,
            trabajador_id BIGINT REFERENCES trabajadores(id) ON DELETE SET NULL,
            faena_id BIGINT REFERENCES faenas(id) ON DELETE SET NULL,
            fecha TEXT NOT NULL,
            tipo TEXT NOT NULL,
            gravedad TEXT,
            descripcion TEXT NOT NULL,
            organismo_admin TEXT,
            dias_perdidos INTEGER DEFAULT 0,
            medidas TEXT,
            estado TEXT NOT NULL DEFAULT 'PENDIENTE',
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_capacitaciones (
            id BIGSERIAL PRIMARY KEY,
            trabajador_id BIGINT REFERENCES trabajadores(id) ON DELETE SET NULL,
            faena_id BIGINT REFERENCES faenas(id) ON DELETE SET NULL,
            tipo TEXT NOT NULL,
            tema TEXT NOT NULL,
            fecha TEXT NOT NULL,
            vigencia TEXT,
            horas NUMERIC,
            relator TEXT,
            estado TEXT NOT NULL DEFAULT 'VIGENTE',
            evidencia TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sgsst_auditoria (
            id BIGSERIAL PRIMARY KEY,
            modulo TEXT NOT NULL,
            accion TEXT NOT NULL,
            detalle TEXT,
            usuario TEXT,
            created_at TEXT NOT NULL
        );
        """,
    ]
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_sgsst_programa_faena_id ON sgsst_programa_anual(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_sgsst_miper_faena_id ON sgsst_miper(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_sgsst_inspecciones_faena_id ON sgsst_inspecciones(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_sgsst_incidentes_faena_id ON sgsst_incidentes(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_sgsst_incidentes_trabajador_id ON sgsst_incidentes(trabajador_id);",
        "CREATE INDEX IF NOT EXISTS idx_sgsst_capacitaciones_faena_id ON sgsst_capacitaciones(faena_id);",
        "CREATE INDEX IF NOT EXISTS idx_sgsst_capacitaciones_trabajador_id ON sgsst_capacitaciones(trabajador_id);",
    ]
    with conn() as c:
        for s in stmts + indexes:
            c.execute(s)
        c.commit()


def ensure_sgsst_tables_sqlite(c):
    if DB_BACKEND == "postgres":
        return
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_empresa (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        razon_social TEXT,
        rut TEXT,
        direccion TEXT,
        actividad TEXT,
        organismo_admin TEXT,
        representantes TEXT,
        prevencionista TEXT,
        canal_denuncias TEXT,
        dotacion_total INTEGER DEFAULT 0,
        politica_version TEXT,
        politica_fecha TEXT,
        observaciones TEXT,
        created_at TEXT,
        updated_at TEXT
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_matriz_legal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        norma TEXT NOT NULL,
        articulo TEXT,
        tema TEXT NOT NULL,
        obligacion TEXT NOT NULL,
        aplica_a TEXT,
        periodicidad TEXT,
        responsable TEXT,
        evidencia TEXT,
        estado TEXT NOT NULL DEFAULT 'PENDIENTE',
        created_at TEXT,
        updated_at TEXT
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_programa_anual (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        anio INTEGER NOT NULL,
        objetivo TEXT NOT NULL,
        actividad TEXT NOT NULL,
        faena_id INTEGER,
        responsable TEXT,
        fecha_compromiso TEXT,
        estado TEXT NOT NULL DEFAULT 'PENDIENTE',
        avance INTEGER DEFAULT 0,
        evidencia TEXT,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE SET NULL
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_miper (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        faena_id INTEGER,
        proceso TEXT,
        tarea TEXT,
        cargo TEXT,
        peligro TEXT NOT NULL,
        riesgo TEXT NOT NULL,
        consecuencia TEXT,
        controles_existentes TEXT,
        probabilidad INTEGER DEFAULT 1,
        severidad INTEGER DEFAULT 1,
        nivel_riesgo INTEGER DEFAULT 1,
        medidas TEXT,
        responsable TEXT,
        plazo TEXT,
        estado TEXT NOT NULL DEFAULT 'PENDIENTE',
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE SET NULL
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_inspecciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        faena_id INTEGER,
        tipo TEXT,
        area TEXT,
        item TEXT NOT NULL,
        resultado TEXT NOT NULL DEFAULT 'OBSERVACIÓN',
        observacion TEXT,
        accion_correctiva TEXT,
        responsable TEXT,
        plazo TEXT,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE SET NULL
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_incidentes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        faena_id INTEGER,
        fecha TEXT NOT NULL,
        tipo TEXT NOT NULL,
        gravedad TEXT,
        descripcion TEXT NOT NULL,
        organismo_admin TEXT,
        dias_perdidos INTEGER DEFAULT 0,
        medidas TEXT,
        estado TEXT NOT NULL DEFAULT 'PENDIENTE',
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE SET NULL,
        FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE SET NULL
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_capacitaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trabajador_id INTEGER,
        faena_id INTEGER,
        tipo TEXT NOT NULL,
        tema TEXT NOT NULL,
        fecha TEXT NOT NULL,
        vigencia TEXT,
        horas REAL,
        relator TEXT,
        estado TEXT NOT NULL DEFAULT 'VIGENTE',
        evidencia TEXT,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE SET NULL,
        FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE SET NULL
    );
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS sgsst_auditoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        modulo TEXT NOT NULL,
        accion TEXT NOT NULL,
        detalle TEXT,
        usuario TEXT,
        created_at TEXT NOT NULL
    );
    ''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_sgsst_programa_faena_id ON sgsst_programa_anual(faena_id);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sgsst_miper_faena_id ON sgsst_miper(faena_id);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sgsst_inspecciones_faena_id ON sgsst_inspecciones(faena_id);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sgsst_incidentes_faena_id ON sgsst_incidentes(faena_id);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sgsst_capacitaciones_faena_id ON sgsst_capacitaciones(faena_id);")


def ensure_sgsst_seed_data():
    try:
        if int(fetch_value("SELECT COUNT(*) FROM sgsst_empresa", default=0) or 0) == 0:
            execute(
                """
                INSERT INTO sgsst_empresa(razon_social, rut, direccion, actividad, organismo_admin, representantes, prevencionista, canal_denuncias, dotacion_total, politica_version, politica_fecha, observaciones, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "Empresa demo",
                    "",
                    "",
                    "General",
                    "Organismo administrador",
                    "",
                    "",
                    "",
                    0,
                    "1.0",
                    date.today().isoformat(),
                    "Base inicial de SEGAV ERP / SGSST configurable para cualquier empresa.",
                    datetime.now().isoformat(timespec='seconds'),
                    datetime.now().isoformat(timespec='seconds'),
                ),
            )
        existing = fetch_df("SELECT norma, tema, obligacion FROM sgsst_matriz_legal")
        existing_keys = set()
        if existing is not None and not existing.empty:
            existing_keys = set(
                (str(r[0] or ""), str(r[1] or ""), str(r[2] or ""))
                for r in existing[["norma", "tema", "obligacion"]].itertuples(index=False, name=None)
            )
        for item in SGSST_MATRIZ_BASE:
            key = (item["norma"], item["tema"], item["obligacion"])
            if key in existing_keys:
                continue
            execute(
                """
                INSERT INTO sgsst_matriz_legal(norma, articulo, tema, obligacion, aplica_a, periodicidad, responsable, evidencia, estado, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    item.get("norma"), item.get("articulo"), item.get("tema"), item.get("obligacion"), item.get("aplica_a"),
                    item.get("periodicidad"), item.get("responsable"), item.get("evidencia"), item.get("estado"),
                    datetime.now().isoformat(timespec='seconds'), datetime.now().isoformat(timespec='seconds'),
                ),
            )
    except Exception:
        pass



def ensure_segav_erp_tables():
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS segav_erp_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS segav_erp_cargos (
            cargo_key TEXT PRIMARY KEY,
            cargo_label TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            activo INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS segav_erp_docs_cargo (
            cargo_key TEXT NOT NULL,
            doc_tipo TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (cargo_key, doc_tipo)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS segav_erp_docs_empresa (
            doc_tipo TEXT PRIMARY KEY,
            obligatorio INTEGER NOT NULL DEFAULT 1,
            mensual INTEGER NOT NULL DEFAULT 1,
            por_mandante INTEGER NOT NULL DEFAULT 1,
            por_faena INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS segav_erp_templates (
            template_key TEXT PRIMARY KEY,
            template_label TEXT NOT NULL,
            vertical TEXT,
            description TEXT,
            payload_json TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            activo INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS segav_erp_clientes (
            cliente_key TEXT PRIMARY KEY,
            cliente_nombre TEXT NOT NULL,
            rut TEXT,
            vertical TEXT,
            modo_implementacion TEXT,
            activo INTEGER NOT NULL DEFAULT 1,
            contacto TEXT,
            email TEXT,
            observaciones TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS segav_erp_parametros_cliente (
            cliente_key TEXT NOT NULL,
            param_key TEXT NOT NULL,
            param_value TEXT,
            updated_at TEXT,
            PRIMARY KEY (cliente_key, param_key)
        );
        """,
    ]
    for s in stmts:
        execute(s)


def set_segav_erp_config_value(key: str, value: str):
    now = datetime.now().isoformat(timespec='seconds')
    execute("DELETE FROM segav_erp_config WHERE config_key=?", (key,))
    execute("INSERT INTO segav_erp_config(config_key, config_value, updated_at) VALUES(?,?,?)", (key, str(value), now))


def ensure_segav_erp_seed_data():
    now = datetime.now().isoformat(timespec='seconds')
    defaults = {
        'erp_name': 'SEGAV ERP',
        'erp_slogan': 'ERP comercializable de cumplimiento, prevención y operación documental',
        'erp_vertical': 'General',
        'multiempresa': 'SI',
        'cliente_actual': 'Empresa actual',
        'modo_implementacion': 'DESDE_CERO',
        'template_actual': 'GENERAL',
    }
    for k, v in defaults.items():
        execute(
            "INSERT INTO segav_erp_config(config_key, config_value, updated_at) SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM segav_erp_config WHERE config_key=?)",
            (k, v, now, k),
        )

    if int(fetch_value("SELECT COUNT(*) FROM segav_erp_cargos", default=0) or 0) == 0:
        for idx, cargo in enumerate(CARGO_DOCS_ORDER, start=1):
            execute(
                "INSERT INTO segav_erp_cargos(cargo_key, cargo_label, sort_order, activo, updated_at) VALUES(?,?,?,?,?)",
                (cargo, cargo, idx, 1, now),
            )

    if int(fetch_value("SELECT COUNT(*) FROM segav_erp_docs_cargo", default=0) or 0) == 0:
        for cargo, docs in CARGO_DOCS_RULES.items():
            for idx, doc_tipo in enumerate(list(dict.fromkeys(docs)), start=1):
                execute(
                    "INSERT INTO segav_erp_docs_cargo(cargo_key, doc_tipo, sort_order, updated_at) VALUES(?,?,?,?)",
                    (cargo, doc_tipo, idx, now),
                )

    if int(fetch_value("SELECT COUNT(*) FROM segav_erp_docs_empresa", default=0) or 0) == 0:
        for idx, doc_tipo in enumerate(DOC_EMPRESA_MENSUALES, start=1):
            execute(
                "INSERT INTO segav_erp_docs_empresa(doc_tipo, obligatorio, mensual, por_mandante, por_faena, sort_order, updated_at) VALUES(?,?,?,?,?,?,?)",
                (doc_tipo, 1, 1, 1, 1, idx, now),
            )

    if int(fetch_value("SELECT COUNT(*) FROM segav_erp_templates", default=0) or 0) == 0:
        for idx, (template_key, payload) in enumerate(ERP_TEMPLATE_PRESETS.items(), start=1):
            execute(
                "INSERT INTO segav_erp_templates(template_key, template_label, vertical, description, payload_json, sort_order, activo, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (template_key, payload.get('label') or template_key, payload.get('vertical') or '', payload.get('description') or '', json.dumps(payload, ensure_ascii=False), idx, 1, now),
            )

    if int(fetch_value("SELECT COUNT(*) FROM segav_erp_clientes", default=0) or 0) == 0:
        empresa = fetch_df("SELECT razon_social, rut FROM sgsst_empresa ORDER BY id LIMIT 1")
        razon = 'Empresa actual'
        rut = ''
        if empresa is not None and not empresa.empty:
            razon = str(empresa.iloc[0].get('razon_social') or razon)
            rut = clean_rut(empresa.iloc[0].get('rut') or '')
        cliente_nombre = _bootstrap_config_value('cliente_actual', razon) or razon
        cliente_key = make_erp_key(cliente_nombre, prefix='cli_')
        execute(
            "INSERT INTO segav_erp_clientes(cliente_key, cliente_nombre, rut, vertical, modo_implementacion, activo, contacto, email, observaciones, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (cliente_key, cliente_nombre, rut, _bootstrap_config_value('erp_vertical', 'General'), _bootstrap_config_value('modo_implementacion', 'CONFIGURABLE'), 1, '', '', 'Cliente inicial sembrado desde la configuración actual.', now, now),
        )
        for param_key, param_value in ERP_CLIENT_PARAM_DEFAULTS.items():
            execute(
                "INSERT INTO segav_erp_parametros_cliente(cliente_key, param_key, param_value, updated_at) VALUES(?,?,?,?)",
                (cliente_key, param_key, str(param_value), now),
            )
        if not _bootstrap_config_value('current_client_key', ''):
            set_segav_erp_config_value('current_client_key', cliente_key)
            set_segav_erp_config_value('cliente_actual', cliente_nombre)

    # asegura cliente actual y parámetros base
    cliente_df = fetch_df("SELECT cliente_key, cliente_nombre FROM segav_erp_clientes WHERE COALESCE(activo,1)=1 ORDER BY cliente_nombre")
    if cliente_df is not None and not cliente_df.empty:
        current_key = _bootstrap_config_value('current_client_key', '')
        if not current_key or current_key not in cliente_df['cliente_key'].astype(str).tolist():
            current_key = str(cliente_df.iloc[0].get('cliente_key'))
            set_segav_erp_config_value('current_client_key', current_key)
            set_segav_erp_config_value('cliente_actual', str(cliente_df.iloc[0].get('cliente_nombre') or 'Empresa actual'))
        for _, row in cliente_df.iterrows():
            ckey = str(row.get('cliente_key') or '')
            if not ckey:
                continue
            for param_key, param_value in ERP_CLIENT_PARAM_DEFAULTS.items():
                execute(
                    "INSERT INTO segav_erp_parametros_cliente(cliente_key, param_key, param_value, updated_at) SELECT ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM segav_erp_parametros_cliente WHERE cliente_key=? AND param_key=?)",
                    (ckey, param_key, str(param_value), now, ckey, param_key),
                )



def ensure_storage_columns_postgres():
    if DB_BACKEND != "postgres":
        return
    stmts = [
        "ALTER TABLE IF EXISTS trabajador_documentos ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS trabajador_documentos ADD COLUMN IF NOT EXISTS object_path TEXT;",
        "ALTER TABLE IF EXISTS empresa_documentos ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS empresa_documentos ADD COLUMN IF NOT EXISTS object_path TEXT;",
        "ALTER TABLE IF EXISTS faena_empresa_documentos ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS faena_empresa_documentos ADD COLUMN IF NOT EXISTS object_path TEXT;",
        "ALTER TABLE IF EXISTS faena_empresa_documentos ADD COLUMN IF NOT EXISTS mandante_id BIGINT;",
        "ALTER TABLE IF EXISTS faena_empresa_documentos ADD COLUMN IF NOT EXISTS periodo_anio INTEGER;",
        "ALTER TABLE IF EXISTS faena_empresa_documentos ADD COLUMN IF NOT EXISTS periodo_mes INTEGER;",
        "ALTER TABLE IF EXISTS faena_anexos ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS faena_anexos ADD COLUMN IF NOT EXISTS object_path TEXT;",
        "ALTER TABLE IF EXISTS contratos_faena ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS contratos_faena ADD COLUMN IF NOT EXISTS object_path TEXT;",
        "ALTER TABLE IF EXISTS export_historial ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS export_historial ADD COLUMN IF NOT EXISTS object_path TEXT;",
        "ALTER TABLE IF EXISTS export_historial_mes ADD COLUMN IF NOT EXISTS bucket TEXT;",
        "ALTER TABLE IF EXISTS export_historial_mes ADD COLUMN IF NOT EXISTS object_path TEXT;",
    ]
    for s in stmts:
        try:
            execute(s)
        except Exception:
            pass


def sync_postgres_identity_sequence(table: str, pk: str = "id"):
    """Sincroniza la secuencia/identity de Postgres con el MAX(pk) real de la tabla."""
    if DB_BACKEND != "postgres":
        return
    import re as _re
    table = str(table or "").strip()
    pk = str(pk or "id").strip()
    if not (_re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) and _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", pk)):
        raise ValueError("Nombre de tabla/columna inválido para sincronizar secuencia")
    sql = f"""
    SELECT setval(
        pg_get_serial_sequence('{table}', '{pk}'),
        COALESCE((SELECT MAX({pk}) + 1 FROM {table}), 1),
        false
    )
    """
    try:
        execute(sql)
    except Exception:
        # No bloquear la app por una secuencia no encontrada; solo evitar crash.
        pass


def sync_postgres_core_sequences():
    if DB_BACKEND != "postgres":
        return
    for _table in [
        "mandantes",
        "contratos_faena",
        "faenas",
        "faena_anexos",
        "trabajadores",
        "asignaciones",
        "trabajador_documentos",
        "empresa_documentos",
        "faena_empresa_documentos",
        "export_historial",
        "export_historial_mes",
        "sgsst_empresa",
        "sgsst_matriz_legal",
        "sgsst_programa_anual",
        "sgsst_miper",
        "sgsst_inspecciones",
        "sgsst_incidentes",
        "sgsst_capacitaciones",
        "sgsst_auditoria",
        "users",
    ]:
        sync_postgres_identity_sequence(_table, "id")




@st.cache_resource(show_spinner=False)
def bootstrap_once(db_backend: str, dsn_fingerprint: str):
    _ = (db_backend, dsn_fingerprint)
    ensure_dirs()
    init_db()
    ensure_segav_erp_tables()
    ensure_segav_erp_seed_data()
    return True


def show_bootstrap_error(exc: Exception):
    st.error("No se pudo iniciar la app por un problema de conexión a Postgres/Supabase.")
    st.code(str(exc))
    st.markdown("""
Revisa estos puntos en **Secrets** de Streamlit Cloud:
- `SUPABASE_DB_URL` sin comillas extras ni saltos de línea.
- O bien usa secretos separados: `SUPABASE_DB_HOST`, `SUPABASE_DB_PORT`, `SUPABASE_DB_NAME`, `SUPABASE_DB_USER`, `SUPABASE_DB_PASSWORD`.
- El puerto del pooler suele ser **6543** y requiere `sslmode=require`.
- Verifica que la contraseña no esté truncada.
- Si cambiaste la contraseña en Supabase, actualiza también Streamlit Cloud.
""")
    st.stop()


def bootstrap_app(db_backend: str, dsn_fingerprint: str):
    try:
        bootstrap_once(db_backend, dsn_fingerprint)
    except Exception as e:
        show_bootstrap_error(e)
