import os
import re
import io
import zipfile
import hashlib
import base64
import sqlite3
import shutil
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

# ----------------------------
# Config
# ----------------------------
st.set_page_config(page_title="Control Documental Faenas", layout="wide")

APP_NAME = "Control Documental de Faenas"
DB_PATH = "app.db"
UPLOAD_ROOT = "uploads"  # En Streamlit Community Cloud: filesystem NO es persistente garantizado entre reboots.

ESTADOS_FAENA = ["ACTIVA", "TERMINADA"]
DOC_OBLIGATORIOS = [
    "REGISTRO_EPP",
    "ENTREGA_RIOHS",
    "IRL",
    "CONTRATO_TRABAJO",
    "ANEXO_CONTRATO",
    "LIQUIDACIONES",
    "FINIQUITO",
]
DOC_EMPRESA_SUGERIDOS = [
    "CERTIFICADO_CUMPLIMIENTO_LABORAL",
    "CERTIFICADO_ACCIDENTABILIDAD",
    "OTROS",
]
DOC_EMPRESA_REQUERIDOS = [
    "CERTIFICADO_CUMPLIMIENTO_LABORAL",
    "CERTIFICADO_ACCIDENTABILIDAD",
]
REQ_DOCS_N = len(DOC_OBLIGATORIOS)

# ----------------------------
# Helpers
# ----------------------------
def inject_css():
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1rem; padding-bottom: 2rem;}
        /* Metric cards */
        div[data-testid="stMetric"]{
            padding: 14px 14px 10px 14px;
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 16px;
        }
        /* Dataframes */
        div[data-testid="stDataFrame"]{
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 14px;
            overflow: hidden;
        }
        /* Expander */
        details[data-testid="stExpander"]{
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 14px;
            padding: 6px 10px;
        }
        
/* Buttons */
div.stButton > button {
    border-radius: 14px !important;
    padding-top: 0.55rem !important;
    padding-bottom: 0.55rem !important;
}
/* Sidebar spacing */
section[data-testid="stSidebar"] .block-container {padding-top: 1rem;}

        
/* iOS-like look & feel */
html, body, [class*="css"]  {
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
section[data-testid="stSidebar"] { border-right: 1px solid rgba(49,51,63,0.12); }
section[data-testid="stSidebar"] .block-container { padding-top: 1rem; }

/* Cards */
.gyd-card {
    background: rgba(255,255,255,0.72);
    border: 1px solid rgba(49,51,63,0.10);
    border-radius: 18px;
    padding: 14px 16px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.06);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    margin-bottom: 12px;
}
.gyd-muted { opacity: 0.75; }

/* Buttons */
div.stButton > button, div.stDownloadButton > button {
    border-radius: 16px !important;
    padding: 0.62rem 0.9rem !important;
}

/* Tabs */
button[data-baseweb="tab"] {
    border-radius: 14px;
    margin-right: 6px;
    padding-left: 14px;
    padding-right: 14px;
}

/* Dataframe container */
[data-testid="stDataFrame"] {
    border-radius: 16px;
    border: 1px solid rgba(49,51,63,0.10);
    overflow: hidden;
}

/* Metric cards */
[data-testid="stMetric"] {
    border: 1px solid rgba(49,51,63,0.10);
    border-radius: 16px;
    padding: 10px 12px;
}

        </style>
        """,
        unsafe_allow_html=True,
    )

def ui_header(title: str, desc: str = ""):
    st.markdown(
        f"""
        <div class="gyd-card">
            <div style="font-size:1.35rem; font-weight:700; line-height:1.25;">{title}</div>
            {f'<div class="gyd-muted" style="margin-top:6px;">{desc}</div>' if desc else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )

def ui_tip(text: str):
    st.info(text, icon="ℹ️")

def safe_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "item"



def fetch_assigned_workers(faena_id: int):
    """Devuelve dataframe de trabajadores asignados a una faena."""
    return fetch_df(
        '''
        SELECT t.id, t.rut, t.apellidos, t.nombres, COALESCE(a.cargo_faena,'') AS cargo_faena, COALESCE(t.cargo,'') AS cargo
        FROM asignaciones a
        JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.faena_id=? AND a.estado='ACTIVA'
        ORDER BY t.apellidos, t.nombres
        ''',
        (int(faena_id),),
    )

def get_global_counts():
    """Devuelve conteos básicos para UI (tolerante a tablas vacías)."""
    out = {}
    pairs = [
        ("mandantes", "SELECT COUNT(*) AS n FROM mandantes"),
        ("contratos_faena", "SELECT COUNT(*) AS n FROM contratos_faena"),
        ("faenas", "SELECT COUNT(*) AS n FROM faenas"),
        ("faenas_activas", "SELECT COUNT(*) AS n FROM faenas WHERE estado='ACTIVA'"),
        ("trabajadores", "SELECT COUNT(*) AS n FROM trabajadores"),
        ("asignaciones", "SELECT COUNT(*) AS n FROM asignaciones"),
        ("docs", "SELECT COUNT(*) AS n FROM trabajador_documentos"),
        ("docs_empresa", "SELECT COUNT(*) AS n FROM empresa_documentos"),
        ("docs_empresa_faena", "SELECT COUNT(*) AS n FROM faena_empresa_documentos"),
        ("exports", "SELECT COUNT(*) AS n FROM export_historial"),
        ("exports_mes", "SELECT COUNT(*) AS n FROM export_historial_mes"),
    ]
    for key, sql in pairs:
        try:
            out[key] = int(fetch_df(sql)["n"].iloc[0])
        except Exception:
            out[key] = 0
    return out

def norm_col(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def clean_rut(rut: str) -> str:
    rut = (rut or "").strip().upper()
    rut = rut.replace(" ", "")
    return rut

def split_nombre_completo(nombre: str):
    nombre = (nombre or "").strip()
    if not nombre:
        return "", ""
    toks = [t for t in re.split(r"\s+", nombre) if t]
    if len(toks) >= 4:
        apellidos = " ".join(toks[-2:])
        nombres = " ".join(toks[:-2])
    elif len(toks) == 3:
        apellidos = toks[-1]
        nombres = " ".join(toks[:-1])
    elif len(toks) == 2:
        apellidos = toks[-1]
        nombres = toks[0]
    else:
        apellidos = ""
        nombres = toks[0]
    return nombres.strip(), apellidos.strip()

def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def ensure_dirs():
    os.makedirs(UPLOAD_ROOT, exist_ok=True)
    os.makedirs(os.path.join(UPLOAD_ROOT, "exports"), exist_ok=True)
    os.makedirs(os.path.join(UPLOAD_ROOT, "auto_backups"), exist_ok=True)

def conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        c.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    return c

def migrate_add_columns_if_missing(c, table: str, cols_sql: dict):
    info = c.execute(f"PRAGMA table_info({table});").fetchall()
    existing = {row[1] for row in info}
    for col, coltype in cols_sql.items():
        if col not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")

def init_db():
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
            doc_tipo TEXT NOT NULL,
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE
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

        c.commit()

def fetch_df(q: str, params=()):
    with conn() as c:
        return pd.read_sql_query(q, c, params=params)

def execute(q: str, params=()):
    with conn() as c:
        c.execute(q, params)
        c.commit()

def executemany(q: str, seq_params):
    with conn() as c:
        c.executemany(q, seq_params)
        c.commit()

def save_file(folder_parts, file_name: str, file_bytes: bytes):
    ensure_dirs()
    folder = os.path.join(UPLOAD_ROOT, *[str(x) for x in folder_parts])
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, file_name)
    with open(path, "wb") as f:
        f.write(file_bytes)
    return path

def trabajador_folder(apellidos: str, nombres: str, rut: str) -> str:
    return f"{safe_name(apellidos)}_{safe_name(nombres)}_{safe_name(rut)}"

def validate_faena_dates(inicio: date, termino, estado: str):
    errors = []
    if termino and termino < inicio:
        errors.append("La fecha de término no puede ser anterior a la fecha de inicio.")
    if estado == "TERMINADA" and not termino:
        errors.append("Si la faena está TERMINADA, debes indicar fecha término.")
    return errors

def pendientes_obligatorios(faena_id: int):
    asign = fetch_df('''
        SELECT t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
        FROM asignaciones a
        JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.faena_id=?
        ORDER BY t.apellidos, t.nombres
    ''', (faena_id,))
    out = {}
    if asign.empty:
        return out

    ids = asign["trabajador_id"].astype(int).tolist()
    docs_all = fetch_df(
        "SELECT trabajador_id, doc_tipo FROM trabajador_documentos WHERE trabajador_id IN (%s)" % ",".join(["?"]*len(ids)),
        tuple(ids),
    )

    docs_map = {}
    if not docs_all.empty:
        for tid, grp in docs_all.groupby("trabajador_id"):
            docs_map[int(tid)] = set(grp["doc_tipo"].tolist())

    for _, r in asign.iterrows():
        tid = int(r["trabajador_id"])
        label = f"{r['apellidos']} {r['nombres']} ({r['rut']})"
        have = docs_map.get(tid, set())
        missing = [d for d in DOC_OBLIGATORIOS if d not in have]
        out[label] = missing
    return out


def pendientes_empresa_faena(faena_id: int):
    """Lista de documentos de empresa requeridos faltantes para una faena."""
    df = fetch_df("SELECT DISTINCT doc_tipo FROM faena_empresa_documentos WHERE faena_id=?", (int(faena_id),))
    present = set(df["doc_tipo"].astype(str).tolist()) if not df.empty else set()
    missing = [d for d in DOC_EMPRESA_REQUERIDOS if d not in present]
    return missing


def faena_progress_table():

    faenas = fetch_df('''
        SELECT f.id AS faena_id, f.nombre AS faena, f.estado, f.fecha_inicio, f.fecha_termino,
               m.nombre AS mandante
        FROM faenas f
        JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        return faenas

    asg = fetch_df("SELECT faena_id, trabajador_id FROM asignaciones")
    if asg.empty:
        faenas["trabajadores"] = 0
        faenas["trab_ok"] = 0
        faenas["cobertura_docs_pct"] = 0.0
        faenas["faltantes_total"] = 0
        return faenas

    placeholders = ",".join(["?"]*len(DOC_OBLIGATORIOS))
    q = f'''
        SELECT a.faena_id, a.trabajador_id,
               COUNT(DISTINCT CASE WHEN d.doc_tipo IN ({placeholders}) THEN d.doc_tipo END) AS req_docs_present
        FROM asignaciones a
        LEFT JOIN trabajador_documentos d ON d.trabajador_id=a.trabajador_id
        GROUP BY a.faena_id, a.trabajador_id
    '''
    stats = fetch_df(q, tuple(DOC_OBLIGATORIOS))
    stats["req_docs_present"] = stats["req_docs_present"].fillna(0).astype(int)
    stats["worker_ok"] = (stats["req_docs_present"] >= REQ_DOCS_N).astype(int)

    agg = stats.groupby("faena_id").agg(
        trabajadores=("trabajador_id", "nunique"),
        trab_ok=("worker_ok", "sum"),
        req_docs_sum=("req_docs_present", "sum"),
    ).reset_index()

    agg["faltantes_total"] = (agg["trabajadores"] * REQ_DOCS_N) - agg["req_docs_sum"]
    agg["cobertura_docs_pct"] = (agg["req_docs_sum"] / (agg["trabajadores"] * REQ_DOCS_N)).where(agg["trabajadores"] > 0, 0.0) * 100.0

    out = faenas.merge(agg, how="left", on="faena_id")
    out["trabajadores"] = out["trabajadores"].fillna(0).astype(int)
    out["trab_ok"] = out["trab_ok"].fillna(0).astype(int)
    out["faltantes_total"] = out["faltantes_total"].fillna(0).astype(int)
    out["cobertura_docs_pct"] = out["cobertura_docs_pct"].fillna(0.0).round(1)
    return out

def parse_date_maybe(s):
    if s is None:
        return None
    if isinstance(s, (date, datetime)):
        return s.date() if isinstance(s, datetime) else s
    s = str(s).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None

def export_zip_for_faena(
    faena_id: int,
    include_global_empresa_docs: bool = True,
    include_contrato: bool = True,
    include_anexos: bool = True,
    include_empresa_faena: bool = True,
    include_trabajadores: bool = True,
    doc_types_empresa_global=None,
    doc_types_empresa_faena=None,
    doc_types_trabajador=None,
):
    faena = fetch_df('''
        SELECT f.*, m.nombre AS mandante_nombre, cf.nombre AS contrato_nombre, cf.file_path AS contrato_path
        FROM faenas f
        JOIN mandantes m ON m.id=f.mandante_id
        LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
        WHERE f.id = ?
    ''', (faena_id,))
    if faena.empty:
        raise ValueError("Faena no encontrada.")
    f = faena.iloc[0]

    # Filtros opcionales (None => incluye todo)
    _set_emp_glob = set(doc_types_empresa_global) if doc_types_empresa_global else None
    _set_emp_faena = set(doc_types_empresa_faena) if doc_types_empresa_faena else None
    _set_trab = set(doc_types_trabajador) if doc_types_trabajador else None

    buff = io.BytesIO()
    z = zipfile.ZipFile(buff, "w", zipfile.ZIP_DEFLATED)

    # Index + pendientes (se incluyen siempre como guía)
    pend_t = pendientes_obligatorios(faena_id)
    miss_emp = pendientes_empresa_faena(faena_id)

    idx = []
    idx.append(f"MANDANTE: {f['mandante_nombre']}")
    idx.append(f"FAENA: {f['nombre']}")
    idx.append(f"ESTADO: {f['estado']}")
    idx.append(f"INICIO: {f['fecha_inicio']} | TERMINO: {f['fecha_termino'] or '-'}")
    idx.append(f"UBICACION: {f['ubicacion'] or '-'}")
    idx.append(f"CONTRATO_FAENA: {f['contrato_nombre'] or '(sin contrato cargado)'}")
    idx.append("")
    idx.append("PENDIENTES DOCUMENTOS OBLIGATORIOS POR TRABAJADOR:")
    if not pend_t:
        idx.append("- (sin trabajadores asignados)")
    else:
        for k, missing in pend_t.items():
            if missing:
                idx.append(f"* {k}: faltan {', '.join(missing)}")
            else:
                idx.append(f"* {k}: OK")
    idx.append("")
    idx.append("PENDIENTES DOCUMENTOS EMPRESA (POR FAENA):")
    if miss_emp:
        idx.append(f"* faltan: {', '.join(miss_emp)}")
    else:
        idx.append("* OK")

    z.writestr("99_Index_Pendientes.txt", "\\n".join(idx))

    # 00_Contrato_Faena
    if include_contrato and f.get("contrato_path") and os.path.exists(f["contrato_path"]):
        fname = os.path.basename(f["contrato_path"])
        with open(f["contrato_path"], "rb") as fp:
            z.writestr(f"00_Contrato_Faena/{fname}", fp.read())

    # 01_Anexos_Faena
    if include_anexos:
        anexos = fetch_df("SELECT * FROM faena_anexos WHERE faena_id=? ORDER BY id", (faena_id,))
        for _, a in anexos.iterrows():
            src = a["file_path"]
            if src and os.path.exists(src):
                fname = os.path.basename(src)
                with open(src, "rb") as fp:
                    z.writestr(f"01_Anexos_Faena/{fname}", fp.read())

    # 02_Documentos_Empresa (global)
    if include_global_empresa_docs:
        edocs = fetch_df("SELECT * FROM empresa_documentos ORDER BY id")
        for _, d in edocs.iterrows():
            if _set_emp_glob is not None and str(d.get("doc_tipo", "")) not in _set_emp_glob:
                continue
            src = d["file_path"]
            if src and os.path.exists(src):
                fname = os.path.basename(src)
                tipo = safe_name(d["doc_tipo"])
                with open(src, "rb") as fp:
                    z.writestr(f"02_Documentos_Empresa/{tipo}/{fname}", fp.read())

    # 02_Documentos_Empresa_Faena (por faena)
    if include_empresa_faena:
        fedocs = fetch_df("SELECT * FROM faena_empresa_documentos WHERE faena_id=? ORDER BY id", (faena_id,))
        for _, d in fedocs.iterrows():
            if _set_emp_faena is not None and str(d.get("doc_tipo", "")) not in _set_emp_faena:
                continue
            src = d["file_path"]
            if src and os.path.exists(src):
                fname = os.path.basename(src)
                tipo = safe_name(d["doc_tipo"])
                with open(src, "rb") as fp:
                    z.writestr(f"02_Documentos_Empresa_Faena/{tipo}/{fname}", fp.read())

    # 03_Trabajadores
    if include_trabajadores:
        asign = fetch_df('''
            SELECT t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
            FROM asignaciones a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.faena_id=?
            ORDER BY t.apellidos, t.nombres
        ''', (faena_id,))
        for _, r in asign.iterrows():
            tid = int(r["trabajador_id"])
            tdir = trabajador_folder(r["apellidos"], r["nombres"], r["rut"])

            tdocs = fetch_df("SELECT * FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id", (tid,))
            for _, d in tdocs.iterrows():
                if _set_trab is not None and str(d.get("doc_tipo", "")) not in _set_trab:
                    continue
                src = d["file_path"]
                if src and os.path.exists(src):
                    fname = os.path.basename(src)
                    tipo = safe_name(d["doc_tipo"])
                    with open(src, "rb") as fp:
                        z.writestr(f"03_Trabajadores/{tdir}/{tipo}/{fname}", fp.read())

    z.close()
    buff.seek(0)
    return buff.getvalue(), str(f["nombre"])

def persist_export_mes(year_month: str, zip_bytes: bytes):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"mes_{year_month}_{ts}.zip"
    path = save_file(["exports", "mes"], fname, zip_bytes)
    sha = sha256_bytes(zip_bytes)
    size = len(zip_bytes)
    execute(
        "INSERT INTO export_historial_mes(year_month, file_path, sha256, size_bytes, created_at) VALUES(?,?,?,?,?)",
        (str(year_month), path, sha, int(size), datetime.utcnow().isoformat(timespec="seconds")),
    )
    return path

def export_zip_for_mes(year: int, month: int, include_global_empresa_docs: bool = True):
    """Exporta en un ZIP todas las faenas cuyo fecha_inicio cae dentro del mes (YYYY-MM)."""
    ym = f"{int(year):04d}-{int(month):02d}"
    faenas = fetch_df(
        '''
        SELECT f.id, f.nombre, f.estado, f.fecha_inicio, m.nombre AS mandante
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        WHERE substr(f.fecha_inicio,1,7)=?
        ORDER BY f.id DESC
        ''',
        (ym,),
    )
    if faenas.empty:
        raise ValueError("No hay faenas para ese mes.")

    buff = io.BytesIO()
    z = zipfile.ZipFile(buff, "w", zipfile.ZIP_DEFLATED)

    # Index del mes
    idx = []
    idx.append(f"EXPORT MENSUAL: {ym}")
    idx.append(f"FAENAS INCLUIDAS: {len(faenas)}")
    idx.append("")
    for _, r in faenas.iterrows():
        idx.append(f"- {int(r['id'])}: {r['mandante']} / {r['nombre']} ({r['estado']}) inicio {r['fecha_inicio']}")
    z.writestr(f"{ym}/00_Index_Mes.txt", "\n".join(idx))

    # Documentos empresa global (una sola vez)
    if include_global_empresa_docs:
        edocs = fetch_df("SELECT * FROM empresa_documentos ORDER BY id")
        for _, d in edocs.iterrows():
            src = d["file_path"]
            if src and os.path.exists(src):
                fname = os.path.basename(src)
                tipo = safe_name(d["doc_tipo"])
                with open(src, "rb") as fp:
                    z.writestr(f"{ym}/00_Documentos_Empresa_Global/{tipo}/{fname}", fp.read())

    # Cada faena dentro de carpeta
    for _, fr in faenas.iterrows():
        fid = int(fr["id"])
        fname_faena = safe_name(str(fr["nombre"]))
        prefix = f"{ym}/FAENA_{fid}_{fname_faena}"

        # Reutiliza export estándar, pero con prefijo y sin repetir docs empresa global
        faena = fetch_df(
            '''
            SELECT f.*, m.nombre AS mandante_nombre, cf.nombre AS contrato_nombre, cf.file_path AS contrato_path
            FROM faenas f
            JOIN mandantes m ON m.id=f.mandante_id
            LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
            WHERE f.id = ?
            ''',
            (fid,),
        )
        if faena.empty:
            continue
        f = faena.iloc[0]

        pend_t = pendientes_obligatorios(fid)
        miss_emp = pendientes_empresa_faena(fid)

        idx2 = []
        idx2.append(f"MANDANTE: {f['mandante_nombre']}")
        idx2.append(f"FAENA: {f['nombre']}")
        idx2.append(f"ESTADO: {f['estado']}")
        idx2.append(f"INICIO: {f['fecha_inicio']} | TERMINO: {f['fecha_termino'] or '-'}")
        idx2.append(f"UBICACION: {f['ubicacion'] or '-'}")
        idx2.append(f"CONTRATO_FAENA: {f['contrato_nombre'] or '(sin contrato cargado)'}")
        idx2.append("")
        idx2.append("PENDIENTES DOCUMENTOS OBLIGATORIOS POR TRABAJADOR:")
        if not pend_t:
            idx2.append("- (sin trabajadores asignados)")
        else:
            for k, missing in pend_t.items():
                if missing:
                    idx2.append(f"* {k}: faltan {', '.join(missing)}")
                else:
                    idx2.append(f"* {k}: OK")
        idx2.append("")
        idx2.append("PENDIENTES DOCUMENTOS EMPRESA (POR FAENA):")
        if miss_emp:
            idx2.append(f"* faltan: {', '.join(miss_emp)}")
        else:
            idx2.append("* OK")
        z.writestr(f"{prefix}/99_Index_Pendientes.txt", "\n".join(idx2))

        # 00 contrato
        if f.get("contrato_path") and os.path.exists(f["contrato_path"]):
            fn = os.path.basename(f["contrato_path"])
            with open(f["contrato_path"], "rb") as fp:
                z.writestr(f"{prefix}/00_Contrato_Faena/{fn}", fp.read())

        # 01 anexos
        anexos = fetch_df("SELECT * FROM faena_anexos WHERE faena_id=? ORDER BY id", (fid,))
        for _, a in anexos.iterrows():
            src = a["file_path"]
            if src and os.path.exists(src):
                fn = os.path.basename(src)
                with open(src, "rb") as fp:
                    z.writestr(f"{prefix}/01_Anexos_Faena/{fn}", fp.read())

        # 02 empresa por faena
        fedocs = fetch_df("SELECT * FROM faena_empresa_documentos WHERE faena_id=? ORDER BY id", (fid,))
        for _, d in fedocs.iterrows():
            src = d["file_path"]
            if src and os.path.exists(src):
                fn = os.path.basename(src)
                tipo = safe_name(d["doc_tipo"])
                with open(src, "rb") as fp:
                    z.writestr(f"{prefix}/02_Documentos_Empresa_Faena/{tipo}/{fn}", fp.read())

        # 03 trabajadores
        asign = fetch_df(
            '''
            SELECT t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
            FROM asignaciones a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.faena_id=?
            ORDER BY t.apellidos, t.nombres
            ''',
            (fid,),
        )
        for _, r in asign.iterrows():
            tid = int(r["trabajador_id"])
            tdir = trabajador_folder(r["apellidos"], r["nombres"], r["rut"])
            tdocs = fetch_df("SELECT * FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id", (tid,))
            for _, d in tdocs.iterrows():
                src = d["file_path"]
                if src and os.path.exists(src):
                    fn = os.path.basename(src)
                    tipo = safe_name(d["doc_tipo"])
                    with open(src, "rb") as fp:
                        z.writestr(f"{prefix}/03_Trabajadores/{tdir}/{tipo}/{fn}", fp.read())

    z.close()
    buff.seek(0)
    return buff.getvalue(), ym

def persist_export(faena_id: int, zip_bytes: bytes, faena_nombre: str):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"faena_{faena_id}_{safe_name(faena_nombre)}_{ts}.zip"
    path = save_file(["exports", f"faena_{faena_id}"], fname, zip_bytes)
    sha = sha256_bytes(zip_bytes)
    size = len(zip_bytes)
    execute(
        "INSERT INTO export_historial(faena_id, file_path, sha256, size_bytes, created_at) VALUES(?,?,?,?,?)",
        (int(faena_id), path, sha, int(size), datetime.utcnow().isoformat(timespec="seconds")),
    )
    return path

def make_backup_zip_bytes():
    buff = io.BytesIO()
    z = zipfile.ZipFile(buff, "w", zipfile.ZIP_DEFLATED)
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            z.writestr("backup/app.db", f.read())
    if os.path.exists(UPLOAD_ROOT):
        for root, _, files in os.walk(UPLOAD_ROOT):
            for fn in files:
                p = os.path.join(root, fn)
                arc = os.path.relpath(p, ".")
                z.write(p, arcname=f"backup/{arc}")
    z.writestr("backup/META.txt", f"created_at_utc={datetime.utcnow().isoformat(timespec='seconds')}\n")
    z.close()
    buff.seek(0)
    return buff.getvalue()


def restore_from_backup_zip(uploaded_bytes: bytes):
    """Restaura backup completo. Soporta formato actual (backup/app.db) y formatos antiguos si contienen algún .db."""
    tmp = f"_restore_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(tmp, exist_ok=True)
    try:
        with zipfile.ZipFile(io.BytesIO(uploaded_bytes), "r") as z:
            z.extractall(tmp)

        # 1) Buscar base de datos en distintas rutas (compatibilidad)
        candidates = [
            os.path.join(tmp, "backup", "app.db"),
            os.path.join(tmp, "app.db"),
            os.path.join(tmp, "backup", "DB", "app.db"),
            os.path.join(tmp, "data", "app.db"),
        ]

        # buscar cualquier .db/.sqlite dentro del ZIP
        for root, _, files in os.walk(tmp):
            for fn in files:
                low = fn.lower()
                if low.endswith((".db", ".sqlite", ".sqlite3")):
                    candidates.append(os.path.join(root, fn))

        db_candidate = next((p for p in candidates if p and os.path.exists(p)), None)

        # Caso típico de backups antiguos: zip de código sin base de datos
        if db_candidate is None:
            # heurística: si existe streamlit_app.py, es backup de código
            has_code = os.path.exists(os.path.join(tmp, "streamlit_app.py")) or os.path.exists(os.path.join(tmp, "backup", "streamlit_app.py"))
            if has_code:
                raise ValueError(
                    "Este ZIP parece ser un backup de CÓDIGO (incluye streamlit_app.py), pero NO contiene la base de datos (app.db). "
                    "Para restaurar datos necesitas un backup que incluya 'backup/app.db' (Backup completo) o subir un archivo .db en 'Base (app.db)'."
                )
            raise ValueError("El ZIP no contiene una base de datos (.db/.sqlite).")

        # 2) Reemplazar DB
        ensure_dirs()
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        os.replace(db_candidate, DB_PATH)

        # 3) Restaurar uploads si vienen (formatos: backup/uploads/... o uploads/...)
        #    Nota: en el formato actual los uploads quedan como backup/uploads/...
        up1 = os.path.join(tmp, "backup", UPLOAD_ROOT)
        up2 = os.path.join(tmp, UPLOAD_ROOT)
        up_candidate = up1 if os.path.exists(up1) else (up2 if os.path.exists(up2) else None)

        if up_candidate:
            if os.path.exists(UPLOAD_ROOT):
                shutil.rmtree(UPLOAD_ROOT, ignore_errors=True)
            shutil.copytree(up_candidate, UPLOAD_ROOT)

        # 4) Migraciones: crear tablas/columnas nuevas para compatibilidad
        init_db()
        ensure_dirs()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)



def make_db_only_bytes():
    if not os.path.exists(DB_PATH):
        init_db()
    with open(DB_PATH, "rb") as f:
        return f.read()

def cleanup_old_auto_backups(keep_last: int = 20):
    try:
        hist = fetch_df("SELECT id, file_path FROM auto_backup_historial ORDER BY id DESC")
        if hist.empty:
            return
        to_delete = hist.iloc[keep_last:]
        for _, r in to_delete.iterrows():
            p = r["file_path"]
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
        if not to_delete.empty:
            ids = tuple(int(x) for x in to_delete["id"].tolist())
            with conn() as c:
                c.execute("DELETE FROM auto_backup_historial WHERE id IN (%s)" % ",".join(["?"]*len(ids)), ids)
                c.commit()
    except Exception:
        pass

def auto_backup_db(tag: str = "auto"):
    """Backup automático SOLO de la base (app.db). El manual sigue siendo ZIP completo."""
    try:
        if not st.session_state.get("auto_backup_enabled", True):
            return

        db_bytes = make_db_only_bytes()
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"auto_db_{ts}_{safe_name(tag)}.db"
        path = save_file(["auto_backups"], fname, db_bytes)

        sha = sha256_bytes(db_bytes)
        size = len(db_bytes)

        execute(
            "INSERT INTO auto_backup_historial(tag, file_path, sha256, size_bytes, created_at) VALUES(?,?,?,?,?)",
            (tag, path, sha, int(size), datetime.utcnow().isoformat(timespec="seconds")),
        )
        cleanup_old_auto_backups(keep_last=20)

        st.session_state["last_auto_backup"] = {"name": fname, "bytes": db_bytes, "created_at": datetime.utcnow().isoformat(timespec="seconds")}
    except Exception:
        pass


def go(page: str, faena_id=None):
    """Navegación segura hacia otra página del sidebar.
    Evita setear directamente el key del widget 'nav_page' luego de renderizado.
    """
    st.session_state["nav_request"] = page
    if faena_id is not None:
        st.session_state["nav_request_faena_id"] = int(faena_id)
    st.rerun()

# ----------------------------
# Init
# ----------------------------
ensure_dirs()
init_db()

inject_css()

# ----------------------------
# Sidebar navigation
# ----------------------------
PAGES = [
    "Dashboard",
    "Mandantes",
    "Contratos de Faena",
    "Faenas",
    "Trabajadores",
    "Documentos Empresa",
    "Asignar Trabajadores",
    "Documentos Trabajador",
    "Export (ZIP)",
    "Backup / Restore",
]


# Aplica navegación solicitada por botones (antes de crear el widget del sidebar)
if st.session_state.get("nav_request") is not None:
    _req = st.session_state.get("nav_request")
    if _req in PAGES:
        st.session_state["nav_page"] = _req
    if st.session_state.get("nav_request_faena_id") is not None:
        st.session_state["selected_faena_id"] = int(st.session_state.get("nav_request_faena_id"))
    st.session_state.pop("nav_request", None)
    st.session_state.pop("nav_request_faena_id", None)

# Normaliza nav_page por si quedó un valor antiguo en session_state
if st.session_state.get("nav_page") not in PAGES:
    st.session_state["nav_page"] = "Dashboard"


if "nav_page" not in st.session_state:
    st.session_state["nav_page"] = "Dashboard"
with st.sidebar:
    st.markdown("### 🧾 Control documental de faenas")

    PAGE_LABELS = {
        "Dashboard": "📊 Dashboard",
        "Mandantes": "🏢 Mandantes",
        "Contratos de Faena": "📄 Contratos de Faena",
        "Faenas": "🛠️ Faenas",
        "Trabajadores": "👷 Trabajadores",
        "Documentos Empresa": "🏛️ Documentos Empresa",
        "Asignar Trabajadores": "🧩 Asignar Trabajadores",
        "Documentos Trabajador": "📎 Documentos Trabajador",
        "Export (ZIP)": "📦 Export (ZIP)",
        "Backup / Restore": "💾 Backup / Restore",
    }

    st.radio("Secciones", PAGES, key="nav_page", format_func=lambda x: PAGE_LABELS.get(x, x))

    st.divider()
    st.markdown("### 🔎 Contexto")
    try:
        _fa = fetch_df("""
            SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
            FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
            ORDER BY f.id DESC
        """)
    except Exception:
        _fa = pd.DataFrame()

    if not _fa.empty:
        default_id = st.session_state.get("selected_faena_id", int(_fa["id"].iloc[0]))
        opts = _fa["id"].tolist()
        if default_id not in opts:
            default_id = int(opts[0])
        idx = opts.index(default_id)
        faena_id = st.selectbox(
            "Faena seleccionada",
            opts,
            index=idx,
            format_func=lambda x: f"{x} - {_fa[_fa['id']==x].iloc[0]['mandante']} / {_fa[_fa['id']==x].iloc[0]['nombre']} ({_fa[_fa['id']==x].iloc[0]['estado']})",
        )
        st.session_state["selected_faena_id"] = int(faena_id)

        cctx1, cctx2 = st.columns(2)
        with cctx1:
            if st.button("📎 Docs", use_container_width=True):
                st.session_state["nav_page"] = "Documentos Trabajador"
                st.rerun()
        with cctx2:
            if st.button("📦 Export", use_container_width=True):
                st.session_state["nav_page"] = "Export (ZIP)"
                st.rerun()
    else:
        st.caption("(Aún no hay faenas para seleccionar)")

    st.divider()
    st.markdown("### ⚡ Resumen")
    counts = get_global_counts()
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Faenas", counts.get("faenas", 0))
        st.metric("Trabajadores", counts.get("trabajadores", 0))
    with c2:
        st.metric("Activas", counts.get("faenas_activas", 0))
        st.metric("Docs", counts.get("docs", 0))

    st.divider()
    st.markdown("### ➕ Atajos")
    cqa1, cqa2 = st.columns(2)
    with cqa1:
        if st.button("Mandante", use_container_width=True):
            st.session_state["nav_page"] = "Mandantes"
            st.rerun()
        if st.button("Faena", use_container_width=True):
            st.session_state["nav_page"] = "Faenas"
            st.rerun()
    with cqa2:
        if st.button("Trabajador", use_container_width=True):
            st.session_state["nav_page"] = "Trabajadores"
            st.rerun()
        if st.button("Asignar", use_container_width=True):
            st.session_state["nav_page"] = "Asignar Trabajadores"
            st.rerun()

    st.divider()
    st.markdown("### 💾 Respaldo")
    if "auto_backup_enabled" not in st.session_state:
        st.session_state["auto_backup_enabled"] = True
    st.checkbox("Auto-backup al guardar (solo app.db)", key="auto_backup_enabled")

    last = st.session_state.get("last_auto_backup")
    if last and last.get("bytes"):
        st.success("Auto-backup listo")
        st.download_button(
            "Descargar último auto-backup (app.db)",
            data=last["bytes"],
            file_name=last["name"],
            mime="application/octet-stream",
            use_container_width=True,
        )
        if st.button("Limpiar aviso", use_container_width=True):
            st.session_state.pop("last_auto_backup", None)
            st.rerun()

    st.caption("⚠️ En Streamlit Community Cloud, el disco puede perderse en reboots/redeploy. Usa Backup/Restore para respaldos completos.")

current_section = st.session_state.get("nav_page", "Dashboard")
st.title(f"{APP_NAME} — {current_section}")



# ----------------------------
# Pages
# ----------------------------
def page_dashboard():
    ui_header("Dashboard", "Centro de control: pendientes, acciones rápidas y estado documental (estilo app).")

    counts = get_global_counts()
    mand_n = counts.get("mandantes", 0)
    faena_n = counts.get("faenas", 0)
    fa_act = counts.get("faenas_activas", 0)
    trab_n = counts.get("trabajadores", 0)
    docs_emp = counts.get("docs_empresa", 0)

    # Contexto de vista
    df_prog = faena_progress_table()

    # Top KPIs (compactos)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Faenas", faena_n)
    c2.metric("Activas", fa_act)
    c3.metric("Trabajadores", trab_n)
    c4.metric("Mandantes", mand_n)
    c5.metric("Docs empresa", docs_emp)

    st.markdown('<div class="gyd-card">', unsafe_allow_html=True)
    view = st.radio("Vista", ["🌐 Global", "🛠️ Faena"], horizontal=True, label_visibility="collapsed")

    selected_faena_id = st.session_state.get("selected_faena_id")
    if view == "🛠️ Faena":
        if df_prog.empty:
            st.info("No hay faenas para seleccionar.")
            st.markdown("</div>", unsafe_allow_html=True)
            return

        show = df_prog.rename(columns={"faena_id": "id", "faena": "faena_nombre"})
        if selected_faena_id not in show["id"].tolist():
            selected_faena_id = int(show["id"].iloc[0])
            st.session_state["selected_faena_id"] = selected_faena_id

        faena_id = st.selectbox(
            "Faena",
            show["id"].tolist(),
            index=show["id"].tolist().index(selected_faena_id),
            format_func=lambda x: f"{int(x)} - {show[show['id']==x].iloc[0]['mandante']} / {show[show['id']==x].iloc[0]['faena_nombre']} ({show[show['id']==x].iloc[0]['estado']})",
        )
        st.session_state["selected_faena_id"] = int(faena_id)

        row = show[show["id"] == int(faena_id)].iloc[0]
        cc1, cc2, cc3, cc4 = st.columns(4)
        cc1.metric("Cobertura docs %", int(round(float(row["cobertura_docs_pct"] or 0), 0)))
        cc2.metric("Faltantes", int(row["faltantes_total"] or 0))
        cc3.metric("Trabajadores", int(row["trabajadores"] or 0))
        cc4.metric("OK", int(row["trab_ok"] or 0))

        ab1, ab2, ab3 = st.columns(3)
        with ab1:
            if st.button("📎 Cargar docs trabajador", use_container_width=True):
                go("Documentos Trabajador", faena_id=int(faena_id))
        with ab2:
            if st.button("📦 Exportar ZIP", type="primary", use_container_width=True):
                go("Export (ZIP)", faena_id=int(faena_id))
        with ab3:
            if st.button("🧩 Asignar trabajadores", use_container_width=True):
                go("Asignar Trabajadores", faena_id=int(faena_id))

    st.markdown("</div>", unsafe_allow_html=True)

    # Acciones rápidas (estilo app)
    st.markdown("### ⚡ Acciones rápidas")
    st.markdown('<div class="gyd-card">', unsafe_allow_html=True)
    qa1, qa2, qa3, qa4 = st.columns(4)
    with qa1:
        if st.button("🏢 Nuevo mandante", use_container_width=True):
            go("Mandantes")
    with qa2:
        if st.button("📄 Nuevo contrato", use_container_width=True):
            go("Contratos de Faena")
    with qa3:
        if st.button("🛠️ Nueva faena", use_container_width=True):
            go("Faenas")
    with qa4:
        if st.button("👷 Importar trabajadores", use_container_width=True):
            go("Trabajadores")
    st.markdown("</div>", unsafe_allow_html=True)

    # Inbox de gestión (críticas/pendientes + CTA)
    st.markdown("### 📥 Inbox de gestión")
    st.caption("Lo importante primero. Tarjetas accionables para resolver rápido.")

    if df_prog.empty:
        ui_tip("Aún no hay faenas. Crea una faena para empezar.")
        return

    def semaforo(r):
        tr = int(r.get("trabajadores", 0) or 0)
        pct = float(r.get("cobertura_docs_pct", 0) or 0)
        falt = int(r.get("faltantes_total", 0) or 0)
        if tr == 0:
            return "CRITICO"
        if falt == 0 and pct >= 100:
            return "OK"
        if pct >= 70:
            return "PENDIENTE"
        return "CRITICO"

    inbox = df_prog.copy()
    inbox["semaforo"] = inbox.apply(semaforo, axis=1)
    inbox = inbox.rename(columns={"faena_id":"id", "faena":"faena_nombre"})

    # Críticas y pendientes (Top 5)
    crit = inbox[inbox["semaforo"]=="CRITICO"].sort_values(["faltantes_total","cobertura_docs_pct"], ascending=[False, True]).head(5)
    pend = inbox[inbox["semaforo"]=="PENDIENTE"].sort_values(["faltantes_total","cobertura_docs_pct"], ascending=[False, True]).head(5)

    colA, colB = st.columns(2)

    with colA:
        st.markdown('<div class="gyd-card">', unsafe_allow_html=True)
        st.markdown("#### 🔴 Críticas (resolver hoy)")
        if crit.empty:
            st.success("No hay faenas críticas 🎉")
        else:
            for _, r in crit.iterrows():
                st.markdown(f"**{int(r['id'])} — {r['mandante']} / {r['faena_nombre']}**")
                st.caption(f"Cobertura: {int(round(float(r['cobertura_docs_pct'] or 0),0))}% · Faltantes: {int(r['faltantes_total'] or 0)} · Trabajadores: {int(r['trabajadores'] or 0)}")
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("📎 Resolver docs", key=f"cta_docs_crit_{int(r['id'])}", use_container_width=True):
                        go("Export (ZIP)", faena_id=int(r["id"]))
                with b2:
                    if st.button("🧩 Asignar", key=f"cta_asg_crit_{int(r['id'])}", use_container_width=True):
                        go("Asignar Trabajadores", faena_id=int(r["id"]))
                st.divider()
        st.markdown("</div>", unsafe_allow_html=True)

    with colB:
        st.markdown('<div class="gyd-card">', unsafe_allow_html=True)
        st.markdown("#### 🟡 Pendientes (subir a OK)")
        if pend.empty:
            st.info("No hay faenas pendientes.")
        else:
            for _, r in pend.iterrows():
                st.markdown(f"**{int(r['id'])} — {r['mandante']} / {r['faena_nombre']}**")
                st.caption(f"Cobertura: {int(round(float(r['cobertura_docs_pct'] or 0),0))}% · Faltantes: {int(r['faltantes_total'] or 0)}")
                b1, b2 = st.columns(2)
                with b1:
                    if st.button("📎 Docs", key=f"cta_docs_pen_{int(r['id'])}", use_container_width=True):
                        go("Documentos Trabajador", faena_id=int(r["id"]))
                with b2:
                    if st.button("📦 Export", key=f"cta_exp_pen_{int(r['id'])}", type="primary", use_container_width=True):
                        go("Export (ZIP)", faena_id=int(r["id"]))
                st.divider()
        st.markdown("</div>", unsafe_allow_html=True)

    # Gráficos mínimos (2 max)
    st.markdown("### 📊 Gráficos (mínimos)")
    st.markdown('<div class="gyd-card">', unsafe_allow_html=True)
    g1, g2 = st.columns(2)

    with g1:
        st.markdown("**Faenas por estado**")
        s = inbox.groupby("estado")["id"].count().rename("cantidad")
        st.bar_chart(s)

    with g2:
        st.markdown("**Cobertura promedio por mandante**")
        mdf = inbox.groupby("mandante")["cobertura_docs_pct"].mean().sort_values(ascending=False).rename("cobertura_promedio_%")
        st.bar_chart(mdf)

    st.markdown("</div>", unsafe_allow_html=True)

    # Acceso a detalles (sin llenar de tablas)
    st.markdown("### 🔎 Ver detalle")
    d1, d2, d3 = st.columns(3)
    with d1:
        if st.button("📋 Ver listado de faenas (semáforo)", use_container_width=True):
            go("Faenas")
    with d2:
        if st.button("🏛️ Ver documentos empresa", use_container_width=True):
            go("Documentos Empresa")
    with d3:
        if st.button("💾 Backup / Restore", use_container_width=True):
            go("Backup / Restore")


def page_mandantes():
    ui_header("Mandantes", "Registra mandantes. Cada faena se asocia a un mandante. Aquí puedes crear, editar y revisar su avance.")

    # KPIs rápidos
    k1, k2, k3 = st.columns(3)
    with k1:
        try:
            st.metric("Mandantes", int(fetch_df("SELECT COUNT(*) AS n FROM mandantes")["n"].iloc[0]))
        except Exception:
            st.metric("Mandantes", 0)
    with k2:
        try:
            st.metric("Contratos de faena", int(fetch_df("SELECT COUNT(*) AS n FROM contratos_faena")["n"].iloc[0]))
        except Exception:
            st.metric("Contratos de faena", 0)
    with k3:
        try:
            st.metric("Faenas", int(fetch_df("SELECT COUNT(*) AS n FROM faenas")["n"].iloc[0]))
        except Exception:
            st.metric("Faenas", 0)

    tab_over, tab_create, tab_manage = st.tabs(["📌 Overview", "➕ Crear", "✏️ Editar / 🗑️ Eliminar"])

    # -------------------------
    # Tab Overview
    # -------------------------
    with tab_over:
        df = fetch_df('''
            SELECT
                m.id,
                m.nombre,
                (SELECT COUNT(*) FROM contratos_faena cf WHERE cf.mandante_id=m.id) AS contratos,
                (SELECT COUNT(*) FROM faenas f WHERE f.mandante_id=m.id) AS faenas_total,
                (SELECT COUNT(*) FROM faenas f WHERE f.mandante_id=m.id AND f.estado='ACTIVA') AS faenas_activas
            FROM mandantes m
            ORDER BY m.id DESC
        ''')

        q = st.text_input("Buscar mandante", placeholder="Escribe nombre…", key="mand_q")
        out = df.copy()
        if q.strip():
            qq = q.strip().lower()
            out = out[out["nombre"].astype(str).str.lower().str.contains(qq, na=False)]

        c1, c2 = st.columns([2, 1])
        with c1:
            st.dataframe(
                out.rename(columns={
                    "nombre": "Mandante",
                    "contratos": "Contratos",
                    "faenas_total": "Faenas",
                    "faenas_activas": "Activas",
                }),
                use_container_width=True,
                hide_index=True,
            )
        with c2:
            st.markdown("#### Detalle rápido")
            if out.empty:
                st.info("Sin resultados.")
            else:
                mid = st.selectbox("Mandante", out["id"].tolist(),
                                   format_func=lambda x: out[out["id"]==x].iloc[0]["nombre"],
                                   key="mand_detail_sel")
                row = df[df["id"] == mid].iloc[0]
                st.metric("Contratos", int(row["contratos"]))
                st.metric("Faenas", int(row["faenas_total"]))
                st.metric("Faenas activas", int(row["faenas_activas"]))
                # Lista corta de faenas
                fa = fetch_df('''
                    SELECT id, nombre, estado, fecha_inicio, fecha_termino
                    FROM faenas
                    WHERE mandante_id=?
                    ORDER BY id DESC
                    LIMIT 10
                ''', (int(mid),))
                if fa.empty:
                    st.caption("Sin faenas asociadas.")
                else:
                    st.caption("Últimas faenas (máx 10)")
                    st.dataframe(
                        fa.rename(columns={
                            "nombre":"Faena", "estado":"Estado",
                            "fecha_inicio":"Inicio", "fecha_termino":"Término"
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )

    # -------------------------
    # Tab Crear
    # -------------------------
    with tab_create:
        with st.form("form_mandante", clear_on_submit=True):
            nombre = st.text_input("Nombre mandante", placeholder="Bosque Los Lagos", key="mandante_nombre_in")
            ok = st.form_submit_button("Guardar mandante", type="primary")
        if ok:
            nombre_clean = (nombre or "").strip()
            if not nombre_clean:
                st.warning("Ingresa un nombre de mandante.")
            else:
                try:
                    execute("INSERT INTO mandantes(nombre) VALUES(?)", (nombre_clean,))
                    st.success("Mandante creado.")
                    auto_backup_db("mandante")
                    st.rerun()
                except Exception as e:
                    msg = str(e)
                    if "UNIQUE" in msg.upper():
                        st.error("Ya existe un mandante con ese nombre.")
                    else:
                        st.error(f"No se pudo crear: {e}")

    # -------------------------
    # Tab Editar / Eliminar
    # -------------------------
    with tab_manage:
        df_all = fetch_df("SELECT id, nombre FROM mandantes ORDER BY id DESC")
        if df_all.empty:
            st.info("No hay mandantes para gestionar.")
        else:
            mid = st.selectbox(
                "Selecciona mandante",
                df_all["id"].tolist(),
                format_func=lambda x: df_all[df_all["id"]==x].iloc[0]["nombre"],
                key="mand_manage_sel",
            )
            row = df_all[df_all["id"] == mid].iloc[0]

            st.markdown("### ✏️ Editar")
            with st.form("form_mand_edit"):
                nombre_new = st.text_input("Nombre", value=str(row["nombre"] or ""), key="mand_name_new")
                ok_upd = st.form_submit_button("Guardar cambios", type="primary")
            if ok_upd:
                nn = (nombre_new or "").strip()
                if not nn:
                    st.error("El nombre no puede estar vacío.")
                else:
                    try:
                        execute("UPDATE mandantes SET nombre=? WHERE id=?", (nn, int(mid)))
                        st.success("Mandante actualizado.")
                        auto_backup_db("mandante_edit")
                        st.rerun()
                    except Exception as e:
                        msg = str(e)
                        if "UNIQUE" in msg.upper():
                            st.error("Ya existe un mandante con ese nombre.")
                        else:
                            st.error(f"No se pudo actualizar: {e}")

            st.divider()
            st.markdown("### 🗑️ Eliminar")
            dep = fetch_df("SELECT COUNT(*) AS n FROM faenas WHERE mandante_id=?", (int(mid),))
            n_faenas = int(dep["n"].iloc[0]) if not dep.empty else 0
            if n_faenas > 0:
                st.warning(f"No se puede eliminar porque tiene {n_faenas} faena(s) asociada(s). Primero reasigna o elimina esas faenas.")
            else:
                confirm = st.checkbox("Confirmo que deseo eliminar este mandante", key="mand_del_confirm")
                if st.button("Eliminar mandante definitivamente", type="secondary", key="mand_del_btn"):
                    if not confirm:
                        st.error("Debes confirmar antes de eliminar.")
                        st.stop()
                    try:
                        execute("DELETE FROM mandantes WHERE id=?", (int(mid),))
                        st.success("Mandante eliminado.")
                        auto_backup_db("mandante_delete")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo eliminar: {e}")

def page_contratos_faena():
    ui_header("Contratos de Faena", "Crea, edita o elimina contratos por mandante. Puedes adjuntar archivo al contrato.")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        ui_tip("Primero crea un mandante.")
        return

    tab1, tab2 = st.tabs(["➕ Crear contrato", "✏️ Editar / Eliminar / Archivo"])

    with tab1:
        with st.form("form_contrato_faena", clear_on_submit=False):
            mandante_id = st.selectbox(
                "Mandante",
                mand["id"].tolist(),
                format_func=lambda x: mand[mand["id"] == x].iloc[0]["nombre"],
            )
            nombre = st.text_input("Nombre contrato de faena", placeholder="Contrato Faena Bellavista")
            fi = st.date_input("Fecha inicio (opcional)", value=None)
            ft = st.date_input("Fecha término (opcional)", value=None)
            archivo = st.file_uploader("Archivo contrato (opcional)", key="up_contrato_faena", type=None)
            ok = st.form_submit_button("Guardar contrato de faena", type="primary")

        if ok:
            if not nombre.strip():
                st.error("Debes ingresar un nombre para el contrato de faena.")
                st.stop()
            try:
                file_path = None
                sha = None
                created_at = datetime.utcnow().isoformat(timespec="seconds")
                if archivo is not None:
                    b = archivo.getvalue()
                    file_path = save_file(["contratos_faena", mandante_id], archivo.name, b)
                    sha = sha256_bytes(b)

                execute(
                    "INSERT INTO contratos_faena(mandante_id, nombre, fecha_inicio, fecha_termino, file_path, sha256, created_at) VALUES(?,?,?,?,?,?,?)",
                    (int(mandante_id), nombre.strip(), str(fi) if fi else None, str(ft) if ft else None, file_path, sha, created_at),
                )
                st.success("Contrato de faena creado.")
                auto_backup_db("contrato_faena")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    with tab2:
        df = fetch_df('''
            SELECT cf.id, cf.mandante_id, m.nombre AS mandante, cf.nombre, cf.fecha_inicio, cf.fecha_termino, cf.file_path,
                   CASE WHEN cf.file_path IS NULL THEN '(sin archivo)' ELSE 'OK' END AS archivo
            FROM contratos_faena cf
            JOIN mandantes m ON m.id=cf.mandante_id
            ORDER BY cf.id DESC
        ''')

        if df.empty:
            st.info("No hay contratos.")
            return

        st.markdown("### 📋 Contratos existentes")
        st.dataframe(df.drop(columns=["file_path","mandante_id"]), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ✏️ Editar datos del contrato")

        contrato_id = st.selectbox(
            "Selecciona contrato",
            df["id"].tolist(),
            format_func=lambda x: f"{x} - {df[df['id']==x].iloc[0]['mandante']} / {df[df['id']==x].iloc[0]['nombre']}",
            key="sel_contrato_edit",
        )
        row = df[df["id"] == contrato_id].iloc[0]

        with st.form("form_edit_contrato"):
            mandante_id_new = st.selectbox(
                "Mandante (cambiar)",
                mand["id"].tolist(),
                index=mand["id"].tolist().index(int(row["mandante_id"])) if int(row["mandante_id"]) in mand["id"].tolist() else 0,
                format_func=lambda x: mand[mand["id"] == x].iloc[0]["nombre"],
            )
            nombre_new = st.text_input("Nombre", value=str(row["nombre"]))
            fi_new = st.date_input("Fecha inicio (opcional)", value=parse_date_maybe(row["fecha_inicio"]))
            ft_new = st.date_input("Fecha término (opcional)", value=parse_date_maybe(row["fecha_termino"]))
            upd = st.form_submit_button("Guardar cambios", type="primary")

        if upd:
            if not nombre_new.strip():
                st.error("El nombre no puede estar vacío.")
                st.stop()
            try:
                execute(
                    "UPDATE contratos_faena SET mandante_id=?, nombre=?, fecha_inicio=?, fecha_termino=? WHERE id=?",
                    (int(mandante_id_new), nombre_new.strip(), str(fi_new) if fi_new else None, str(ft_new) if ft_new else None, int(contrato_id)),
                )
                st.success("Contrato actualizado.")
                auto_backup_db("contrato_edit")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar: {e}")

        st.divider()
        st.markdown("### 📎 Archivo del contrato")

        up = st.file_uploader("Subir / reemplazar archivo", key="up_contrato_existente", type=None)
        cfa1, cfa2 = st.columns([1, 1])
        with cfa1:
            if st.button("Guardar archivo", type="primary", use_container_width=True):
                if up is None:
                    st.error("Debes subir un archivo primero.")
                    st.stop()
                b = up.getvalue()
                file_path = save_file(["contratos_faena", "id", contrato_id], up.name, b)
                sha = sha256_bytes(b)
                execute(
                    "UPDATE contratos_faena SET file_path=?, sha256=?, created_at=? WHERE id=?",
                    (file_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(contrato_id)),
                )
                st.success("Archivo actualizado.")
                auto_backup_db("contrato_archivo")
                st.rerun()
        with cfa2:
            # descarga si existe
            current_path = row.get("file_path")
            if current_path and os.path.exists(str(current_path)):
                with open(str(current_path), "rb") as fp:
                    st.download_button(
                        "Descargar archivo actual",
                        data=fp.read(),
                        file_name=os.path.basename(str(current_path)),
                        mime="application/octet-stream",
                        use_container_width=True,
                    )
            else:
                st.button("Descargar archivo actual", disabled=True, use_container_width=True)

        st.divider()
        st.markdown("### 🗑️ Eliminar contrato")
        st.caption("Si este contrato está asociado a faenas existentes, al eliminarlo esas faenas quedarán con contrato en blanco (contrato_faena_id = NULL).")

        # Dependencias
        dep = fetch_df("SELECT COUNT(*) AS n FROM faenas WHERE contrato_faena_id=?", (int(contrato_id),))
        dep_n = int(dep["n"].iloc[0]) if not dep.empty else 0

        st.warning(f"Faenas asociadas a este contrato: {dep_n}")

        confirm = st.checkbox("Confirmo que deseo eliminar este contrato", key="chk_del_contrato")
        if st.button("Eliminar contrato definitivamente", type="secondary"):
            if not confirm:
                st.error("Debes confirmar el checkbox antes de eliminar.")
                st.stop()
            try:
                # primero desvincular (por seguridad explícita, aunque FK está ON DELETE SET NULL)
                execute("UPDATE faenas SET contrato_faena_id=NULL WHERE contrato_faena_id=?", (int(contrato_id),))
                execute("DELETE FROM contratos_faena WHERE id=?", (int(contrato_id),))
                st.success("Contrato eliminado.")
                auto_backup_db("contrato_delete")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo eliminar: {e}")

def page_faenas():
    ui_header("Faenas", "Crea, edita y gestiona faenas por mandante. Registra fechas/estado y carga anexos si aplica.")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        ui_tip("Primero crea un mandante.")
        return

    contratos = fetch_df('''
        SELECT cf.id, cf.nombre, cf.mandante_id, m.nombre AS mandante
        FROM contratos_faena cf
        JOIN mandantes m ON m.id=cf.mandante_id
        ORDER BY m.nombre, cf.nombre
    ''')

    tab1, tab2, tab3, tab4 = st.tabs(["➕ Crear faena", "📋 Listado (semáforo)", "📎 Anexos", "✏️ Editar / Eliminar"])

    # ------------------
    # Tab 1: crear
    # ------------------
    with tab1:
        mandante_id = st.selectbox(
            "Mandante",
            mand["id"].tolist(),
            format_func=lambda x: mand[mand["id"] == x].iloc[0]["nombre"],
            key="faena_mandante_sel",
        )

        contratos_m = contratos[contratos["mandante_id"] == mandante_id] if not contratos.empty else pd.DataFrame()
        contrato_opts = [None] + (contratos_m["id"].tolist() if not contratos_m.empty else [])

        def _fmt_contrato(x):
            if x is None:
                return "(sin contrato asociado)"
            row = contratos[contratos["id"] == x]
            if row.empty:
                return str(x)
            return f"{int(x)} - {row.iloc[0]['nombre']}"

        with st.form("form_faena"):
            contrato_id = st.selectbox("Contrato de faena (opcional)", contrato_opts, format_func=_fmt_contrato)
            nombre = st.text_input("Nombre faena", placeholder="Bellavista 3")
            ubicacion = st.text_input("Ubicación", placeholder="Predio / Comuna")
            fi = st.date_input("Fecha inicio", value=date.today())
            ft = st.date_input("Fecha término (opcional)", value=None)
            estado = st.selectbox("Estado", ESTADOS_FAENA, index=0)

            errors = validate_faena_dates(fi, ft, estado)
            if errors:
                st.warning("Revisar: " + " | ".join(errors))

            ok = st.form_submit_button("Guardar faena", type="primary")

        if ok:
            if not nombre.strip():
                st.error("Debes ingresar un nombre para la faena.")
                st.stop()
            if errors:
                st.error("Corrige las fechas/estado antes de guardar la faena.")
                st.stop()
            try:
                execute(
                    "INSERT INTO faenas(mandante_id, contrato_faena_id, nombre, ubicacion, fecha_inicio, fecha_termino, estado) VALUES(?,?,?,?,?,?,?)",
                    (int(mandante_id), int(contrato_id) if contrato_id else None, nombre.strip(), ubicacion.strip(), str(fi), str(ft) if ft else None, estado),
                )
                st.success("Faena creada.")
                auto_backup_db("faena")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    # ------------------
    # Tab 2: listado
    # ------------------
    with tab2:
        df = faena_progress_table()
        if df.empty:
            st.info("No hay faenas aún.")
        else:
            out = df.copy()

            def _semaforo(r):
                try:
                    tr = int(r.get("trabajadores", 0) or 0)
                    pct = float(r.get("cobertura_docs_pct", 0) or 0)
                    falt = int(r.get("faltantes_total", 0) or 0)
                except Exception:
                    tr, pct, falt = 0, 0, 0

                if tr == 0:
                    return "🔴 CRÍTICO"
                if falt == 0 and pct >= 100:
                    return "🟢 OK"
                if pct >= 70:
                    return "🟡 PENDIENTE"
                return "🔴 CRÍTICO"

            out["estado_docs"] = out.apply(_semaforo, axis=1)
            out["cobertura_%"] = out["cobertura_docs_pct"].round(0).astype(int)

            show = out.rename(columns={"faena_id": "id", "faena": "faena_nombre"})
            show = show[["estado_docs", "id", "mandante", "faena_nombre", "estado", "fecha_inicio", "fecha_termino", "trabajadores", "trab_ok", "cobertura_%", "faltantes_total"]].copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.caption("Regla semáforo: 🔴 sin trabajadores o cobertura <70% | 🟡 ≥70% con faltantes | 🟢 100% sin faltantes.")

            colq1, colq2, colq3 = st.columns([2, 1, 1])
            with colq1:
                fid = st.selectbox(
                    "Acción rápida: seleccionar faena",
                    show["id"].tolist(),
                    format_func=lambda x: f"{int(x)} - {show[show['id']==x].iloc[0]['mandante']} / {show[show['id']==x].iloc[0]['faena_nombre']}",
                )
            with colq2:
                if st.button("Ir a Docs", use_container_width=True):
                    st.session_state["selected_faena_id"] = int(fid)
                    st.session_state["nav_page"] = "Documentos Trabajador"
                    st.rerun()
            with colq3:
                if st.button("Ir a Export", type="primary", use_container_width=True):
                    st.session_state["selected_faena_id"] = int(fid)
                    st.session_state["nav_page"] = "Export (ZIP)"
                    st.rerun()

    # ------------------
    # Tab 3: anexos
    # ------------------
    with tab3:
        base = fetch_df('''
            SELECT f.id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio, f.fecha_termino, f.ubicacion,
                   COALESCE(cf.nombre, '') AS contrato_faena
            FROM faenas f
            JOIN mandantes m ON m.id=f.mandante_id
            LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
            ORDER BY f.id DESC
        ''')

        if base.empty:
            st.info("No hay faenas.")
            return

        faena_id = st.selectbox(
            "Faena",
            base["id"].tolist(),
            format_func=lambda x: f"{x} - {base[base['id']==x].iloc[0]['mandante']} / {base[base['id']==x].iloc[0]['nombre']}",
        )
        st.session_state["selected_faena_id"] = int(faena_id)

        st.markdown("### Subir anexo")
        up = st.file_uploader("Archivo anexo", key="up_anexo_faena", type=None)
        if st.button("Guardar anexo", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()
            b = up.getvalue()
            file_path = save_file(["faenas", faena_id, "anexos"], up.name, b)
            sha = sha256_bytes(b)
            execute(
                "INSERT INTO faena_anexos(faena_id, nombre, file_path, sha256, created_at) VALUES(?,?,?,?,?)",
                (int(faena_id), up.name, file_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            st.success("Anexo guardado.")
            auto_backup_db("anexo_faena")
            st.rerun()

        anexos = fetch_df("SELECT id, nombre, created_at FROM faena_anexos WHERE faena_id=? ORDER BY id DESC", (int(faena_id),))
        st.caption("Anexos cargados")
        st.dataframe(anexos if not anexos.empty else pd.DataFrame([{"info": "(sin anexos)"}]), use_container_width=True)

    # ------------------
    # Tab 4: editar/eliminar
    # ------------------
    with tab4:
        base = fetch_df('''
            SELECT f.id, f.mandante_id, m.nombre AS mandante, f.nombre, f.ubicacion, f.fecha_inicio, f.fecha_termino, f.estado,
                   f.contrato_faena_id, COALESCE(cf.nombre,'') AS contrato_nombre
            FROM faenas f
            JOIN mandantes m ON m.id=f.mandante_id
            LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
            ORDER BY f.id DESC
        ''')
        if base.empty:
            st.info("No hay faenas para editar.")
            return

        fid = st.selectbox(
            "Selecciona faena",
            base["id"].tolist(),
            format_func=lambda x: f"{int(x)} - {base[base['id']==x].iloc[0]['mandante']} / {base[base['id']==x].iloc[0]['nombre']} ({base[base['id']==x].iloc[0]['estado']})",
            key="faena_edit_sel",
        )
        st.session_state["selected_faena_id"] = int(fid)
        row = base[base["id"] == int(fid)].iloc[0]

        # Contratos compatibles con el mandante de la faena (o ninguno)
        contratos_m = contratos[contratos["mandante_id"] == int(row["mandante_id"])] if not contratos.empty else pd.DataFrame()
        contrato_opts = [None] + (contratos_m["id"].tolist() if not contratos_m.empty else [])

        def _fmt_contrato2(x):
            if x is None:
                return "(sin contrato asociado)"
            r2 = contratos_m[contratos_m["id"] == x]
            if r2.empty:
                return str(x)
            return f"{int(x)} - {r2.iloc[0]['nombre']}"

        default_c = None if pd.isna(row["contrato_faena_id"]) else int(row["contrato_faena_id"])
        contrato_index = contrato_opts.index(default_c) if default_c in contrato_opts else 0

        st.markdown("### ✏️ Editar faena")
        with st.form("form_edit_faena"):
            nombre_new = st.text_input("Nombre", value=str(row["nombre"] or ""))
            ubic_new = st.text_input("Ubicación", value=str(row["ubicacion"] or ""))
            fi_new = st.date_input("Fecha inicio", value=parse_date_maybe(row["fecha_inicio"]) or date.today())
            ft_new = st.date_input("Fecha término (opcional)", value=parse_date_maybe(row["fecha_termino"]))
            estado_new = st.selectbox("Estado", ESTADOS_FAENA, index=ESTADOS_FAENA.index(str(row["estado"])) if str(row["estado"]) in ESTADOS_FAENA else 0)
            contrato_new = st.selectbox("Contrato de faena (opcional)", contrato_opts, index=contrato_index, format_func=_fmt_contrato2)

            errors = validate_faena_dates(fi_new, ft_new, estado_new)
            if errors:
                st.warning("Revisar: " + " | ".join(errors))

            ok_upd = st.form_submit_button("Guardar cambios", type="primary")

        if ok_upd:
            if not nombre_new.strip():
                st.error("El nombre no puede estar vacío.")
                st.stop()
            if errors:
                st.error("Corrige las fechas/estado antes de guardar.")
                st.stop()
            try:
                execute(
                    "UPDATE faenas SET nombre=?, ubicacion=?, fecha_inicio=?, fecha_termino=?, estado=?, contrato_faena_id=? WHERE id=?",
                    (
                        nombre_new.strip(),
                        ubic_new.strip(),
                        str(fi_new),
                        str(ft_new) if ft_new else None,
                        estado_new,
                        int(contrato_new) if contrato_new else None,
                        int(fid),
                    ),
                )
                st.success("Faena actualizada.")
                auto_backup_db("faena_edit")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar: {e}")

        st.divider()
        st.markdown("### 🗑️ Eliminar faena")
        st.caption("Se eliminará la faena y sus anexos/asignaciones asociadas. Los trabajadores NO se eliminan.")

        dep1 = fetch_df("SELECT COUNT(*) AS n FROM asignaciones WHERE faena_id=?", (int(fid),))
        dep2 = fetch_df("SELECT COUNT(*) AS n FROM faena_anexos WHERE faena_id=?", (int(fid),))
        n_asg = int(dep1["n"].iloc[0]) if not dep1.empty else 0
        n_anx = int(dep2["n"].iloc[0]) if not dep2.empty else 0

        st.warning(f"Dependencias: {n_asg} asignaciones · {n_anx} anexos")

        confirm = st.checkbox("Confirmo que deseo eliminar esta faena", key="chk_del_faena")
        if st.button("Eliminar faena definitivamente", type="secondary"):
            if not confirm:
                st.error("Debes confirmar el checkbox antes de eliminar.")
                st.stop()
            try:
                execute("DELETE FROM faena_anexos WHERE faena_id=?", (int(fid),))
                execute("DELETE FROM asignaciones WHERE faena_id=?", (int(fid),))
                execute("DELETE FROM faenas WHERE id=?", (int(fid),))
                st.success("Faena eliminada.")
                auto_backup_db("faena_delete")
                # limpiar selección
                st.session_state["selected_faena_id"] = None
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo eliminar: {e}")

def page_trabajadores():
    ui_header("Trabajadores", "Carga masiva por Excel o gestión manual. Puedes crear, editar o eliminar trabajadores. Luego asigna a faenas y adjunta documentos.")
    tab1, tab2, tab3 = st.tabs(["📥 Importar Excel", "🧩 Gestión", "📋 Listado"])

    # -------------------------
    # Tab 1: Importación Excel
    # -------------------------
    with tab1:
        st.write("Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        up = st.file_uploader("Sube Excel (.xlsx)", type=["xlsx"], key="up_excel_trabajadores")
        if up is not None:
            try:
                xls = pd.ExcelFile(up)
                sheet = st.selectbox("Hoja", xls.sheet_names, index=0, key="sheet_excel_trab")
                raw = pd.read_excel(xls, sheet_name=sheet)

                colmap = {c: norm_col(str(c)) for c in raw.columns}
                df = raw.rename(columns=colmap).copy()

                st.caption("Vista previa (primeras 10 filas)")
                st.dataframe(df.head(10), use_container_width=True)

                if "rut" not in df.columns or "nombre" not in df.columns:
                    st.error("El Excel debe tener columnas 'RUT' y 'NOMBRE'.")
                else:
                    overwrite = st.checkbox("Sobrescribir si el RUT ya existe", value=True, key="ow_excel_trab")

                    if st.button("Importar Excel ahora", type="primary", key="btn_import_excel_trab"):
                        existing_set = set(fetch_df("SELECT rut FROM trabajadores")["rut"].astype(str).tolist())

                        rows = inserted = updated = skipped = 0
                        has_cargo = "cargo" in df.columns
                        has_cc = "centro_costo" in df.columns
                        has_email = "email" in df.columns
                        fc_col = "fecha_de_contrato" if "fecha_de_contrato" in df.columns else ("fecha_contrato" if "fecha_contrato" in df.columns else None)
                        has_ve = "vigencia_examen" in df.columns

                        def _to_text_date(v):
                            if v is None or pd.isna(v):
                                return None
                            if isinstance(v, datetime):
                                return str(v.date())
                            if isinstance(v, date):
                                return str(v)
                            return str(v)

                        with conn() as c:
                            c.execute("PRAGMA foreign_keys = ON;")
                            for _, r in df.iterrows():
                                rows += 1
                                rut = clean_rut(str(r.get("rut", "") or ""))
                                nombre = str(r.get("nombre", "") or "").strip()

                                if not rut or rut.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue
                                if not nombre or nombre.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue

                                nombres, apellidos = split_nombre_completo(nombre)
                                cargo = str(r.get("cargo", "") or "").strip() if has_cargo else ""
                                centro_costo = str(r.get("centro_costo", "") or "").strip() if has_cc else ""
                                email = str(r.get("email", "") or "").strip() if has_email else ""
                                fecha_contrato = _to_text_date(r.get(fc_col)) if fc_col else None
                                vigencia_examen = _to_text_date(r.get("vigencia_examen")) if has_ve else None

                                if overwrite:
                                    c.execute(
                                        '''
                                        INSERT INTO trabajadores(rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)
                                        VALUES(?,?,?,?,?,?,?,?)
                                        ON CONFLICT(rut) DO UPDATE SET
                                            nombres=excluded.nombres,
                                            apellidos=excluded.apellidos,
                                            cargo=excluded.cargo,
                                            centro_costo=excluded.centro_costo,
                                            email=excluded.email,
                                            fecha_contrato=excluded.fecha_contrato,
                                            vigencia_examen=excluded.vigencia_examen
                                        ''',
                                        (rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen),
                                    )
                                    if rut in existing_set:
                                        updated += 1
                                    else:
                                        inserted += 1
                                        existing_set.add(rut)
                                else:
                                    if rut in existing_set:
                                        skipped += 1
                                        continue
                                    c.execute(
                                        '''
                                        INSERT INTO trabajadores(rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)
                                        VALUES(?,?,?,?,?,?,?,?)
                                        ''',
                                        (rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen),
                                    )
                                    inserted += 1
                                    existing_set.add(rut)

                            c.commit()

                        st.success(f"Importación lista. Filas leídas: {rows} | Insertados: {inserted} | Actualizados: {updated} | Omitidos: {skipped}")
                        auto_backup_db("import_excel")
                        st.rerun()
            except Exception as e:
                st.error(f"No se pudo leer/importar el Excel: {e}")

    # -------------------------
    # Tab 2: Gestión (crear/editar/eliminar)
    # -------------------------
    with tab2:
        t_create, t_edit = st.tabs(["➕ Crear", "✏️ Editar / 🗑️ Eliminar"])

        with t_create:
            with st.form("form_trabajador_manual"):
                rut = st.text_input("RUT", placeholder="12.345.678-9")
                nombres = st.text_input("Nombres", placeholder="Juan")
                apellidos = st.text_input("Apellidos", placeholder="Pérez")
                cargo = st.text_input("Cargo", placeholder="Operador Harvester")
                centro_costo = st.text_input("Centro de costo (opcional)", placeholder="FAENA")
                email = st.text_input("Email (opcional)")
                fecha_contrato = st.date_input("Fecha de contrato (opcional)", value=None)
                vigencia_examen = st.date_input("Vigencia examen (opcional)", value=None)
                ok = st.form_submit_button("Guardar trabajador", type="primary")

            if ok:
                if not (rut.strip() and nombres.strip() and apellidos.strip()):
                    st.error("Debes completar RUT, Nombres y Apellidos.")
                    st.stop()
                try:
                    execute(
                        '''
                        INSERT INTO trabajadores(rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)
                        VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(rut) DO UPDATE SET
                            nombres=excluded.nombres,
                            apellidos=excluded.apellidos,
                            cargo=excluded.cargo,
                            centro_costo=excluded.centro_costo,
                            email=excluded.email,
                            fecha_contrato=excluded.fecha_contrato,
                            vigencia_examen=excluded.vigencia_examen
                        ''',
                        (clean_rut(rut), nombres.strip(), apellidos.strip(), cargo.strip(), centro_costo.strip(), email.strip(),
                         str(fecha_contrato) if fecha_contrato else None,
                         str(vigencia_examen) if vigencia_examen else None),
                    )
                    st.success("Trabajador guardado.")
                    auto_backup_db("trabajador")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo guardar: {e}")

        with t_edit:
            df = fetch_df("SELECT id, rut, apellidos, nombres, cargo, centro_costo, email, fecha_contrato, vigencia_examen FROM trabajadores ORDER BY apellidos, nombres")
            if df.empty:
                st.info("No hay trabajadores aún.")
                return

            def _fmt(x):
                r = df[df["id"] == x].iloc[0]
                return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

            tid = st.selectbox("Selecciona trabajador", df["id"].tolist(), format_func=_fmt, key="trab_edit_sel")
            row = df[df["id"] == tid].iloc[0]

            st.markdown("### ✏️ Editar trabajador")
            with st.form("form_trabajador_edit"):
                rut_new = st.text_input("RUT", value=str(row["rut"] or ""))
                nombres_new = st.text_input("Nombres", value=str(row["nombres"] or ""))
                apellidos_new = st.text_input("Apellidos", value=str(row["apellidos"] or ""))
                cargo_new = st.text_input("Cargo", value=str(row["cargo"] or ""))
                cc_new = st.text_input("Centro de costo (opcional)", value=str(row["centro_costo"] or ""))
                email_new = st.text_input("Email (opcional)", value=str(row["email"] or ""))
                fc_new = st.date_input("Fecha de contrato (opcional)", value=parse_date_maybe(row["fecha_contrato"]))
                ve_new = st.date_input("Vigencia examen (opcional)", value=parse_date_maybe(row["vigencia_examen"]))
                ok_upd = st.form_submit_button("Guardar cambios", type="primary")

            if ok_upd:
                if not (rut_new.strip() and nombres_new.strip() and apellidos_new.strip()):
                    st.error("Debes completar RUT, Nombres y Apellidos.")
                    st.stop()
                try:
                    execute(
                        "UPDATE trabajadores SET rut=?, nombres=?, apellidos=?, cargo=?, centro_costo=?, email=?, fecha_contrato=?, vigencia_examen=? WHERE id=?",
                        (
                            clean_rut(rut_new),
                            nombres_new.strip(),
                            apellidos_new.strip(),
                            cargo_new.strip(),
                            cc_new.strip(),
                            email_new.strip(),
                            str(fc_new) if fc_new else None,
                            str(ve_new) if ve_new else None,
                            int(tid),
                        ),
                    )
                    st.success("Trabajador actualizado.")
                    auto_backup_db("trabajador_edit")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo actualizar: {e}")

            st.divider()
            st.markdown("### 🗑️ Eliminar trabajador")
            st.caption("Se eliminarán también sus asignaciones a faenas y sus documentos. Esto NO elimina archivos físicos en disco (solo la base de datos).")

            dep_asg = fetch_df("SELECT COUNT(*) AS n FROM asignaciones WHERE trabajador_id=?", (int(tid),))
            dep_docs = fetch_df("SELECT COUNT(*) AS n FROM trabajador_documentos WHERE trabajador_id=?", (int(tid),))
            dep_faenas = fetch_df("SELECT COUNT(DISTINCT faena_id) AS n FROM asignaciones WHERE trabajador_id=?", (int(tid),))

            n_asg = int(dep_asg["n"].iloc[0]) if not dep_asg.empty else 0
            n_docs = int(dep_docs["n"].iloc[0]) if not dep_docs.empty else 0
            n_faenas = int(dep_faenas["n"].iloc[0]) if not dep_faenas.empty else 0

            st.warning(f"Dependencias: {n_asg} asignaciones (en {n_faenas} faenas) · {n_docs} documentos")

            confirm = st.checkbox("Confirmo que deseo eliminar este trabajador", key="chk_del_trab")
            if st.button("Eliminar trabajador definitivamente", type="secondary", key="btn_del_trab"):
                if not confirm:
                    st.error("Debes confirmar el checkbox antes de eliminar.")
                    st.stop()
                try:
                    execute("DELETE FROM asignaciones WHERE trabajador_id=?", (int(tid),))
                    execute("DELETE FROM trabajador_documentos WHERE trabajador_id=?", (int(tid),))
                    execute("DELETE FROM trabajadores WHERE id=?", (int(tid),))
                    st.success("Trabajador eliminado.")
                    auto_backup_db("trabajador_delete")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo eliminar: {e}")

    # -------------------------
    # Tab 3: Listado
    # -------------------------
    with tab3:
        df = fetch_df("SELECT id, rut, apellidos, nombres, cargo, centro_costo, email, fecha_contrato, vigencia_examen FROM trabajadores ORDER BY id DESC")
        q = st.text_input("Buscar", placeholder="RUT, nombre o cargo", key="q_trab_list")
        out = df.copy()
        if q.strip():
            qq = q.strip().lower()
            out = out[
                out["rut"].astype(str).str.lower().str.contains(qq, na=False) |
                out["apellidos"].astype(str).str.lower().str.contains(qq, na=False) |
                out["nombres"].astype(str).str.lower().str.contains(qq, na=False) |
                out["cargo"].astype(str).str.lower().str.contains(qq, na=False)
            ]
        st.dataframe(out, use_container_width=True, hide_index=True)
        st.caption("Para editar/eliminar: ve a la pestaña **Gestión → Editar / Eliminar**.")

def page_asignar_trabajadores():
    ui_header("Asignar Trabajadores", "Carga e incorpora trabajadores por faena. Si un trabajador se repite en otra faena, mantiene su documentación ya cargada.")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        ui_tip("Crea faenas primero.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        faena_id = st.selectbox(
            "Faena",
            faenas["id"].tolist(),
            format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']}",
        )
    with col2:
        st.session_state["selected_faena_id"] = int(faena_id)

    tab1, tab2, tab3 = st.tabs(["🧩 Asignar existentes", "📥 Importar Excel y asignar", "📋 Asignados"])

    # -------------------------
    # Tab 1: asignar existentes
    # -------------------------
    with tab1:
        trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
        if trab.empty:
            ui_tip("Crea trabajadores primero (o usa 'Importar Excel y asignar').")
            return

        asignados = fetch_df("SELECT trabajador_id FROM asignaciones WHERE faena_id=?", (int(faena_id),))
        asignados_ids = set(asignados["trabajador_id"].tolist()) if not asignados.empty else set()
        disponibles = trab[~trab["id"].isin(asignados_ids)].copy()

        def _fmt_trab(x):
            r = trab[trab["id"] == x].iloc[0]
            return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

        st.markdown("#### Agregar asignaciones")
        if disponibles.empty:
            st.success("Todos los trabajadores ya están asignados.")
        else:
            with st.form("form_asignar"):
                seleccion = st.multiselect("Selecciona trabajadores", disponibles["id"].tolist(), format_func=_fmt_trab)
                fecha_ingreso = st.date_input("Fecha ingreso", value=date.today())
                cargo_faena = st.text_input("Cargo en faena (opcional, aplica a todos)")
                ok = st.form_submit_button("Asignar seleccionados", type="primary")

            if ok:
                if len(seleccion) == 0:
                    st.error("Selecciona al menos un trabajador para asignar.")
                    st.stop()
                params = []
                for tid in seleccion:
                    params.append((int(faena_id), int(tid), cargo_faena.strip(), str(fecha_ingreso), None, "ACTIVA"))
                executemany(
                    "INSERT OR IGNORE INTO asignaciones(faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado) VALUES(?,?,?,?,?,?)",
                    params,
                )
                st.success("Trabajadores asignados.")
                auto_backup_db("asignacion")
                st.rerun()

    # ---------------------------------
    # Tab 2: importar Excel y asignar
    # ---------------------------------
    with tab2:
        st.write("Sube Excel de trabajadores para **esta faena**. Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        up = st.file_uploader("Sube Excel (.xlsx)", type=["xlsx"], key="up_excel_trab_por_faena")
        if up is not None:
            try:
                xls = pd.ExcelFile(up)
                sheet = st.selectbox("Hoja", xls.sheet_names, index=0, key="sheet_trab_por_faena")
                raw = pd.read_excel(xls, sheet_name=sheet)

                colmap = {c: norm_col(str(c)) for c in raw.columns}
                df = raw.rename(columns=colmap).copy()

                st.caption("Vista previa (primeras 10 filas)")
                st.dataframe(df.head(10), use_container_width=True)

                if "rut" not in df.columns or "nombre" not in df.columns:
                    st.error("El Excel debe tener columnas 'RUT' y 'NOMBRE'.")
                else:
                    overwrite = st.checkbox("Sobrescribir datos si el RUT ya existe", value=True, key="ow_trab_por_faena")
                    fecha_ingreso = st.date_input("Fecha ingreso para esta faena", value=date.today(), key="fi_trab_por_faena")
                    cargo_faena_all = st.text_input("Cargo en faena (opcional, aplica a todos)", key="cargo_faena_all")

                    if st.button("Importar y asignar a esta faena", type="primary"):
                        existing = fetch_df("SELECT rut, id FROM trabajadores")
                        rut_to_id = {str(r["rut"]): int(r["id"]) for _, r in existing.iterrows()} if not existing.empty else {}

                        rows = inserted = updated = skipped = assigned = 0

                        has_cargo = "cargo" in df.columns
                        has_cc = "centro_costo" in df.columns
                        has_email = "email" in df.columns
                        fc_col = "fecha_de_contrato" if "fecha_de_contrato" in df.columns else ("fecha_contrato" if "fecha_contrato" in df.columns else None)
                        has_ve = "vigencia_examen" in df.columns

                        def _to_text_date(v):
                            if v is None or pd.isna(v):
                                return None
                            if isinstance(v, datetime):
                                return str(v.date())
                            if isinstance(v, date):
                                return str(v)
                            return str(v)

                        with conn() as c:
                            c.execute("PRAGMA foreign_keys = ON;")
                            for _, r in df.iterrows():
                                rows += 1
                                rut = clean_rut(str(r.get("rut", "") or ""))
                                nombre = str(r.get("nombre", "") or "").strip()

                                if not rut or rut.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue
                                if not nombre or nombre.lower() in ("nan", "none"):
                                    skipped += 1
                                    continue

                                nombres, apellidos = split_nombre_completo(nombre)
                                cargo = str(r.get("cargo", "") or "").strip() if has_cargo else ""
                                centro_costo = str(r.get("centro_costo", "") or "").strip() if has_cc else ""
                                email = str(r.get("email", "") or "").strip() if has_email else ""
                                fecha_contrato = _to_text_date(r.get(fc_col)) if fc_col else None
                                vigencia_examen = _to_text_date(r.get("vigencia_examen")) if has_ve else None

                                if overwrite:
                                    c.execute(
                                        '''
                                        INSERT INTO trabajadores(rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)
                                        VALUES(?,?,?,?,?,?,?,?)
                                        ON CONFLICT(rut) DO UPDATE SET
                                            nombres=excluded.nombres,
                                            apellidos=excluded.apellidos,
                                            cargo=excluded.cargo,
                                            centro_costo=excluded.centro_costo,
                                            email=excluded.email,
                                            fecha_contrato=excluded.fecha_contrato,
                                            vigencia_examen=excluded.vigencia_examen
                                        ''',
                                        (rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen),
                                    )
                                    if rut in rut_to_id:
                                        updated += 1
                                    else:
                                        inserted += 1
                                else:
                                    if rut in rut_to_id:
                                        skipped += 1
                                        continue
                                    c.execute(
                                        '''
                                        INSERT INTO trabajadores(rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)
                                        VALUES(?,?,?,?,?,?,?,?)
                                        ''',
                                        (rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen),
                                    )
                                    inserted += 1

                                # obtener id del trabajador
                                if rut not in rut_to_id:
                                    rid = c.execute("SELECT id FROM trabajadores WHERE rut=?", (rut,)).fetchone()
                                    if rid:
                                        rut_to_id[rut] = int(rid[0])

                                tid = rut_to_id.get(rut)
                                if tid:
                                    c.execute(
                                        "INSERT OR IGNORE INTO asignaciones(faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado) VALUES(?,?,?,?,?,?)",
                                        (int(faena_id), int(tid), cargo_faena_all.strip(), str(fecha_ingreso), None, "ACTIVA"),
                                    )
                                    assigned += 1

                            c.commit()

                        st.success(f"Listo. Filas: {rows} | Insertados: {inserted} | Actualizados: {updated} | Omitidos: {skipped} | Asignados: {assigned}")
                        auto_backup_db("import_asignar_faena")
                        # llevar a docs con la faena seleccionada
                        st.session_state["selected_faena_id"] = int(faena_id)
                        go("Documentos Trabajador", faena_id=int(faena_id))
            except Exception as e:
                st.error(f"No se pudo leer/importar el Excel: {e}")

    # -------------------------
    # Tab 3: asignados + quitar
    # -------------------------
    with tab3:
        docs_asg = fetch_df('''
            SELECT a.id AS asignacion_id,
                   t.id AS trabajador_id,
                   t.apellidos || ' ' || t.nombres AS trabajador,
                   t.rut,
                   a.cargo_faena,
                   a.fecha_ingreso,
                   a.estado
            FROM asignaciones a
            JOIN trabajadores t ON t.id=a.trabajador_id
            WHERE a.faena_id=?
            ORDER BY t.apellidos, t.nombres
        ''', (int(faena_id),))

        if docs_asg.empty:
            st.info("(sin trabajadores asignados)")
        else:
            st.dataframe(
                docs_asg[["trabajador","rut","cargo_faena","fecha_ingreso","estado"]],
                use_container_width=True,
                hide_index=True,
            )

            st.divider()
            st.markdown("#### 🗑️ Quitar trabajadores de esta faena")
            st.caption("Esto **solo elimina la asignación** (no elimina al trabajador ni sus documentos).")

            def _fmt_asg(tid):
                r = docs_asg[docs_asg["trabajador_id"] == tid].iloc[0]
                return f"{r['trabajador']} ({r['rut']})"

            to_remove = st.multiselect(
                "Selecciona trabajadores a quitar",
                docs_asg["trabajador_id"].tolist(),
                format_func=_fmt_asg,
                key="asg_remove_multi",
            )
            confirm = st.checkbox(
                "Confirmo que deseo quitar los seleccionados de esta faena",
                key="asg_remove_confirm",
            )

            cols = st.columns([1, 1, 2])
            with cols[0]:
                if st.button("Quitar seleccionados", type="secondary", use_container_width=True, key="btn_asg_remove"):
                    if not to_remove:
                        st.error("Selecciona al menos un trabajador.")
                        st.stop()
                    if not confirm:
                        st.error("Debes confirmar el checkbox antes de quitar.")
                        st.stop()
                    try:
                        params = [(int(faena_id), int(tid)) for tid in to_remove]
                        executemany("DELETE FROM asignaciones WHERE faena_id=? AND trabajador_id=?", params)
                        st.success(f"Listo. Quitados: {len(to_remove)}")
                        auto_backup_db("asignacion_remove")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo quitar: {e}")

            with cols[1]:
                if st.button("Limpiar selección", use_container_width=True, key="btn_asg_remove_clear"):
                    st.session_state["asg_remove_multi"] = []
                    st.rerun()
def page_documentos_empresa():
    ui_header("Documentos Empresa", "Carga documentos corporativos (valen para todas las faenas) y se incluyen en el ZIP de exportación.")
    st.caption("Puedes subir múltiples archivos por tipo. Los tipos sugeridos son opcionales y puedes crear tus propios tipos con OTRO.")

    df = fetch_df("SELECT id, doc_tipo, nombre_archivo, file_path, created_at FROM empresa_documentos ORDER BY id DESC")
    tipos_presentes = set(df["doc_tipo"].astype(str).tolist()) if not df.empty else set()
    faltan = [d for d in DOC_EMPRESA_SUGERIDOS if d not in tipos_presentes]

    c1, c2, c3 = st.columns([1, 1, 2])
    c1.metric("Tipos sugeridos", len(DOC_EMPRESA_SUGERIDOS))
    c2.metric("Tipos presentes", len(set(tipos_presentes)))
    c3.metric("Faltan sugeridos", len(faltan))

    if faltan:
        st.warning("Sugeridos faltantes: " + ", ".join(faltan))
    else:
        st.success("Sugeridos completos (si aplica).")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos sugeridos:")
        st.code("\n".join(DOC_EMPRESA_SUGERIDOS))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", DOC_EMPRESA_SUGERIDOS + ["OTRO"])
        with colx2:
            tipo_otro = st.text_input("Si eliges OTRO, escribe el nombre", placeholder="Ej: Política SST, Organigrama, Procedimiento crítico...")

        up = st.file_uploader("Archivo", key="up_doc_empresa", type=None)
        if st.button("Guardar documento empresa", type="primary"):
            if up is None:
                st.error("Debes subir un archivo.")
                st.stop()
            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            b = up.getvalue()
            folder = ["empresa", safe_name(doc_tipo)]
            file_path = save_file(folder, up.name, b)
            sha = sha256_bytes(b)
            execute(
                "INSERT INTO empresa_documentos(doc_tipo, nombre_archivo, file_path, sha256, created_at) VALUES(?,?,?,?,?)",
                (doc_tipo, up.name, file_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            st.success("Documento empresa guardado.")
            auto_backup_db("doc_empresa")
            st.rerun()



    with tab2:
        if df.empty:
            st.info("(sin documentos empresa)")
        else:
            docs = df.copy()
            show = (
                docs[["doc_tipo", "nombre_archivo", "created_at"]].copy()
                if all(c in docs.columns for c in ["doc_tipo", "nombre_archivo", "created_at"])
                else docs.copy()
            )
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### ⬇️ Descargar documento")
            pick_id = st.selectbox(
                "Documento",
                docs["id"].tolist(),
                format_func=lambda x: f"{docs[docs['id']==x].iloc[0]['doc_tipo']} — {docs[docs['id']==x].iloc[0]['nombre_archivo']}",
                key="emp_pick_doc",
            )
            row = docs[docs["id"] == pick_id].iloc[0]
            fpath = row.get("file_path", "")
            fname = row.get("nombre_archivo", "documento")
            if not fpath or not os.path.exists(fpath):
                st.warning(
                    "El archivo no está disponible en disco (posible reboot/redeploy). "
                    "Si necesitas conservarlo, usa Backup/Restore o vuelve a cargarlo."
                )
            else:
                with open(fpath, "rb") as fp:
                    b = fp.read()
                st.download_button(
                    "Descargar",
                    data=b,
                    file_name=fname,
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="emp_dl_btn",
                )
def page_documentos_empresa_faena():
    ui_header("Documentos Empresa (Faena)", "Carga documentos de empresa requeridos POR FAENA (igual que Documentos Trabajador). Se incluirán en el ZIP de la faena.")

    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        ui_tip("Crea una faena primero.")
        return

    default_id = st.session_state.get("selected_faena_id", None)
    opts = faenas["id"].tolist()
    idx = opts.index(default_id) if default_id in opts else 0

    faena_id = st.selectbox(
        "Faena",
        opts,
        index=idx,
        format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']} ({faenas[faenas['id']==x].iloc[0]['estado']})",
        key="emp_faena_sel",
    )
    st.session_state["selected_faena_id"] = int(faena_id)

    docs = fetch_df(
        "SELECT id, doc_tipo, nombre_archivo, file_path, created_at FROM faena_empresa_documentos WHERE faena_id=? ORDER BY id DESC",
        (int(faena_id),),
    )
    tipos_presentes = set(docs["doc_tipo"].astype(str).tolist()) if not docs.empty else set()
    faltan = [d for d in DOC_EMPRESA_REQUERIDOS if d not in tipos_presentes]

    c1, c2, c3 = st.columns([1, 1, 2])
    c1.metric("Requeridos", len(DOC_EMPRESA_REQUERIDOS))
    c2.metric("Cargados", len([d for d in DOC_EMPRESA_REQUERIDOS if d in tipos_presentes]))
    c3.metric("Faltan", len(faltan))

    if faltan:
        st.warning("Faltan requeridos: " + ", ".join(faltan))
    else:
        st.success("Empresa (por faena) completa: requeridos OK.")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos sugeridos:")
        st.code("\n".join(DOC_EMPRESA_SUGERIDOS))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", DOC_EMPRESA_SUGERIDOS + ["OTRO"], key="emp_faena_tipo")
        with colx2:
            tipo_otro = st.text_input("Si eliges OTRO, escribe el nombre", placeholder="Ej: Certificación, Permiso, Seguro adicional", key="emp_faena_otro")

        up = st.file_uploader("Archivo", key="up_doc_emp_faena", type=None)
        if st.button("Guardar documento (empresa por faena)", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()

            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            b = up.getvalue()
            folder = ["faenas", faena_id, "empresa", safe_name(doc_tipo)]
            file_path = save_file(folder, up.name, b)
            sha = sha256_bytes(b)

            execute(
                "INSERT INTO faena_empresa_documentos(faena_id, doc_tipo, nombre_archivo, file_path, sha256, created_at) VALUES(?,?,?,?,?,?)",
                (int(faena_id), doc_tipo, up.name, file_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            st.success("Documento guardado (empresa por faena).")
            auto_backup_db("doc_empresa_faena")
            st.rerun()


    with tab2:
        if docs.empty:
            st.info("(sin documentos)")
        else:
            show = docs[["doc_tipo","nombre_archivo","created_at"]].copy() if all(c in docs.columns for c in ["doc_tipo","nombre_archivo","created_at"]) else docs.copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("#### 🔎 Ver / Descargar")
    pick_id = st.selectbox(
        "Documento",
        docs["id"].tolist(),
        format_func=lambda x: f"{docs[docs['id']==x].iloc[0]['doc_tipo']} — {docs[docs['id']==x].iloc[0]['nombre_archivo']}",
        key="empf_pick_doc",
    )
    row = docs[docs["id"] == pick_id].iloc[0]
    fpath = row.get("file_path", "")
    fname = row.get("nombre_archivo", "documento")
    if not fpath or not os.path.exists(fpath):
        st.warning("El archivo no está disponible en disco (posible reboot/redeploy). Si necesitas conservarlo, usa Backup/Restore o vuelve a cargarlo.")
    else:
        with open(fpath, "rb") as fp:
            b = fp.read()
        st.download_button("Descargar documento", data=b, file_name=fname, mime="application/octet-stream", use_container_width=True, key="empf_dl_btn")

def page_documentos_trabajador():
    ui_header(
        "Documentos Trabajador",
        "Carga documentos obligatorios por trabajador. Puedes trabajar por FAENA: selecciona una faena y verás solo los trabajadores asignados.",
    )

    # Lista de faenas para selector local (en este mismo apartado)
    faenas = fetch_df(
        '''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
        '''
    )

    # Selector de faena dentro del apartado (no genera cajas vacías)
    current = st.session_state.get("selected_faena_id")
    ids = [None] + (faenas["id"].tolist() if not faenas.empty else [])
    default_index = ids.index(current) if (current in ids) else 0

    c1, c2 = st.columns([3, 1])
    with c1:
        faena_pick = st.selectbox(
            "Faena (opcional)",
            ids,
            index=default_index,
            format_func=lambda x: "(sin faena)" if x is None else (
                f"{int(x)} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']} ({faenas[faenas['id']==x].iloc[0]['estado']})"
            ),
            key="docs_faena_pick",
        )
        st.session_state["selected_faena_id"] = None if faena_pick is None else int(faena_pick)

    with c2:
        default_scoped = True if faena_pick is not None else False
        scoped = st.toggle("Solo esta faena", value=default_scoped, key="docs_scoped_toggle")

    st.divider()

    # Fuente de trabajadores: por faena o global
    if scoped:
        if faena_pick is None:
            st.error("Activa 'Solo esta faena' pero no has seleccionado una faena.")
            st.stop()

        trab = fetch_assigned_workers(int(faena_pick))
        if trab.empty:
            ui_tip("Esta faena no tiene trabajadores asignados. Ve a 'Asignar Trabajadores' para incorporar personal.")
            return

        # Pendientes por faena (resumen accionable)
        with st.expander("✅ Pendientes de la faena (por trabajador)", expanded=True):
            pend = pendientes_obligatorios(int(faena_pick))
            if not pend:
                st.info("(sin asignaciones)")
            else:
                ok = sum(1 for v in pend.values() if not v)
                total = len(pend)
                st.metric("Trabajadores OK", f"{ok}/{total}")
                for k, missing in pend.items():
                    if missing:
                        st.error(f"{k} — faltan: {', '.join(missing)}")
                    else:
                        st.success(f"{k} — OK")
    else:
        trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
        if trab.empty:
            ui_tip("Crea trabajadores primero.")
            return

    # Selector de trabajador (solo asignados si scoped)
    def _fmt(x):
        r = trab[trab["id"] == x].iloc[0]
        return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

    tid = st.selectbox("Trabajador", trab["id"].tolist(), format_func=_fmt, key="docs_trabajador_pick")

    # Estado documental del trabajador (global: se reutiliza entre faenas)
    docs = fetch_df(
        "SELECT id, doc_tipo, nombre_archivo, file_path, created_at FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id DESC",
        (int(tid),),
    )
    tipos_presentes = set(docs["doc_tipo"].astype(str).tolist()) if not docs.empty else set()
    faltan = [d for d in DOC_OBLIGATORIOS if d not in tipos_presentes]

    col1, col2, col3 = st.columns([1, 1, 2])
    col1.metric("Obligatorios", len(DOC_OBLIGATORIOS))
    col2.metric("Cargados", len([d for d in DOC_OBLIGATORIOS if d in tipos_presentes]))
    col3.metric("Faltan", len(faltan))

    if faltan:
        st.warning("Faltan obligatorios: " + ", ".join(faltan))
    else:
        st.success("Trabajador completo (obligatorios OK).")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos obligatorios configurados:")
        st.code("\n".join(DOC_OBLIGATORIOS))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", DOC_OBLIGATORIOS + ["OTRO"], key="doc_tipo_pick")
        with colx2:
            tipo_otro = st.text_input(
                "Si eliges OTRO, escribe el nombre",
                placeholder="Ej: Certificación operador, Licencia, Examen ocupacional",
                key="doc_tipo_otro",
            )

        up = st.file_uploader("Archivo", key="up_doc_trabajador", type=None)
        if st.button("Guardar documento", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()

            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            b = up.getvalue()
            folder = ["trabajadores", tid, safe_name(doc_tipo)]
            file_path = save_file(folder, up.name, b)
            sha = sha256_bytes(b)

            execute(
                "INSERT INTO trabajador_documentos(trabajador_id, doc_tipo, nombre_archivo, file_path, sha256, created_at) VALUES(?,?,?,?,?,?)",
                (int(tid), doc_tipo, up.name, file_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            st.success("Documento guardado.")
            auto_backup_db("doc_trabajador")
            st.rerun()


    with tab2:
        if docs.empty:
            st.info("(sin documentos)")
        else:
            show = docs[["doc_tipo","nombre_archivo","created_at"]].copy() if all(c in docs.columns for c in ["doc_tipo","nombre_archivo","created_at"]) else docs.copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("#### 🔎 Ver / Descargar")
    pick_id = st.selectbox(
        "Documento",
        docs["id"].tolist(),
        format_func=lambda x: f"{docs[docs['id']==x].iloc[0]['doc_tipo']} — {docs[docs['id']==x].iloc[0]['nombre_archivo']}",
        key="trab_pick_doc",
    )
    row = docs[docs["id"] == pick_id].iloc[0]
    fpath = row.get("file_path", "")
    fname = row.get("nombre_archivo", "documento")
    if not fpath or not os.path.exists(fpath):
        st.warning("El archivo no está disponible en disco (posible reboot/redeploy). Si necesitas conservarlo, usa Backup/Restore o vuelve a cargarlo.")
    else:
        with open(fpath, "rb") as fp:
            b = fp.read()
        st.download_button("Descargar documento", data=b, file_name=fname, mime="application/octet-stream", use_container_width=True, key="trab_dl_btn")

def page_export_zip():
    ui_header("Export (ZIP)", "Genera carpeta por faena con documentos de trabajadores y deja historial.")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        ui_tip("Crea una faena primero.")
        return

    default_id = st.session_state.get("selected_faena_id", None)
    opts = faenas["id"].tolist()
    idx = opts.index(default_id) if default_id in opts else 0

    faena_id = st.selectbox(
        "Faena",
        opts,
        index=idx,
        format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']} ({faenas[faenas['id']==x].iloc[0]['estado']})",
    )
    st.session_state["selected_faena_id"] = int(faena_id)

    tab1, tab2, tab3, tab4 = st.tabs(["✅ Pendientes", "📦 Generar ZIP", "🗂️ Historial", "📅 Export por mes"])

    with tab1:
        pend = pendientes_obligatorios(int(faena_id))
        miss_emp = pendientes_empresa_faena(int(faena_id))
        st.write("**Pendientes obligatorios (antes de exportar):**")
        if not pend:
            st.info("(sin trabajadores asignados)")
        else:
            for k, missing in pend.items():
                if missing:
                    st.error(f"{k} — faltan: {', '.join(missing)}")
                else:
                    st.success(f"{k} — OK")


        st.divider()
        st.write("**Documentos empresa (por faena):**")
        if miss_emp:
            st.error("Faltan: " + ", ".join(miss_emp))
        else:
            st.success("OK (requeridos completos).")



with tab2:
    st.markdown("### 📦 Selecciona qué incluir en el ZIP")

    cA, cB, cC = st.columns(3)
    with cA:
        inc_contrato = st.checkbox("Contrato de faena", value=True, key="exp_inc_contrato")
        inc_anexos = st.checkbox("Anexos de faena", value=True, key="exp_inc_anexos")
    with cB:
        inc_emp_faena = st.checkbox("Docs empresa (por faena)", value=True, key="exp_inc_emp_faena")
        inc_emp_global = st.checkbox("Docs empresa (global)", value=True, key="exp_inc_emp_global")
    with cC:
        inc_trab = st.checkbox("Docs trabajadores", value=True, key="exp_inc_trab")

    st.divider()
    st.markdown("#### (Opcional) Filtrar por tipo de documento")

    # Tipos disponibles
    emp_global_types = fetch_df("SELECT DISTINCT doc_tipo FROM empresa_documentos ORDER BY doc_tipo")
    emp_global_list = emp_global_types["doc_tipo"].dropna().astype(str).tolist() if not emp_global_types.empty else []

    emp_faena_types = fetch_df("SELECT DISTINCT doc_tipo FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo", (int(faena_id),))
    emp_faena_list = emp_faena_types["doc_tipo"].dropna().astype(str).tolist() if not emp_faena_types.empty else []

    trab_types = fetch_df('''
        SELECT DISTINCT td.doc_tipo AS doc_tipo
        FROM trabajador_documentos td
        JOIN asignaciones a ON a.trabajador_id = td.trabajador_id
        WHERE a.faena_id=?
        ORDER BY td.doc_tipo
    ''', (int(faena_id),))
    trab_list = trab_types["doc_tipo"].dropna().astype(str).tolist() if not trab_types.empty else []

    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        emp_global_sel = []
        if inc_emp_global and emp_global_list:
            emp_global_sel = st.multiselect("Tipos Empresa Global", emp_global_list, default=emp_global_list, key="exp_types_emp_global")
        elif inc_emp_global and not emp_global_list:
            st.caption("Sin docs empresa global cargados.")
    with colf2:
        emp_faena_sel = []
        if inc_emp_faena and emp_faena_list:
            emp_faena_sel = st.multiselect("Tipos Empresa por Faena", emp_faena_list, default=emp_faena_list, key="exp_types_emp_faena")
        elif inc_emp_faena and not emp_faena_list:
            st.caption("Sin docs empresa por faena cargados.")
    with colf3:
        trab_sel = []
        if inc_trab and trab_list:
            trab_sel = st.multiselect("Tipos Trabajador", trab_list, default=trab_list, key="exp_types_trab")
        elif inc_trab and not trab_list:
            st.caption("Sin docs trabajador cargados para esta faena.")

    st.divider()
    colx1, colx2 = st.columns([1, 1])
    with colx1:
        if st.button("Generar ZIP y guardar en historial", type="primary", use_container_width=True):
            try:
                zip_bytes, name = export_zip_for_faena(
                    int(faena_id),
                    include_global_empresa_docs=inc_emp_global,
                    include_contrato=inc_contrato,
                    include_anexos=inc_anexos,
                    include_empresa_faena=inc_emp_faena,
                    include_trabajadores=inc_trab,
                    doc_types_empresa_global=(emp_global_sel or None),
                    doc_types_empresa_faena=(emp_faena_sel or None),
                    doc_types_trabajador=(trab_sel or None),
                )
                path = persist_export(int(faena_id), zip_bytes, name)
                st.success(f"ZIP generado y guardado: {os.path.basename(path)}")
                auto_backup_db("export_zip")
                st.download_button(
                    "Descargar ZIP (recién generado)",
                    data=zip_bytes,
                    file_name=os.path.basename(path),
                    mime="application/zip",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"No se pudo generar ZIP: {e}")
    with colx2:
        st.caption("Para conservar historial entre reboots, usa Backup / Restore.")

    with tab3:
        hist = fetch_df("SELECT id, file_path, size_bytes, created_at FROM export_historial WHERE faena_id=? ORDER BY id DESC", (int(faena_id),))
        if hist.empty:
            st.info("(sin exportaciones guardadas aún)")
        else:
            show = hist.copy()
            show["archivo"] = show["file_path"].apply(lambda p: os.path.basename(p))
            show["size_kb"] = (show["size_bytes"] / 1024).round(1)
            st.dataframe(show[["id", "archivo", "size_kb", "created_at"]], use_container_width=True, hide_index=True)

            exp_id = st.selectbox("Elegir export para descargar", show["id"].tolist(), format_func=lambda x: f"{int(x)} - {show[show['id']==x].iloc[0]['archivo']}")
            row = show[show["id"] == exp_id].iloc[0]
            p = row["file_path"]
            if os.path.exists(p):
                with open(p, "rb") as f:
                    b = f.read()
                st.download_button("Descargar export seleccionado", data=b, file_name=os.path.basename(p), mime="application/zip", use_container_width=True)
            else:
                st.warning("El archivo no está en disco (posible reboot/redeploy). Usa Backup/Restore para conservarlo.")


    with tab4:
        st.markdown("Genera un ZIP con **todas las faenas** cuyo **inicio** cae dentro de un mes (YYYY-MM).")
        fa = fetch_df("SELECT DISTINCT substr(fecha_inicio,1,7) AS ym FROM faenas WHERE fecha_inicio IS NOT NULL AND TRIM(fecha_inicio)<>'' ORDER BY ym DESC")
        ym_opts = fa["ym"].tolist() if not fa.empty else []
        if not ym_opts:
            st.info("No hay fechas de inicio registradas para exportar por mes.")
        else:
            ym = st.selectbox("Mes", ym_opts, key="exp_mes_pick")
            include_global = st.checkbox("Incluir documentos empresa globales (una vez)", value=True, key="exp_mes_inc_global")

            # Mostrar faenas incluidas
            lst = fetch_df('''
                SELECT f.id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio
                FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
                WHERE substr(f.fecha_inicio,1,7)=?
                ORDER BY f.id DESC
            ''', (ym,))
            st.caption(f"Faenas incluidas: {len(lst)}")
            st.dataframe(lst[["id","mandante","nombre","estado","fecha_inicio"]], use_container_width=True, hide_index=True)

            colm1, colm2 = st.columns([1,1])
            with colm1:
                if st.button("Generar ZIP mensual y guardar", type="primary", use_container_width=True):
                    try:
                        y, m = ym.split("-")
                        zip_bytes, name = export_zip_for_mes(int(y), int(m), include_global_empresa_docs=include_global)
                        path = persist_export_mes(ym, zip_bytes)
                        st.success(f"ZIP mensual generado: {os.path.basename(path)}")
                        auto_backup_db("export_mes")
                        st.download_button("Descargar ZIP mensual (recién generado)", data=zip_bytes, file_name=os.path.basename(path), mime="application/zip", use_container_width=True)
                    except Exception as e:
                        st.error(f"No se pudo generar ZIP mensual: {e}")
            with colm2:
                st.caption("Se guarda en historial mensual (si no hay reboot). Para conservar, usa Backup / Restore.")

        st.divider()
        st.markdown("#### 🗂️ Historial mensual")
        histm = fetch_df("SELECT id, year_month, file_path, size_bytes, created_at FROM export_historial_mes ORDER BY id DESC")
        if histm.empty:
            st.info("(sin exportaciones mensuales guardadas aún)")
        else:
            show = histm.copy()
            show["archivo"] = show["file_path"].apply(lambda p: os.path.basename(p))
            show["size_kb"] = (show["size_bytes"] / 1024).round(1)
            st.dataframe(show[["id","year_month","archivo","size_kb","created_at"]], use_container_width=True, hide_index=True)

            exp_id = st.selectbox("Elegir export mensual para descargar", show["id"].tolist(), format_func=lambda x: f"{int(x)} - {show[show['id']==x].iloc[0]['year_month']} / {show[show['id']==x].iloc[0]['archivo']}", key="pick_hist_mes")
            row = show[show["id"] == exp_id].iloc[0]
            p = row["file_path"]
            if os.path.exists(p):
                with open(p, "rb") as f:
                    b = f.read()
                st.download_button("Descargar export mensual seleccionado", data=b, file_name=os.path.basename(p), mime="application/zip", use_container_width=True)
            else:
                st.warning("El archivo no está en disco (posible reboot/redeploy). Usa Backup/Restore para conservarlo.")

def page_backup_restore():
    ui_header("Backup / Restore", "Respalda la base y documentos para evitar pérdidas en Streamlit Community Cloud.")
    st.warning(
        "En Streamlit Community Cloud, los archivos locales (incluyendo SQLite y uploads) pueden perderse en reboots/redeploy. "
        "Este módulo te permite descargar un **Backup ZIP** con la base y documentos, y luego restaurarlo."
    )

    tab1, tab2, tab3 = st.tabs(["⚡ Auto-backups", "🗄️ Base (app.db)", "📦 Backup completo (ZIP)"])

    with tab1:
        st.caption("Auto-backups generados al guardar (solo app.db). Se guardan localmente y conviene descargarlos.")
        hist = fetch_df("SELECT id, tag, file_path, size_bytes, created_at FROM auto_backup_historial ORDER BY id DESC")
        if hist.empty:
            st.info("(aún no hay auto-backups)")
        else:
            view = hist.copy()
            view["archivo"] = view["file_path"].apply(lambda p: os.path.basename(p))
            view["size_kb"] = (view["size_bytes"] / 1024).round(1)
            st.dataframe(view[["id", "tag", "archivo", "size_kb", "created_at"]], use_container_width=True, hide_index=True)

            sel = st.selectbox(
                "Elegir auto-backup para descargar",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} - {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['tag']})",
            )
            row = view[view["id"] == sel].iloc[0]
            p = row["file_path"]
            if os.path.exists(p):
                with open(p, "rb") as f:
                    b = f.read()
                st.download_button("Descargar auto-backup (app.db)", data=b, file_name=os.path.basename(p), mime="application/octet-stream", use_container_width=True)
            else:
                st.warning("El archivo no está en disco (posible reboot/redeploy).")

    with tab2:
        coldb1, coldb2 = st.columns([1, 1])

        with coldb1:
            st.markdown("### Descargar app.db")
            if os.path.exists(DB_PATH):
                with open(DB_PATH, "rb") as f:
                    db_bytes = f.read()
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                st.download_button("Descargar app.db", data=db_bytes, file_name=f"app_{ts}.db", mime="application/octet-stream", use_container_width=True)
            else:
                st.info("Aún no existe app.db (no hay datos o no se ha inicializado).")

        with coldb2:
            st.markdown("### Restaurar app.db")
            up_db = st.file_uploader("Sube un archivo .db", type=["db", "sqlite", "sqlite3"], key="up_db_only")
            if st.button("Restaurar app.db", type="primary", use_container_width=True):
                if up_db is None:

                    st.error("Debes subir un archivo .db primero.")

                    st.stop()
                try:
                    with open(DB_PATH, "wb") as f:
                        f.write(up_db.getvalue())
                    init_db()
                    st.success("Base restaurada. La app se reiniciará.")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo restaurar app.db: {e}")

    with tab3:
        st.markdown("### 1) Descargar Backup completo")
        if st.button("Generar Backup ZIP (DB + documentos)", type="primary", use_container_width=True):
            b = make_backup_zip_bytes()
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            st.download_button("Descargar Backup", data=b, file_name=f"backup_control_faenas_{ts}.zip", mime="application/zip", use_container_width=True)
            st.success("Backup listo para descargar.")

        st.divider()
        st.markdown("### 2) Restaurar Backup completo")
        up = st.file_uploader("Sube backup ZIP", type=["zip"], key="up_backup_zip")
        if st.button("Restaurar ahora", type="primary", use_container_width=True):
            if up is None:

                st.error("Debes subir un backup ZIP primero.")

                st.stop()
            try:
                restore_from_backup_zip(up.getvalue())
                st.success("Backup restaurado. La app se reiniciará.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo restaurar: {e}")

# ----------------------------
# Route
# ----------------------------
p = st.session_state.get("nav_page", "Dashboard")
if p == "Dashboard":
    page_dashboard()
elif p == "Mandantes":
    page_mandantes()
elif p == "Contratos de Faena":
    page_contratos_faena()
elif p == "Faenas":
    page_faenas()
elif p == "Trabajadores":
    page_trabajadores()
elif p == "Documentos Empresa":
    page_documentos_empresa()
elif p == "Documentos Empresa (Faena)":
    page_documentos_empresa_faena()
elif p == "Asignar Trabajadores":
    page_asignar_trabajadores()
elif p == "Documentos Trabajador":
    page_documentos_trabajador()
elif p == "Export (ZIP)":
    page_export_zip()
elif p == "Backup / Restore":
    page_backup_restore()
else:
    # Si el estado quedó con un valor inesperado, vuelve a Dashboard
    st.session_state["nav_page"] = "Dashboard"
    st.rerun()
