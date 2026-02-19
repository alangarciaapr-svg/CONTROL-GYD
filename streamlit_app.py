import os
import re
import io
import zipfile
import hashlib
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

        </style>
        """,
        unsafe_allow_html=True,
    )

def ui_header(title: str, desc: str = ""):
    st.markdown(f"## {title}")
    if desc:
        st.caption(desc)

def ui_tip(text: str):
    st.info(text)

def safe_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "item"


def get_global_counts():
    """Devuelve conteos básicos para UI (tolerante a tablas vacías)."""
    out = {}
    for key, sql in [
        ("mandantes", "SELECT COUNT(*) AS n FROM mandantes"),
        ("contratos_faena", "SELECT COUNT(*) AS n FROM contratos_faena"),
        ("faenas", "SELECT COUNT(*) AS n FROM faenas"),
        ("faenas_activas", "SELECT COUNT(*) AS n FROM faenas WHERE estado='ACTIVA'"),
        ("trabajadores", "SELECT COUNT(*) AS n FROM trabajadores"),
        ("asignaciones", "SELECT COUNT(*) AS n FROM asignaciones"),
        ("docs", "SELECT COUNT(*) AS n FROM trabajador_documentos"),
        ("exports", "SELECT COUNT(*) AS n FROM export_historial"),
    ]:
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

def export_zip_for_faena(faena_id: int):
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

    buff = io.BytesIO()
    z = zipfile.ZipFile(buff, "w", zipfile.ZIP_DEFLATED)

    pend = pendientes_obligatorios(faena_id)
    idx = []
    idx.append(f"MANDANTE: {f['mandante_nombre']}")
    idx.append(f"FAENA: {f['nombre']}")
    idx.append(f"ESTADO: {f['estado']}")
    idx.append(f"INICIO: {f['fecha_inicio']} | TERMINO: {f['fecha_termino'] or '-'}")
    idx.append(f"UBICACION: {f['ubicacion'] or '-'}")
    idx.append(f"CONTRATO_FAENA: {f['contrato_nombre'] or '(sin contrato cargado)'}")
    idx.append("")
    idx.append("PENDIENTES DOCUMENTOS OBLIGATORIOS POR TRABAJADOR:")
    if not pend:
        idx.append("- (sin trabajadores asignados)")
    else:
        for k, missing in pend.items():
            if missing:
                idx.append(f"* {k}: faltan {', '.join(missing)}")
            else:
                idx.append(f"* {k}: OK")
    z.writestr("99_Index_Pendientes.txt", "\n".join(idx))

    # 00_Contrato_Faena
    if f.get("contrato_path") and os.path.exists(f["contrato_path"]):
        fname = os.path.basename(f["contrato_path"])
        with open(f["contrato_path"], "rb") as fp:
            z.writestr(f"00_Contrato_Faena/{fname}", fp.read())

    # 01_Anexos_Faena
    anexos = fetch_df("SELECT * FROM faena_anexos WHERE faena_id=? ORDER BY id", (faena_id,))
    for _, a in anexos.iterrows():
        src = a["file_path"]
        if src and os.path.exists(src):
            fname = os.path.basename(src)
            with open(src, "rb") as fp:
                z.writestr(f"01_Anexos_Faena/{fname}", fp.read())

    # 03_Trabajadores
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
            src = d["file_path"]
            if src and os.path.exists(src):
                fname = os.path.basename(src)
                tipo = safe_name(d["doc_tipo"])
                with open(src, "rb") as fp:
                    z.writestr(f"03_Trabajadores/{tdir}/{tipo}/{fname}", fp.read())

    z.close()
    buff.seek(0)
    return buff.getvalue(), str(f["nombre"])

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
    tmp = f"_restore_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(tmp, exist_ok=True)
    try:
        with zipfile.ZipFile(io.BytesIO(uploaded_bytes), "r") as z:
            z.extractall(tmp)

        db_candidate = os.path.join(tmp, "backup", "app.db")
        if not os.path.exists(db_candidate):
            raise ValueError("El ZIP no contiene backup/app.db")

        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        os.replace(db_candidate, DB_PATH)

        up_candidate = os.path.join(tmp, "backup", UPLOAD_ROOT)
        if os.path.exists(up_candidate):
            if os.path.exists(UPLOAD_ROOT):
                shutil.rmtree(UPLOAD_ROOT, ignore_errors=True)
            shutil.copytree(up_candidate, UPLOAD_ROOT)
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
    "Asignar Trabajadores",
    "Documentos Trabajador",
    "Export (ZIP)",
    "Backup / Restore",
]


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
    ui_header("Dashboard", "Resumen general, cobertura de documentos y alertas.")
    mand_n = int(fetch_df("SELECT COUNT(*) AS n FROM mandantes")["n"].iloc[0])
    faena_n = int(fetch_df("SELECT COUNT(*) AS n FROM faenas")["n"].iloc[0])
    fa_act = int(fetch_df("SELECT COUNT(*) AS n FROM faenas WHERE estado='ACTIVA'")["n"].iloc[0])
    trab_n = int(fetch_df("SELECT COUNT(*) AS n FROM trabajadores")["n"].iloc[0])
    asg_n = int(fetch_df("SELECT COUNT(*) AS n FROM asignaciones")["n"].iloc[0])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Mandantes", mand_n)
    c2.metric("Faenas", faena_n)
    c3.metric("Faenas activas", fa_act)
    c4.metric("Trabajadores", trab_n)
    c5.metric("Asignaciones", asg_n)

    st.divider()

    tab1, tab2, tab3 = st.tabs(["📋 Avance por faena", "✅ Pendientes", "⏳ Alertas"])
    with tab1:
        df = faena_progress_table()
        if df.empty:
            ui_tip("Crea un mandante, contrato de faena y una faena para comenzar.")
            return

        colf1, colf2, colf3 = st.columns([2, 1, 1])
        with colf1:
            mandantes = ["(todos)"] + sorted(df["mandante"].unique().tolist())
            f_mand = st.selectbox("Mandante", mandantes, index=0)
        with colf2:
            estados = ["(todos)"] + ESTADOS_FAENA
            f_estado = st.selectbox("Estado", estados, index=0)
        with colf3:
            q = st.text_input("Buscar faena", placeholder="Ej: bellavista")

        out = df.copy()
        if f_mand != "(todos)":
            out = out[out["mandante"] == f_mand]
        if f_estado != "(todos)":
            out = out[out["estado"] == f_estado]
        if q.strip():
            out = out[out["faena"].str.lower().str.contains(q.strip().lower(), na=False)]

        def _avance_row(r):
            if int(r["trabajadores"]) == 0:
                return "0/0"
            return f"{int(r['trab_ok'])}/{int(r['trabajadores'])}"

        out["avance_trab"] = out.apply(_avance_row, axis=1)
        out = out.rename(columns={"faena_id": "id", "faena": "faena_nombre", "cobertura_docs_pct": "cobertura_docs_%"})
        show = out[["id", "mandante", "faena_nombre", "estado", "fecha_inicio", "fecha_termino", "trabajadores", "avance_trab", "cobertura_docs_%", "faltantes_total"]].copy()
        st.dataframe(show, use_container_width=True, hide_index=True)

    with tab2:
        df = faena_progress_table()
        if df.empty:
            ui_tip("No hay faenas aún.")
            return
        show = df.rename(columns={"faena_id": "id", "faena": "faena_nombre"})
        faena_id = st.selectbox(
            "Faena",
            show["id"].tolist(),
            format_func=lambda x: f"{int(x)} - {show[show['id']==x].iloc[0]['mandante']} / {show[show['id']==x].iloc[0]['faena_nombre']}",
        )
        pend = pendientes_obligatorios(int(faena_id))
        if not pend:
            st.info("(sin trabajadores asignados)")
        else:
            ok = sum(1 for v in pend.values() if not v)
            total = len(pend)
            st.metric("Trabajadores OK", f"{ok}/{total}")
            for k, missing in pend.items():
                if missing:
                    st.error(f"{k} — faltan: {', '.join(missing)}")
                else:
                    st.success(f"{k} — OK")

        if st.button("Ir a Export de esta faena", use_container_width=True):
            st.session_state["selected_faena_id"] = int(faena_id)
            st.session_state["nav_page"] = "Export (ZIP)"
            st.rerun()

    with tab3:
        today = date.today()
        limit = today + timedelta(days=30)
        tdf = fetch_df("SELECT rut, apellidos, nombres, cargo, vigencia_examen FROM trabajadores")
        rows = []
        for _, r in tdf.iterrows():
            d = parse_date_maybe(r.get("vigencia_examen"))
            if d and d <= limit:
                rows.append({
                    "rut": r["rut"],
                    "trabajador": f"{r['apellidos']} {r['nombres']}",
                    "cargo": r.get("cargo",""),
                    "vigencia_examen": str(d),
                    "dias_restantes": (d - today).days
                })
        if rows:
            adf = pd.DataFrame(rows).sort_values("dias_restantes")
            st.dataframe(adf, use_container_width=True, hide_index=True)
        else:
            st.success("Sin vencimientos dentro de 30 días (según vigencia_examen).")

def page_mandantes():
    ui_header("Mandantes", "Registra mandantes. Cada faena se asocia a un mandante.")
    tab1, tab2 = st.tabs(["📋 Listado", "➕ Crear"])

    with tab2:
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
                    # Manejo amigable de duplicados
                    msg = str(e)
                    if "UNIQUE" in msg.upper():
                        st.error("Ya existe un mandante con ese nombre.")
                    else:
                        st.error(f"No se pudo crear: {e}")

    with tab1:
        df = fetch_df("SELECT id, nombre FROM mandantes ORDER BY id DESC")
        q = st.text_input("Buscar mandante", placeholder="Nombre...")
        out = df.copy()
        if q.strip():
            out = out[out["nombre"].astype(str).str.lower().str.contains(q.strip().lower(), na=False)]
        st.dataframe(out, use_container_width=True, hide_index=True)

def page_contratos_faena():
    ui_header("Contratos de Faena", "Crea contratos por mandante y carga el archivo si aplica.")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        ui_tip("Primero crea un mandante.")
        return

    tab1, tab2 = st.tabs(["➕ Crear contrato", "📋 Listado / Archivo"])

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
                created_at = None
                if archivo is not None:
                    b = archivo.getvalue()
                    file_path = save_file(["contratos_faena", mandante_id], archivo.name, b)
                    sha = sha256_bytes(b)
                    created_at = datetime.utcnow().isoformat(timespec="seconds")

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
            SELECT cf.id, m.nombre AS mandante, cf.nombre, cf.fecha_inicio, cf.fecha_termino,
                   CASE WHEN cf.file_path IS NULL THEN '(sin archivo)' ELSE 'OK' END AS archivo
            FROM contratos_faena cf
            JOIN mandantes m ON m.id=cf.mandante_id
            ORDER BY cf.id DESC
        ''')
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### Actualizar / agregar archivo")
        if df.empty:
            st.info("No hay contratos.")
            return
        contrato_id = st.selectbox(
            "Contrato de faena",
            df["id"].tolist(),
            format_func=lambda x: f"{x} - {df[df['id']==x].iloc[0]['mandante']} / {df[df['id']==x].iloc[0]['nombre']}",
        )
        up = st.file_uploader("Archivo contrato", key="up_contrato_existente", type=None)
        if st.button("Guardar archivo en contrato", type="primary"):
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

def page_faenas():
    ui_header("Faenas", "Crea faenas por mandante, registra fechas/estado y carga anexos si aplica.")
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

    tab1, tab2, tab3 = st.tabs(["➕ Crear faena", "📋 Listado (semáforo)", "📎 Anexos"])

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

            colq1, colq2 = st.columns([2, 1])
            with colq1:
                fid = st.selectbox("Acción rápida: seleccionar faena", show["id"].tolist(), format_func=lambda x: f"{int(x)} - {show[show['id']==x].iloc[0]['mandante']} / {show[show['id']==x].iloc[0]['faena_nombre']}")
            with colq2:
                if st.button("Ir a Export", type="primary", use_container_width=True):
                    st.session_state["selected_faena_id"] = int(fid)
                    st.session_state["nav_page"] = "Export (ZIP)"
                    st.rerun()

    with tab3:
        # Listado base para anexos (usa tabla de faenas)
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

def page_trabajadores():
    ui_header("Trabajadores", "Carga masiva por Excel o gestión manual. Luego asigna a faenas y adjunta documentos.")
    tab1, tab2, tab3 = st.tabs(["📥 Importar Excel", "➕ Crear / Editar", "📋 Listado"])

    with tab1:
        st.write("Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        up = st.file_uploader("Sube Excel (.xlsx)", type=["xlsx"], key="up_excel_trabajadores")
        if up is not None:
            try:
                xls = pd.ExcelFile(up)
                sheet = st.selectbox("Hoja", xls.sheet_names, index=0)
                raw = pd.read_excel(xls, sheet_name=sheet)

                colmap = {c: norm_col(str(c)) for c in raw.columns}
                df = raw.rename(columns=colmap).copy()

                st.caption("Vista previa (primeras 10 filas)")
                st.dataframe(df.head(10), use_container_width=True)

                if "rut" not in df.columns or "nombre" not in df.columns:
                    st.error("El Excel debe tener columnas 'RUT' y 'NOMBRE'.")
                else:
                    overwrite = st.checkbox("Sobrescribir si el RUT ya existe", value=True)

                    if st.button("Importar Excel ahora", type="primary"):
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

    with tab2:
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

    with tab3:
        df = fetch_df("SELECT id, rut, apellidos, nombres, cargo, centro_costo, email, fecha_contrato, vigencia_examen FROM trabajadores ORDER BY id DESC")
        q = st.text_input("Buscar", placeholder="RUT, nombre o cargo")
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

def page_asignar_trabajadores():
    ui_header("Asignar Trabajadores", "Asigna trabajadores a una faena (quedan activos y listos para control documental).")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")

    if faenas.empty:
        ui_tip("Crea faenas primero.")
        return
    if trab.empty:
        ui_tip("Crea trabajadores primero.")
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

    asignados = fetch_df("SELECT trabajador_id FROM asignaciones WHERE faena_id=?", (int(faena_id),))
    asignados_ids = set(asignados["trabajador_id"].tolist()) if not asignados.empty else set()
    disponibles = trab[~trab["id"].isin(asignados_ids)].copy()

    def _fmt_trab(x):
        r = trab[trab["id"] == x].iloc[0]
        return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

    st.divider()
    st.markdown("### Agregar asignaciones")
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

    st.divider()
    st.markdown("### Asignados en esta faena")
    asg = fetch_df('''
        SELECT t.apellidos || ' ' || t.nombres AS trabajador, t.rut, a.cargo_faena, a.fecha_ingreso, a.estado
        FROM asignaciones a JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.faena_id=?
        ORDER BY t.apellidos, t.nombres
    ''', (int(faena_id),))
    st.dataframe(asg, use_container_width=True, hide_index=True)

def page_documentos_trabajador():
    ui_header("Documentos Trabajador", "Carga documentos obligatorios (EPP, RIOHS, IRL, Contrato, Anexo) y extras si es necesario.")
    trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
    if trab.empty:
        ui_tip("Crea trabajadores primero.")
        return

    tid = st.selectbox(
        "Trabajador",
        trab["id"].tolist(),
        format_func=lambda x: f"{trab[trab['id']==x].iloc[0]['apellidos']} {trab[trab['id']==x].iloc[0]['nombres']} ({trab[trab['id']==x].iloc[0]['rut']})",
    )

    # Estado documental del trabajador
    docs = fetch_df("SELECT doc_tipo, nombre_archivo, created_at FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id DESC", (int(tid),))
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
            tipo = st.selectbox("Tipo", DOC_OBLIGATORIOS + ["OTRO"])
        with colx2:
            tipo_otro = st.text_input("Si eliges OTRO, escribe el nombre", placeholder="Ej: Certificación operador, Licencia, Examen ocupacional")

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
            show = docs.copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

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

    tab1, tab2, tab3 = st.tabs(["✅ Pendientes", "📦 Generar ZIP", "🗂️ Historial"])

    with tab1:
        pend = pendientes_obligatorios(int(faena_id))
        st.write("**Pendientes obligatorios (antes de exportar):**")
        if not pend:
            st.info("(sin trabajadores asignados)")
        else:
            for k, missing in pend.items():
                if missing:
                    st.error(f"{k} — faltan: {', '.join(missing)}")
                else:
                    st.success(f"{k} — OK")

    with tab2:
        colx1, colx2 = st.columns([1, 1])
        with colx1:
            if st.button("Generar ZIP y guardar en historial", type="primary", use_container_width=True):
                try:
                    zip_bytes, name = export_zip_for_faena(int(faena_id))
                    path = persist_export(int(faena_id), zip_bytes, name)
                    st.success(f"ZIP generado y guardado: {os.path.basename(path)}")
                    auto_backup_db("export_zip")
                    st.download_button("Descargar ZIP (recién generado)", data=zip_bytes, file_name=os.path.basename(path), mime="application/zip", use_container_width=True)
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
