import os
import re
import io
import zipfile
import hashlib
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st

# ----------------------------
# Config
# ----------------------------
st.set_page_config(page_title="Control Documental Faenas", layout="wide")

APP_NAME = "Control Documental de Faenas"
DB_PATH = "app.db"
UPLOAD_ROOT = "uploads"  # Streamlit Cloud FS no es storage duradero a largo plazo.

ESTADOS_FAENA = ["ACTIVA", "TERMINADA"]
DOC_OBLIGATORIOS = [
    "REGISTRO_EPP",
    "ENTREGA_RIOHS",
    "IRL",
    "CONTRATO_TRABAJO",
    "ANEXO_CONTRATO",
]

# ----------------------------
# Helpers
# ----------------------------
def safe_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "item"

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

def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

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

        # Migración para soportar tu Excel
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

        c.execute('''
        CREATE TABLE IF NOT EXISTS faena_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faena_id INTEGER NOT NULL,
            categoria TEXT DEFAULT '',
            nombre_archivo TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE
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

    for _, r in asign.iterrows():
        tid = int(r["trabajador_id"])
        label = f"{r['apellidos']} {r['nombres']} ({r['rut']})"
        docs = fetch_df("SELECT doc_tipo FROM trabajador_documentos WHERE trabajador_id=?", (tid,))
        have = set(docs["doc_tipo"].tolist()) if not docs.empty else set()
        missing = [d for d in DOC_OBLIGATORIOS if d not in have]
        out[label] = missing
    return out

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

    # 02_Documentos_Faena_Extra
    fdocs = fetch_df("SELECT * FROM faena_documentos WHERE faena_id=? ORDER BY id", (faena_id,))
    for _, d in fdocs.iterrows():
        src = d["file_path"]
        if src and os.path.exists(src):
            fname = os.path.basename(src)
            cat = safe_name(d.get("categoria") or "general")
            with open(src, "rb") as fp:
                z.writestr(f"02_Documentos_Faena_Extra/{cat}/{fname}", fp.read())

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
    return buff.getvalue()

# ----------------------------
# Init
# ----------------------------
ensure_dirs()
init_db()

# ----------------------------
# UI
# ----------------------------
st.title(APP_NAME)

with st.sidebar:
    st.header("Navegación")
    page = st.radio(
        "Ir a",
        ["Dashboard", "Mandantes", "Contratos de Faena", "Faenas", "Trabajadores", "Asignar Trabajadores", "Documentos Trabajador", "Documentos Extra Faena", "Export (ZIP)"],
        index=0,
    )
    st.caption("Flujo: Mandante → Contrato Faena → Faena → Trabajadores → Documentos → Export.")

# ----------------------------
# Pages
# ----------------------------
def page_dashboard():
    st.subheader("Dashboard")
    faenas = fetch_df('''
        SELECT f.id, f.nombre, f.estado, f.fecha_inicio, f.fecha_termino,
               m.nombre AS mandante,
               COALESCE(cf.nombre, '') AS contrato_faena
        FROM faenas f
        JOIN mandantes m ON m.id=f.mandante_id
        LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        st.info("Crea un mandante, contrato de faena y una faena para comenzar.")
        return

    st.dataframe(faenas, use_container_width=True)

    st.divider()
    st.subheader("Pendientes obligatorios (elige una faena)")
    faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['nombre']}")
    pend = pendientes_obligatorios(int(faena_id))
    if not pend:
        st.write("(sin trabajadores asignados)")
    else:
        ok = sum(1 for v in pend.values() if not v)
        st.metric("Trabajadores OK", ok)
        for k, missing in pend.items():
            if missing:
                st.error(f"{k} — faltan: {', '.join(missing)}")
            else:
                st.success(f"{k} — OK")

def page_mandantes():
    st.subheader("Mandantes")
    with st.expander("Crear mandante", expanded=True):
        nombre = st.text_input("Nombre mandante", placeholder="Bosque Los Lagos")
        if st.button("Guardar mandante", type="primary", disabled=not nombre.strip()):
            try:
                execute("INSERT INTO mandantes(nombre) VALUES(?)", (nombre.strip(),))
                st.success("Mandante creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df("SELECT * FROM mandantes ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)

def page_contratos_faena():
    st.subheader("Contratos de Faena (por mandante) + subir archivo")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        st.info("Primero crea un mandante.")
        return

    with st.expander("Crear contrato de faena", expanded=True):
        mandante_id = st.selectbox("Mandante", mand["id"].tolist(), format_func=lambda x: mand[mand["id"]==x].iloc[0]["nombre"])
        nombre = st.text_input("Nombre contrato de faena", placeholder="Contrato Faena Bellavista")
        fi = st.date_input("Fecha inicio (opcional)", value=None)
        ft = st.date_input("Fecha término (opcional)", value=None)
        archivo = st.file_uploader("Archivo contrato (opcional)", key="up_contrato_faena")

        if st.button("Guardar contrato de faena", type="primary", disabled=not nombre.strip()):
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
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df('''
        SELECT cf.id, m.nombre AS mandante, cf.nombre, cf.fecha_inicio, cf.fecha_termino,
               CASE WHEN cf.file_path IS NULL THEN '(sin archivo)' ELSE 'OK' END AS archivo
        FROM contratos_faena cf
        JOIN mandantes m ON m.id=cf.mandante_id
        ORDER BY cf.id DESC
    ''')
    st.dataframe(df, use_container_width=True)

    st.divider()
    st.subheader("Actualizar / agregar archivo a contrato existente")
    if df.empty:
        st.info("No hay contratos.")
        return
    contrato_id = st.selectbox("Contrato de faena", df["id"].tolist(), format_func=lambda x: f"{x} - {df[df['id']==x].iloc[0]['mandante']} / {df[df['id']==x].iloc[0]['nombre']}")
    up = st.file_uploader("Archivo contrato", key="up_contrato_existente")
    if st.button("Guardar archivo en contrato", disabled=up is None):
        b = up.getvalue()
        file_path = save_file(["contratos_faena", "id", contrato_id], up.name, b)
        sha = sha256_bytes(b)
        execute(
            "UPDATE contratos_faena SET file_path=?, sha256=?, created_at=? WHERE id=?",
            (file_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(contrato_id)),
        )
        st.success("Archivo actualizado.")
        st.rerun()

def page_faenas():
    st.subheader("Faenas (por mandante) + anexos")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        st.info("Primero crea un mandante.")
        return

    contratos = fetch_df('''
        SELECT cf.id, cf.nombre, cf.mandante_id, m.nombre AS mandante
        FROM contratos_faena cf
        JOIN mandantes m ON m.id=cf.mandante_id
        ORDER BY m.nombre, cf.nombre
    ''')

    with st.expander("Crear faena", expanded=True):
        mandante_id = st.selectbox("Mandante", mand["id"].tolist(), format_func=lambda x: mand[mand["id"]==x].iloc[0]["nombre"])
        contratos_m = contratos[contratos["mandante_id"] == mandante_id] if not contratos.empty else pd.DataFrame()
        contrato_opts = [None] + (contratos_m["id"].tolist() if not contratos_m.empty else [])

        def _fmt_contrato(x):
            if x is None:
                return "(sin contrato asociado)"
            row = contratos[contratos["id"] == x]
            if row.empty:
                return str(x)
            return f"{int(x)} - {row.iloc[0]['nombre']}"

        contrato_id = st.selectbox("Contrato de faena (opcional)", contrato_opts, format_func=_fmt_contrato)
        nombre = st.text_input("Nombre faena", placeholder="Bellavista 3")
        ubicacion = st.text_input("Ubicación", placeholder="Predio / Comuna")
        fi = st.date_input("Fecha inicio", value=date.today())
        ft = st.date_input("Fecha término (opcional)", value=None)
        estado = st.selectbox("Estado", ESTADOS_FAENA, index=0)

        errors = validate_faena_dates(fi, ft, estado)
        for e in errors:
            st.error(e)

        if st.button("Guardar faena", type="primary", disabled=bool(errors) or not nombre.strip()):
            try:
                execute(
                    "INSERT INTO faenas(mandante_id, contrato_faena_id, nombre, ubicacion, fecha_inicio, fecha_termino, estado) VALUES(?,?,?,?,?,?,?)",
                    (int(mandante_id), int(contrato_id) if contrato_id else None, nombre.strip(), ubicacion.strip(), str(fi), str(ft) if ft else None, estado),
                )
                st.success("Faena creada.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio, f.fecha_termino, f.ubicacion,
               COALESCE(cf.nombre, '') AS contrato_faena
        FROM faenas f
        JOIN mandantes m ON m.id=f.mandante_id
        LEFT JOIN contratos_faena cf ON cf.id=f.contrato_faena_id
        ORDER BY f.id DESC
    ''')
    st.dataframe(df, use_container_width=True)

    st.divider()
    st.subheader("Subir anexos por faena (opcional)")
    if df.empty:
        st.info("No hay faenas.")
        return
    faena_id = st.selectbox("Faena", df["id"].tolist(), format_func=lambda x: f"{x} - {df[df['id']==x].iloc[0]['mandante']} / {df[df['id']==x].iloc[0]['nombre']}")
    up = st.file_uploader("Archivo anexo", key="up_anexo_faena")
    if st.button("Guardar anexo", disabled=up is None):
        b = up.getvalue()
        file_path = save_file(["faenas", faena_id, "anexos"], up.name, b)
        sha = sha256_bytes(b)
        execute(
            "INSERT INTO faena_anexos(faena_id, nombre, file_path, sha256, created_at) VALUES(?,?,?,?,?)",
            (int(faena_id), up.name, file_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
        )
        st.success("Anexo guardado.")
        st.rerun()

    anexos = fetch_df("SELECT id, nombre, created_at FROM faena_anexos WHERE faena_id=? ORDER BY id DESC", (int(faena_id),))
    st.caption("Anexos cargados")
    st.dataframe(anexos if not anexos.empty else pd.DataFrame([{"info":"(sin anexos)"}]), use_container_width=True)

def page_trabajadores():
    st.subheader("Trabajadores")

    # --- Importar Excel ---
    with st.expander("Importar trabajadores desde Excel (como tu plantilla)", expanded=True):
        st.write("Columnas soportadas (recomendado): **RUT, NOMBRE, CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN**.")
        up = st.file_uploader("Sube Excel (.xlsx)", type=["xlsx"], key="up_excel_trabajadores")

        if up is not None:
            try:
                xls = pd.ExcelFile(up)
                sheet = st.selectbox("Hoja", xls.sheet_names, index=0)
                raw = pd.read_excel(xls, sheet_name=sheet)

                # Normalizar columnas
                colmap = {c: norm_col(str(c)) for c in raw.columns}
                df = raw.rename(columns=colmap).copy()

                st.caption("Vista previa (primeras 10 filas)")
                st.dataframe(df.head(10), use_container_width=True)

                if "rut" not in df.columns or "nombre" not in df.columns:
                    st.error("El Excel debe tener columnas 'RUT' y 'NOMBRE'.")
                else:
                    overwrite = st.checkbox("Sobrescribir si el RUT ya existe", value=True)

                    if st.button("Importar Excel ahora", type="primary"):
                        # RUTs existentes para conteo insert/update real
                        existing = fetch_df("SELECT rut FROM trabajadores")["rut"].astype(str).tolist()
                        existing_set = set(existing)

                        rows = 0
                        inserted = 0
                        updated = 0
                        skipped = 0

                        # columnas opcionales
                        has_cargo = "cargo" in df.columns
                        has_cc = "centro_costo" in df.columns
                        has_email = "email" in df.columns
                        fc_col = "fecha_de_contrato" if "fecha_de_contrato" in df.columns else ("fecha_contrato" if "fecha_contrato" in df.columns else None)
                        has_ve = "vigencia_examen" in df.columns

                        def _to_text_date(v):
                            if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
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
                                        (rut, nombres or "-", apellidos or "-", cargo, centro_costo, email, fecha_contrato, vigencia_examen),
                                    )
                                    if rut in existing_set:
                                        updated += 1
                                    else:
                                        inserted += 1
                                        existing_set.add(rut)
                                else:
                                    c.execute(
                                        '''
                                        INSERT OR IGNORE INTO trabajadores(rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)
                                        VALUES(?,?,?,?,?,?,?,?)
                                        ''',
                                        (rut, nombres or "-", apellidos or "-", cargo, centro_costo, email, fecha_contrato, vigencia_examen),
                                    )
                                    # rowcount: 1 insert, 0 ignore
                                    if c.total_changes:
                                        # total_changes es acumulado; para exactitud preferimos comparar set
                                        if rut not in existing_set:
                                            inserted += 1
                                            existing_set.add(rut)

                            c.commit()

                        st.success(f"Importación lista. Filas leídas: {rows} | Insertados: {inserted} | Actualizados: {updated} | Omitidos: {skipped}")
                        st.rerun()

            except Exception as e:
                st.error(f"No se pudo leer el Excel: {e}")

    # --- Crear manual ---
    with st.expander("Crear / editar trabajador manual", expanded=False):
        rut = st.text_input("RUT", placeholder="12.345.678-9")
        nombres = st.text_input("Nombres", placeholder="Juan")
        apellidos = st.text_input("Apellidos", placeholder="Pérez")
        cargo = st.text_input("Cargo", placeholder="Operador Harvester")
        centro_costo = st.text_input("Centro de costo (opcional)", placeholder="FAENA")
        email = st.text_input("Email (opcional)")
        fecha_contrato = st.date_input("Fecha de contrato (opcional)", value=None)
        vigencia_examen = st.date_input("Vigencia examen (opcional)", value=None)

        if st.button("Guardar trabajador", type="primary", disabled=not (rut.strip() and nombres.strip() and apellidos.strip())):
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
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar: {e}")

    st.divider()
    df = fetch_df("SELECT id, rut, apellidos, nombres, cargo, centro_costo, email, fecha_contrato, vigencia_examen FROM trabajadores ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)

def page_asignar_trabajadores():
    st.subheader("Asignar trabajadores a faena")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")

    if faenas.empty:
        st.info("Crea faenas primero.")
        return
    if trab.empty:
        st.info("Crea trabajadores primero.")
        return

    faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']}")
    asignados = fetch_df("SELECT trabajador_id FROM asignaciones WHERE faena_id=?", (int(faena_id),))
    asignados_ids = set(asignados["trabajador_id"].tolist()) if not asignados.empty else set()
    disponibles = trab[~trab["id"].isin(asignados_ids)].copy()

    def _fmt_trab(x):
        r = trab[trab["id"]==x].iloc[0]
        return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

    if disponibles.empty:
        st.success("Todos los trabajadores ya están asignados.")
    else:
        seleccion = st.multiselect("Selecciona trabajadores", disponibles["id"].tolist(), format_func=_fmt_trab)
        fecha_ingreso = st.date_input("Fecha ingreso", value=date.today())
        cargo_faena = st.text_input("Cargo en faena (opcional, aplica a todos)")
        if st.button("Asignar seleccionados", type="primary", disabled=len(seleccion)==0):
            params = []
            for tid in seleccion:
                params.append((int(faena_id), int(tid), cargo_faena.strip(), str(fecha_ingreso), None, "ACTIVA"))
            executemany(
                "INSERT OR IGNORE INTO asignaciones(faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado) VALUES(?,?,?,?,?,?)",
                params,
            )
            st.success("Trabajadores asignados.")
            st.rerun()

    st.divider()
    st.subheader("Asignados en esta faena")
    asg = fetch_df('''
        SELECT t.apellidos || ' ' || t.nombres AS trabajador, t.rut, a.cargo_faena, a.fecha_ingreso, a.estado
        FROM asignaciones a JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.faena_id=?
        ORDER BY t.apellidos, t.nombres
    ''', (int(faena_id),))
    st.dataframe(asg, use_container_width=True)

def page_documentos_trabajador():
    st.subheader("Documentos por trabajador (obligatorios + extras)")
    trab = fetch_df("SELECT id, rut, apellidos, nombres FROM trabajadores ORDER BY apellidos, nombres")
    if trab.empty:
        st.info("Crea trabajadores primero.")
        return

    tid = st.selectbox("Trabajador", trab["id"].tolist(), format_func=lambda x: f"{trab[trab['id']==x].iloc[0]['apellidos']} {trab[trab['id']==x].iloc[0]['nombres']} ({trab[trab['id']==x].iloc[0]['rut']})")

    st.write("**Tipos obligatorios disponibles:**")
    st.code("\n".join(DOC_OBLIGATORIOS))

    col1, col2 = st.columns([1, 2])
    with col1:
        tipo = st.selectbox("Tipo de documento", DOC_OBLIGATORIOS + ["OTRO"])
    with col2:
        tipo_otro = st.text_input("Si eliges OTRO, escribe el nombre", placeholder="Ej: Certificación operador, Licencia, Examen ocupacional")

    up = st.file_uploader("Archivo", key="up_doc_trabajador")
    if st.button("Guardar documento", type="primary", disabled=up is None):
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
        st.rerun()

    st.divider()
    docs = fetch_df("SELECT id, doc_tipo, nombre_archivo, created_at FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id DESC", (int(tid),))
    st.caption("Documentos cargados para este trabajador")
    st.dataframe(docs if not docs.empty else pd.DataFrame([{"info":"(sin documentos)"}]), use_container_width=True)

def page_documentos_extra_faena():
    st.subheader("Documentos extra por faena (opcional)")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        st.info("Crea faenas primero.")
        return

    faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']}")
    categoria = st.text_input("Categoría (opcional)", placeholder="Ej: permiso_trabajo, programa_prevencion, plan_emergencia")
    up = st.file_uploader("Archivo", key="up_doc_faena_extra")

    if st.button("Guardar doc extra faena", type="primary", disabled=up is None):
        b = up.getvalue()
        file_path = save_file(["faenas", faena_id, "docs_extra", safe_name(categoria or "general")], up.name, b)
        sha = sha256_bytes(b)
        execute(
            "INSERT INTO faena_documentos(faena_id, categoria, nombre_archivo, file_path, sha256, created_at) VALUES(?,?,?,?,?,?)",
            (int(faena_id), categoria.strip(), up.name, file_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
        )
        st.success("Documento extra guardado.")
        st.rerun()

    st.divider()
    docs = fetch_df("SELECT id, categoria, nombre_archivo, created_at FROM faena_documentos WHERE faena_id=? ORDER BY id DESC", (int(faena_id),))
    st.caption("Documentos extra cargados")
    st.dataframe(docs if not docs.empty else pd.DataFrame([{"info":"(sin documentos)"}]), use_container_width=True)

def page_export_zip():
    st.subheader("Export (ZIP) – Carpeta por Faena")
    faenas = fetch_df('''
        SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        st.info("Crea una faena primero.")
        return

    faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['mandante']} / {faenas[faenas['id']==x].iloc[0]['nombre']} ({faenas[faenas['id']==x].iloc[0]['estado']})")
    pend = pendientes_obligatorios(int(faena_id))

    st.write("**Pendientes obligatorios antes de exportar:**")
    if not pend:
        st.info("(sin trabajadores asignados)")
    else:
        for k, missing in pend.items():
            if missing:
                st.error(f"{k} — faltan: {', '.join(missing)}")
            else:
                st.success(f"{k} — OK")

    st.divider()
    if st.button("Generar ZIP"):
        try:
            data = export_zip_for_faena(int(faena_id))
            fname = f"faena_{faena_id}_{safe_name(faenas[faenas['id']==faena_id].iloc[0]['nombre'])}.zip"
            st.download_button("Descargar ZIP", data=data, file_name=fname, mime="application/zip")
            st.success("ZIP generado.")
        except Exception as e:
            st.error(f"No se pudo generar ZIP: {e}")

# ----------------------------
# Route
# ----------------------------
if page == "Dashboard":
    page_dashboard()
elif page == "Mandantes":
    page_mandantes()
elif page == "Contratos de Faena":
    page_contratos_faena()
elif page == "Faenas":
    page_faenas()
elif page == "Trabajadores":
    page_trabajadores()
elif page == "Asignar Trabajadores":
    page_asignar_trabajadores()
elif page == "Documentos Trabajador":
    page_documentos_trabajador()
elif page == "Documentos Extra Faena":
    page_documentos_extra_faena()
elif page == "Export (ZIP)":
    page_export_zip()
