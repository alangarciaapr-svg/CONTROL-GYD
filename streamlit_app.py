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
st.set_page_config(page_title="Gestión de Faenas – MVP", layout="wide")

APP_NAME = "Gestión de Faenas – MVP (sin checklist)"
DB_PATH = "app.db"
UPLOAD_ROOT = "uploads"  # AVISO: Streamlit Cloud FS no es storage duradero a largo plazo.
ESTADOS_FAENA = ["PLANIFICADA", "ACTIVA", "TERMINADA"]

# ----------------------------
# Helpers
# ----------------------------
def safe_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "item"

def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def ensure_dirs():
    os.makedirs(UPLOAD_ROOT, exist_ok=True)

def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

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
        CREATE TABLE IF NOT EXISTS contratos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandante_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            fecha_inicio TEXT,
            fecha_termino TEXT,
            FOREIGN KEY(mandante_id) REFERENCES mandantes(id) ON DELETE RESTRICT
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS contrato_archivos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contrato_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(contrato_id) REFERENCES contratos(id) ON DELETE CASCADE
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS faenas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contrato_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            ubicacion TEXT DEFAULT '',
            fecha_inicio TEXT NOT NULL,
            fecha_termino TEXT,
            estado TEXT NOT NULL CHECK(estado IN ('PLANIFICADA','ACTIVA','TERMINADA')),
            FOREIGN KEY(contrato_id) REFERENCES contratos(id) ON DELETE RESTRICT
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS faena_archivos (
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

        # Documentos libres (sin tipos / checklist)
        c.execute('''
        CREATE TABLE IF NOT EXISTS faena_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faena_id INTEGER NOT NULL,
            categoria TEXT DEFAULT '',
            nombre TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(faena_id) REFERENCES faenas(id) ON DELETE CASCADE
        );
        ''')

        c.execute('''
        CREATE TABLE IF NOT EXISTS trabajador_documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trabajador_id INTEGER NOT NULL,
            categoria TEXT DEFAULT '',
            nombre TEXT NOT NULL,
            file_path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(trabajador_id) REFERENCES trabajadores(id) ON DELETE CASCADE
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

def parse_date(s):
    if not s:
        return None
    return date.fromisoformat(s)

def validate_faena_dates(inicio: date, termino, estado: str):
    errors = []
    if termino and termino < inicio:
        errors.append("La fecha de término no puede ser anterior a la fecha de inicio.")
    if estado == "TERMINADA" and not termino:
        errors.append("Si la faena está TERMINADA, debes indicar fecha término.")
    return errors

def save_file(folder_parts, file_name: str, file_bytes: bytes):
    ensure_dirs()
    folder = os.path.join(UPLOAD_ROOT, *[str(x) for x in folder_parts])
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, file_name)
    with open(path, "wb") as f:
        f.write(file_bytes)
    return path

def export_zip_for_faena(faena_id: int):
    faena = fetch_df('''
        SELECT f.*, c.nombre AS contrato_nombre, c.id AS contrato_id, m.nombre AS mandante_nombre
        FROM faenas f
        JOIN contratos c ON c.id = f.contrato_id
        JOIN mandantes m ON m.id = c.mandante_id
        WHERE f.id = ?
    ''', (faena_id,))
    if faena.empty:
        raise ValueError("Faena no encontrada.")
    f = faena.iloc[0]
    contrato_id = int(f["contrato_id"])

    buff = io.BytesIO()
    z = zipfile.ZipFile(buff, "w", zipfile.ZIP_DEFLATED)

    # Index simple
    idx_lines = [
        f"FAENA: {f['nombre']}",
        f"ESTADO: {f['estado']}",
        f"INICIO: {f['fecha_inicio']} | TERMINO: {f['fecha_termino'] or '-'}",
        f"UBICACION: {f['ubicacion'] or '-'}",
        f"MANDANTE: {f['mandante_nombre']}",
        f"CONTRATO: {f['contrato_nombre']}",
        "",
        "CONTENIDO INCLUIDO:",
        "- 00_Contrato_y_Anexos/",
        "- 01_Documentos_Faena/",
        "- 02_Trabajadores/",
        "",
        "NOTA: Esta versión no usa checklist ni tipos de documentos; es carga libre por categoría."
    ]
    z.writestr("99_Index.txt", "\n".join(idx_lines))

    # 00_Contrato_y_Anexos
    cfiles = fetch_df("SELECT * FROM contrato_archivos WHERE contrato_id=? ORDER BY id", (contrato_id,))
    for _, r in cfiles.iterrows():
        src = r["file_path"]
        if src and os.path.exists(src):
            fname = os.path.basename(src)
            arc = f"00_Contrato_y_Anexos/Contrato/{fname}"
            with open(src, "rb") as fsrc:
                z.writestr(arc, fsrc.read())

    afiles = fetch_df("SELECT * FROM faena_archivos WHERE faena_id=? ORDER BY id", (faena_id,))
    for _, r in afiles.iterrows():
        src = r["file_path"]
        if src and os.path.exists(src):
            fname = os.path.basename(src)
            arc = f"00_Contrato_y_Anexos/Anexo_Faena/{fname}"
            with open(src, "rb") as fsrc:
                z.writestr(arc, fsrc.read())

    # 01_Documentos_Faena (libres)
    fdocs = fetch_df("SELECT * FROM faena_documentos WHERE faena_id=? ORDER BY id", (faena_id,))
    for _, d in fdocs.iterrows():
        src = d["file_path"]
        if src and os.path.exists(src):
            fname = os.path.basename(src)
            cat = safe_name(d.get("categoria") or "general")
            arc = f"01_Documentos_Faena/{cat}/{fname}"
            with open(src, "rb") as fsrc:
                z.writestr(arc, fsrc.read())

    # 02_Trabajadores (docs libres por trabajador asignado)
    asign = fetch_df('''
        SELECT a.*, t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
        FROM asignaciones a
        JOIN trabajadores t ON t.id = a.trabajador_id
        WHERE a.faena_id=?
        ORDER BY t.apellidos, t.nombres
    ''', (faena_id,))
    for _, a in asign.iterrows():
        t_id = int(a["trabajador_id"])
        t_folder = f"{safe_name(a['apellidos'])}_{safe_name(a['nombres'])}_{safe_name(a['rut'])}"
        tdocs = fetch_df("SELECT * FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id", (t_id,))
        for _, d in tdocs.iterrows():
            src = d["file_path"]
            if src and os.path.exists(src):
                fname = os.path.basename(src)
                cat = safe_name(d.get("categoria") or "general")
                arc = f"02_Trabajadores/{t_folder}/{cat}/{fname}"
                with open(src, "rb") as fsrc:
                    z.writestr(arc, fsrc.read())

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
        ["Dashboard", "Mandantes", "Contratos", "Faenas", "Trabajadores", "Asignaciones", "Documentos", "Export (ZIP)"],
        index=0,
    )
    st.caption("Simplificado: contrato/anexo + documentos libres por categoría.")

# ----------------------------
# Pages
# ----------------------------
def page_dashboard():
    st.subheader("Dashboard")
    faenas = fetch_df('''
        SELECT f.id, f.nombre, f.estado, f.fecha_inicio, f.fecha_termino,
               c.nombre AS contrato, m.nombre AS mandante
        FROM faenas f
        JOIN contratos c ON c.id=f.contrato_id
        JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY f.id DESC
    ''')
    if faenas.empty:
        st.info("Crea un mandante, contrato y faena para comenzar.")
        return
    col1, col2 = st.columns([2, 1])
    with col1:
        st.dataframe(faenas, use_container_width=True)
    with col2:
        st.metric("Faenas", len(faenas))
        st.metric("Activas", int((faenas["estado"] == "ACTIVA").sum()))
        st.metric("Terminadas", int((faenas["estado"] == "TERMINADA").sum()))

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

def page_contratos():
    st.subheader("Contratos (por mandante) + subir contrato")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        st.info("Primero crea un mandante.")
        return

    with st.expander("Crear contrato", expanded=True):
        mandante_id = st.selectbox(
            "Mandante",
            mand["id"].tolist(),
            format_func=lambda x: mand[mand["id"]==x].iloc[0]["nombre"],
        )
        nombre = st.text_input("Nombre contrato", placeholder="Contrato marco 2026")
        fi = st.date_input("Fecha inicio (opcional)", value=None)
        ft = st.date_input("Fecha término (opcional)", value=None)

        if st.button("Guardar contrato", type="primary", disabled=not nombre.strip()):
            try:
                execute(
                    "INSERT INTO contratos(mandante_id, nombre, fecha_inicio, fecha_termino) VALUES(?,?,?,?)",
                    (int(mandante_id), nombre.strip(), str(fi) if fi else None, str(ft) if ft else None),
                )
                st.success("Contrato creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df('''
        SELECT c.id, c.nombre, m.nombre AS mandante, c.fecha_inicio, c.fecha_termino
        FROM contratos c JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY c.id DESC
    ''')
    st.dataframe(df, use_container_width=True)

    st.divider()
    st.subheader("Subir archivo de contrato (PDF/Word/etc.)")
    if df.empty:
        st.info("Crea un contrato primero.")
        return

    contrato_id = st.selectbox(
        "Contrato",
        df["id"].tolist(),
        format_func=lambda x: f"{x} - {df[df['id']==x].iloc[0]['mandante']} / {df[df['id']==x].iloc[0]['nombre']}",
    )
    archivo = st.file_uploader("Archivo contrato", key="up_contrato")
    if st.button("Guardar archivo de contrato", disabled=archivo is None):
        file_bytes = archivo.getvalue()
        file_name = archivo.name
        file_path = save_file(["contratos", contrato_id], file_name, file_bytes)
        h = sha256_bytes(file_bytes)
        execute(
            "INSERT INTO contrato_archivos(contrato_id, nombre, file_path, sha256, created_at) VALUES(?,?,?,?,?)",
            (int(contrato_id), file_name, file_path, h, datetime.utcnow().isoformat(timespec="seconds")),
        )
        st.success("Archivo de contrato guardado.")
        st.rerun()

    cfiles = fetch_df(
        "SELECT id, nombre, created_at FROM contrato_archivos WHERE contrato_id=? ORDER BY id DESC",
        (int(contrato_id),),
    )
    st.caption("Archivos cargados para este contrato")
    st.dataframe(cfiles if not cfiles.empty else pd.DataFrame([{"info":"(sin archivos)"}]), use_container_width=True)

def page_faenas():
    st.subheader("Faenas (por contrato) + anexo + trabajadores")
    contratos = fetch_df('''
        SELECT c.id, c.nombre, m.nombre AS mandante
        FROM contratos c JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY m.nombre, c.nombre
    ''')
    if contratos.empty:
        st.info("Primero crea un contrato.")
        return

    with st.expander("Crear faena", expanded=True):
        contrato_id = st.selectbox(
            "Contrato",
            contratos["id"].tolist(),
            format_func=lambda x: f"{contratos[contratos['id']==x].iloc[0]['mandante']} - {contratos[contratos['id']==x].iloc[0]['nombre']}",
        )
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
                    "INSERT INTO faenas(contrato_id, nombre, ubicacion, fecha_inicio, fecha_termino, estado) VALUES(?,?,?,?,?,?)",
                    (int(contrato_id), nombre.strip(), ubicacion.strip(), str(fi), str(ft) if ft else None, estado),
                )
                st.success("Faena creada.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    st.divider()
    df = fetch_df('''
        SELECT f.id, f.nombre, f.estado, f.fecha_inicio, f.fecha_termino, f.ubicacion,
               c.nombre AS contrato, m.nombre AS mandante, f.contrato_id
        FROM faenas f
        JOIN contratos c ON c.id=f.contrato_id
        JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY f.id DESC
    ''')
    st.dataframe(df, use_container_width=True)
    if df.empty:
        return

    st.divider()
    st.subheader("Subir anexo de faena")
    faena_id = st.selectbox(
        "Faena",
        df["id"].tolist(),
        format_func=lambda x: f"{x} - {df[df['id']==x].iloc[0]['mandante']} / {df[df['id']==x].iloc[0]['nombre']}",
    )
    aup = st.file_uploader("Archivo anexo faena", key="up_anexo")
    if st.button("Guardar anexo de faena", disabled=aup is None):
        file_bytes = aup.getvalue()
        file_name = aup.name
        file_path = save_file(["faenas", faena_id, "anexo"], file_name, file_bytes)
        h = sha256_bytes(file_bytes)
        execute(
            "INSERT INTO faena_archivos(faena_id, nombre, file_path, sha256, created_at) VALUES(?,?,?,?,?)",
            (int(faena_id), file_name, file_path, h, datetime.utcnow().isoformat(timespec="seconds")),
        )
        st.success("Anexo guardado.")
        st.rerun()

    afiles = fetch_df("SELECT id, nombre, created_at FROM faena_archivos WHERE faena_id=? ORDER BY id DESC", (int(faena_id),))
    st.caption("Anexos cargados para esta faena")
    st.dataframe(afiles if not afiles.empty else pd.DataFrame([{"info":"(sin anexos)"}]), use_container_width=True)

    st.divider()
    st.subheader("Agregar trabajadores a esta faena (rápido)")
    trabajadores = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
    if trabajadores.empty:
        st.info("Primero crea trabajadores.")
        return

    asignados = fetch_df("SELECT trabajador_id FROM asignaciones WHERE faena_id=?", (int(faena_id),))
    asignados_ids = set(asignados["trabajador_id"].tolist()) if not asignados.empty else set()
    disponibles = trabajadores[~trabajadores["id"].isin(asignados_ids)].copy()

    def _fmt_trab(x):
        r = trabajadores[trabajadores["id"] == x].iloc[0]
        return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

    if disponibles.empty:
        st.success("Todos los trabajadores ya están asignados a esta faena.")
        return

    seleccion = st.multiselect("Selecciona trabajadores", disponibles["id"].tolist(), format_func=_fmt_trab)
    fecha_ingreso = st.date_input("Fecha ingreso", value=date.today(), key="bulk_ingreso")
    cargo_faena = st.text_input("Cargo en faena (opcional, aplica a todos)", key="bulk_cargo")

    if st.button("Asignar seleccionados", type="primary", disabled=len(seleccion) == 0):
        params = []
        for tid in seleccion:
            params.append((int(faena_id), int(tid), cargo_faena.strip(), str(fecha_ingreso), None, "ACTIVA"))
        try:
            executemany(
                "INSERT OR IGNORE INTO asignaciones(faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado) VALUES(?,?,?,?,?,?)",
                params,
            )
            st.success("Trabajadores asignados.")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo asignar: {e}")

def page_trabajadores():
    st.subheader("Trabajadores")
    with st.expander("Crear trabajador", expanded=True):
        rut = st.text_input("RUT", placeholder="12.345.678-9")
        nombres = st.text_input("Nombres", placeholder="Juan")
        apellidos = st.text_input("Apellidos", placeholder="Pérez")
        cargo = st.text_input("Cargo", placeholder="Operador Harvester")
        if st.button("Guardar trabajador", type="primary", disabled=not (rut.strip() and nombres.strip() and apellidos.strip())):
            try:
                execute(
                    "INSERT INTO trabajadores(rut, nombres, apellidos, cargo) VALUES(?,?,?,?)",
                    (rut.strip(), nombres.strip(), apellidos.strip(), cargo.strip()),
                )
                st.success("Trabajador creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")
    df = fetch_df("SELECT * FROM trabajadores ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)

def page_asignaciones():
    st.subheader("Asignaciones (detalle)")
    df = fetch_df('''
        SELECT a.id, f.nombre AS faena, t.apellidos || ' ' || t.nombres AS trabajador, t.rut,
               a.cargo_faena, a.fecha_ingreso, a.fecha_egreso, a.estado
        FROM asignaciones a
        JOIN faenas f ON f.id=a.faena_id
        JOIN trabajadores t ON t.id=a.trabajador_id
        ORDER BY a.id DESC
    ''')
    st.dataframe(df, use_container_width=True)

def page_documentos():
    st.subheader("Documentos (carga libre por categoría)")
    tab1, tab2 = st.tabs(["Subir", "Listado"])

    with tab1:
        scope = st.selectbox("Tipo de documento", ["FAENA", "TRABAJADOR"])
        categoria = st.text_input("Categoría (opcional)", placeholder="Ej: induccion, epp, examen_preocupacional")
        if scope == "FAENA":
            owners = fetch_df("SELECT id, nombre FROM faenas ORDER BY id DESC")
            if owners.empty:
                st.info("No hay faenas.")
                return
            owner_id = st.selectbox("Faena", owners["id"].tolist(), format_func=lambda x: f"{x} - {owners[owners['id']==x].iloc[0]['nombre']}")
            up = st.file_uploader("Archivo", key="up_fdoc")
            if st.button("Guardar documento FAENA", type="primary", disabled=up is None):
                file_bytes = up.getvalue()
                file_name = up.name
                file_path = save_file(["docs", "faena", owner_id, safe_name(categoria or "general")], file_name, file_bytes)
                h = sha256_bytes(file_bytes)
                execute(
                    "INSERT INTO faena_documentos(faena_id, categoria, nombre, file_path, sha256, created_at) VALUES(?,?,?,?,?,?)",
                    (int(owner_id), categoria.strip(), file_name, file_path, h, datetime.utcnow().isoformat(timespec="seconds")),
                )
                st.success("Documento de faena guardado.")
                st.rerun()
        else:
            owners = fetch_df("SELECT id, apellidos || ' ' || nombres AS nombre FROM trabajadores ORDER BY apellidos, nombres")
            if owners.empty:
                st.info("No hay trabajadores.")
                return
            owner_id = st.selectbox("Trabajador", owners["id"].tolist(), format_func=lambda x: f"{x} - {owners[owners['id']==x].iloc[0]['nombre']}")
            up = st.file_uploader("Archivo", key="up_tdoc")
            if st.button("Guardar documento TRABAJADOR", type="primary", disabled=up is None):
                file_bytes = up.getvalue()
                file_name = up.name
                file_path = save_file(["docs", "trabajador", owner_id, safe_name(categoria or "general")], file_name, file_bytes)
                h = sha256_bytes(file_bytes)
                execute(
                    "INSERT INTO trabajador_documentos(trabajador_id, categoria, nombre, file_path, sha256, created_at) VALUES(?,?,?,?,?,?)",
                    (int(owner_id), categoria.strip(), file_name, file_path, h, datetime.utcnow().isoformat(timespec="seconds")),
                )
                st.success("Documento de trabajador guardado.")
                st.rerun()

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Documentos FAENA**")
            fdocs = fetch_df('''
                SELECT d.id, d.faena_id, f.nombre AS faena, d.categoria, d.nombre, d.created_at
                FROM faena_documentos d
                JOIN faenas f ON f.id=d.faena_id
                ORDER BY d.id DESC
            ''')
            st.dataframe(fdocs, use_container_width=True)
        with col2:
            st.write("**Documentos TRABAJADOR**")
            tdocs = fetch_df('''
                SELECT d.id, d.trabajador_id, t.apellidos || ' ' || t.nombres AS trabajador, d.categoria, d.nombre, d.created_at
                FROM trabajador_documentos d
                JOIN trabajadores t ON t.id=d.trabajador_id
                ORDER BY d.id DESC
            ''')
            st.dataframe(tdocs, use_container_width=True)

def page_export_zip():
    st.subheader("Export (ZIP) – Carpeta de Faena")
    faenas = fetch_df("SELECT id, nombre FROM faenas ORDER BY id DESC")
    if faenas.empty:
        st.info("Crea una faena primero.")
        return

    faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['nombre']}")
    if st.button("Generar ZIP"):
        try:
            data = export_zip_for_faena(int(faena_id))
            fname = f"faena_{faena_id}_{safe_name(faenas[faenas['id']==faena_id].iloc[0]['nombre'])}.zip"
            st.download_button("Descargar ZIP", data=data, file_name=fname, mime="application/zip")
            st.success("ZIP generado.")
        except Exception as e:
            st.error(f"No se pudo generar ZIP: {e}")

# Route
if page == "Dashboard":
    page_dashboard()
elif page == "Mandantes":
    page_mandantes()
elif page == "Contratos":
    page_contratos()
elif page == "Faenas":
    page_faenas()
elif page == "Trabajadores":
    page_trabajadores()
elif page == "Asignaciones":
    page_asignaciones()
elif page == "Documentos":
    page_documentos()
elif page == "Export (ZIP)":
    page_export_zip()
