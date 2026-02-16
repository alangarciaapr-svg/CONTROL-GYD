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

APP_NAME = "Gestión de Faenas – MVP"
DB_PATH = "app.db"
UPLOAD_ROOT = "uploads"  # AVISO: el filesystem en Streamlit Cloud no es storage duradero a largo plazo.

ESTADOS_FAENA = ["PLANIFICADA", "ACTIVA", "TERMINADA"]
ESTADOS_DOC = ["PENDIENTE", "SUBIDO", "APROBADO", "RECHAZADO", "VENCIDO"]
SCOPES = ["FAENA", "TRABAJADOR"]

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
        CREATE TABLE IF NOT EXISTS checklists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandante_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            UNIQUE(mandante_id, nombre),
            FOREIGN KEY(mandante_id) REFERENCES mandantes(id) ON DELETE CASCADE
        );
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS doc_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            scope TEXT NOT NULL CHECK(scope IN ('FAENA','TRABAJADOR')),
            UNIQUE(nombre, scope)
        );
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS checklist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checklist_id INTEGER NOT NULL,
            doc_type_id INTEGER NOT NULL,
            obligatorio INTEGER NOT NULL DEFAULT 1,
            vigencia_dias INTEGER,
            aplica_cargos TEXT DEFAULT '',
            UNIQUE(checklist_id, doc_type_id),
            FOREIGN KEY(checklist_id) REFERENCES checklists(id) ON DELETE CASCADE,
            FOREIGN KEY(doc_type_id) REFERENCES doc_types(id) ON DELETE RESTRICT
        );
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS contratos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mandante_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            checklist_id INTEGER,
            fecha_inicio TEXT,
            fecha_termino TEXT,
            FOREIGN KEY(mandante_id) REFERENCES mandantes(id) ON DELETE RESTRICT,
            FOREIGN KEY(checklist_id) REFERENCES checklists(id) ON DELETE SET NULL
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
        c.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_type_id INTEGER NOT NULL,
            scope TEXT NOT NULL CHECK(scope IN ('FAENA','TRABAJADOR')),
            owner_id INTEGER NOT NULL,
            estado TEXT NOT NULL DEFAULT 'PENDIENTE' CHECK(estado IN ('PENDIENTE','SUBIDO','APROBADO','RECHAZADO','VENCIDO')),
            fecha_emision TEXT,
            fecha_vencimiento TEXT,
            file_path TEXT,
            sha256 TEXT,
            version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            FOREIGN KEY(doc_type_id) REFERENCES doc_types(id) ON DELETE RESTRICT
        );
        ''')
        c.commit()

def fetch_df(q: str, params=()):
    # pd.read_sql_query preserves column names even when there are 0 rows,
    # which avoids KeyError when filtering empty DataFrames.
    with conn() as c:
        return pd.read_sql_query(q, c, params=params)

def execute(q: str, params=()):
    with conn() as c:
        c.execute(q, params)
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

def is_doc_ok(row) -> bool:
    estado = row["estado"]
    fv = parse_date(row.get("fecha_vencimiento"))
    if fv and fv < date.today():
        return False
    return estado in ("SUBIDO", "APROBADO")

def get_checklist_items_for_faena(faena_id: int):
    faena = fetch_df('''
        SELECT f.*, c.checklist_id
        FROM faenas f
        JOIN contratos c ON c.id = f.contrato_id
        WHERE f.id = ?
    ''', (faena_id,))
    if faena.empty:
        return pd.DataFrame()
    checklist_id = faena.iloc[0]["checklist_id"]
    if not checklist_id:
        return pd.DataFrame()
    return fetch_df('''
        SELECT ci.*, dt.nombre AS doc_nombre, dt.scope AS doc_scope
        FROM checklist_items ci
        JOIN doc_types dt ON dt.id = ci.doc_type_id
        WHERE ci.checklist_id = ?
        ORDER BY dt.scope, dt.nombre
    ''', (checklist_id,))

def compute_pendientes(faena_id: int):
    items = get_checklist_items_for_faena(faena_id)
    pendientes = {"FAENA": [], "TRABAJADORES": {}}
    if items.empty:
        return pendientes

    docs_faena = fetch_df('''
        SELECT d.*, dt.nombre AS doc_nombre
        FROM documentos d
        JOIN doc_types dt ON dt.id = d.doc_type_id
        WHERE d.scope='FAENA' AND d.owner_id=?
    ''', (faena_id,))
    faena_ok = set(docs_faena[docs_faena.apply(is_doc_ok, axis=1)]["doc_type_id"].tolist()) if not docs_faena.empty else set()

    faena_items = items[items["doc_scope"] == "FAENA"]
    for _, it in faena_items.iterrows():
        if int(it["obligatorio"]) == 1 and int(it["doc_type_id"]) not in faena_ok:
            pendientes["FAENA"].append(it["doc_nombre"])

    asign = fetch_df('''
        SELECT a.*, t.rut, t.nombres, t.apellidos, t.cargo
        FROM asignaciones a
        JOIN trabajadores t ON t.id = a.trabajador_id
        WHERE a.faena_id = ?
        ORDER BY t.apellidos, t.nombres
    ''', (faena_id,))

    worker_items = items[items["doc_scope"] == "TRABAJADOR"]

    for _, a in asign.iterrows():
        t_id = int(a["trabajador_id"])
        label = f"{a['apellidos']} {a['nombres']} ({a['rut']})"
        pendientes["TRABAJADORES"][label] = []

        docs_t = fetch_df('''
            SELECT d.*, dt.nombre AS doc_nombre
            FROM documentos d
            JOIN doc_types dt ON dt.id = d.doc_type_id
            WHERE d.scope='TRABAJADOR' AND d.owner_id=?
        ''', (t_id,))
        t_ok = set(docs_t[docs_t.apply(is_doc_ok, axis=1)]["doc_type_id"].tolist()) if not docs_t.empty else set()

        cargo_eval = (a["cargo_faena"] or a["cargo"] or "").strip().lower()

        for _, it in worker_items.iterrows():
            if int(it["obligatorio"]) != 1:
                continue

            cargos_csv = (it.get("aplica_cargos") or "").strip()
            if cargos_csv:
                cargos = [c.strip().lower() for c in cargos_csv.split(",") if c.strip()]
                if cargo_eval and cargo_eval not in cargos:
                    continue

            if int(it["doc_type_id"]) not in t_ok:
                pendientes["TRABAJADORES"][label].append(it["doc_nombre"])

    return pendientes

def save_uploaded_file(scope: str, owner_id: int, doc_type_id: int, file_name: str, file_bytes: bytes):
    ensure_dirs()
    folder = os.path.join(UPLOAD_ROOT, scope.lower(), str(owner_id), str(doc_type_id))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, file_name)
    with open(path, "wb") as f:
        f.write(file_bytes)
    return path

def export_zip_for_faena(faena_id: int):
    faena = fetch_df('''
        SELECT f.*, c.nombre AS contrato_nombre, m.nombre AS mandante_nombre
        FROM faenas f
        JOIN contratos c ON c.id = f.contrato_id
        JOIN mandantes m ON m.id = c.mandante_id
        WHERE f.id = ?
    ''', (faena_id,))
    if faena.empty:
        raise ValueError("Faena no encontrada.")
    f = faena.iloc[0]

    pend = compute_pendientes(faena_id)

    idx_lines = []
    idx_lines.append(f"FAENA: {f['nombre']}")
    idx_lines.append(f"ESTADO: {f['estado']}")
    idx_lines.append(f"INICIO: {f['fecha_inicio']} | TERMINO: {f['fecha_termino'] or '-'}")
    idx_lines.append(f"MANDANTE: {f['mandante_nombre']}")
    idx_lines.append(f"CONTRATO: {f['contrato_nombre']}")
    idx_lines.append("")
    idx_lines.append("PENDIENTES FAENA:")
    for p in pend["FAENA"] or ["(sin pendientes)"]:
        idx_lines.append(f" - {p}")
    idx_lines.append("")
    idx_lines.append("PENDIENTES POR TRABAJADOR:")
    for trabajador, faltas in pend["TRABAJADORES"].items():
        idx_lines.append(f"* {trabajador}")
        if faltas:
            for x in faltas:
                idx_lines.append(f"   - {x}")
        else:
            idx_lines.append("   - (sin pendientes)")
        idx_lines.append("")

    buff = io.BytesIO()
    z = zipfile.ZipFile(buff, "w", zipfile.ZIP_DEFLATED)
    z.writestr("99_Index_Checklist.txt", "\n".join(idx_lines))

    docs_faena = fetch_df('''
        SELECT d.*, dt.nombre AS doc_nombre
        FROM documentos d
        JOIN doc_types dt ON dt.id = d.doc_type_id
        WHERE d.scope='FAENA' AND d.owner_id=?
    ''', (faena_id,))

    for _, d in docs_faena.iterrows():
        if not d.get("file_path"):
            continue
        src = d["file_path"]
        if not os.path.exists(src):
            continue
        fname = os.path.basename(src)
        tipo = safe_name(d["doc_nombre"])
        arc = f"01_Documentos_Faena/{tipo}/{fname}"
        with open(src, "rb") as fsrc:
            z.writestr(arc, fsrc.read())

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
        docs_t = fetch_df('''
            SELECT d.*, dt.nombre AS doc_nombre
            FROM documentos d
            JOIN doc_types dt ON dt.id = d.doc_type_id
            WHERE d.scope='TRABAJADOR' AND d.owner_id=?
        ''', (t_id,))
        for _, d in docs_t.iterrows():
            if not d.get("file_path"):
                continue
            src = d["file_path"]
            if not os.path.exists(src):
                continue
            fname = os.path.basename(src)
            tipo = safe_name(d["doc_nombre"])
            arc = f"02_Trabajadores/{t_folder}/{tipo}/{fname}"
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
        ["Dashboard", "Mandantes", "Checklists", "Tipos de documento", "Contratos", "Faenas", "Trabajadores", "Asignaciones", "Documentos", "Export (ZIP)"],
        index=0,
    )
    st.caption("MVP: faena → trabajadores → documentos → pendientes → ZIP.")

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

    st.divider()
    st.subheader("Pendientes rápidos (elige una faena)")
    faena_id = st.selectbox(
        "Faena",
        faenas["id"].tolist(),
        format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['nombre']}",
    )
    pend = compute_pendientes(int(faena_id))
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Pendientes FAENA**")
        st.write(pend["FAENA"] if pend["FAENA"] else ["(sin pendientes)"])
    with c2:
        st.write("**Pendientes por TRABAJADOR**")
        if not pend["TRABAJADORES"]:
            st.write("(sin trabajadores asignados)")
        else:
            for k, v in pend["TRABAJADORES"].items():
                st.write(f"- {k}: {len(v)} pendientes")

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

def page_checklists():
    st.subheader("Checklists (por mandante)")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    if mand.empty:
        st.info("Primero crea un mandante.")
        return

    with st.expander("Crear checklist", expanded=True):
        mandante_id = st.selectbox(
            "Mandante",
            mand["id"].tolist(),
            format_func=lambda x: mand[mand["id"]==x].iloc[0]["nombre"],
        )
        nombre = st.text_input("Nombre checklist", placeholder="Checklist estándar mandante")
        if st.button("Guardar checklist", type="primary", disabled=not nombre.strip()):
            try:
                execute(
                    "INSERT INTO checklists(mandante_id, nombre) VALUES(?,?)",
                    (int(mandante_id), nombre.strip()),
                )
                st.success("Checklist creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    st.divider()
    st.subheader("Agregar ítems a checklist")
    chk = fetch_df('''
        SELECT c.id, c.nombre, m.nombre AS mandante
        FROM checklists c JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY m.nombre, c.nombre
    ''')
    dt = fetch_df("SELECT * FROM doc_types ORDER BY scope, nombre")
    if chk.empty:
        st.info("Crea un checklist.")
        return
    if dt.empty:
        st.info("Crea tipos de documento primero.")
        return

    checklist_id = st.selectbox(
        "Checklist",
        chk["id"].tolist(),
        format_func=lambda x: f"{chk[chk['id']==x].iloc[0]['mandante']} - {chk[chk['id']==x].iloc[0]['nombre']}",
    )
    doc_type_id = st.selectbox(
        "Tipo documento",
        dt["id"].tolist(),
        format_func=lambda x: f"{dt[dt['id']==x].iloc[0]['nombre']} ({dt[dt['id']==x].iloc[0]['scope']})",
    )
    obligatorio = st.checkbox("Obligatorio", value=True)
    vigencia = st.number_input("Vigencia (días) - opcional", min_value=0, value=0, step=1)
    aplica_cargos = st.text_input("Aplica a cargos (CSV) - opcional", placeholder="motosierrista, operador harvester")

    if st.button("Agregar ítem"):
        try:
            execute(
                "INSERT INTO checklist_items(checklist_id, doc_type_id, obligatorio, vigencia_dias, aplica_cargos) VALUES(?,?,?,?,?)",
                (int(checklist_id), int(doc_type_id), 1 if obligatorio else 0, (int(vigencia) if vigencia > 0 else None), aplica_cargos.strip()),
            )
            st.success("Ítem agregado.")
            st.rerun()
        except Exception as e:
            st.error(f"No se pudo agregar: {e}")

    st.divider()
    items = fetch_df('''
        SELECT ci.id, dt.scope, dt.nombre AS tipo_documento, ci.obligatorio, ci.vigencia_dias, ci.aplica_cargos
        FROM checklist_items ci
        JOIN doc_types dt ON dt.id=ci.doc_type_id
        WHERE ci.checklist_id=?
        ORDER BY dt.scope, dt.nombre
    ''', (int(checklist_id),))
    st.dataframe(items, use_container_width=True)

def page_doc_types():
    st.subheader("Tipos de documento")
    with st.expander("Crear tipo de documento", expanded=True):
        nombre = st.text_input("Nombre", placeholder="Inducción mandante")
        scope = st.selectbox("Scope", SCOPES)
        if st.button("Guardar tipo", type="primary", disabled=not nombre.strip()):
            try:
                execute("INSERT INTO doc_types(nombre, scope) VALUES(?,?)", (nombre.strip(), scope))
                st.success("Tipo creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df("SELECT * FROM doc_types ORDER BY scope, nombre")
    st.dataframe(df, use_container_width=True)

def page_contratos():
    st.subheader("Contratos")
    mand = fetch_df("SELECT * FROM mandantes ORDER BY nombre")
    chk = fetch_df('''
        SELECT c.id, c.nombre, m.nombre AS mandante
        FROM checklists c JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY m.nombre, c.nombre
    ''')
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

        mandante_nombre = mand[mand["id"]==mandante_id].iloc[0]["nombre"]
        chk_mandante = chk[chk["mandante"] == mandante_nombre] if (not chk.empty and "mandante" in chk.columns) else chk

        checklist_opts = [None] + (
            chk_mandante["id"].tolist()
            if (chk_mandante is not None and (not chk_mandante.empty) and "id" in chk_mandante.columns)
            else []
        )

        def _fmt_checklist(x):
            if x is None:
                return "(sin checklist)"
            if chk is None or chk.empty or "id" not in chk.columns:
                return str(x)
            row = chk[chk["id"] == x]
            if row.empty:
                return str(x)
            return f"{int(x)} - {row.iloc[0]['nombre']}"

        checklist_id = st.selectbox(
            "Checklist (opcional)",
            checklist_opts,
            format_func=_fmt_checklist,
        )
        fi = st.date_input("Fecha inicio (opcional)", value=None)
        ft = st.date_input("Fecha término (opcional)", value=None)

        if st.button("Guardar contrato", type="primary", disabled=not nombre.strip()):
            try:
                execute(
                    "INSERT INTO contratos(mandante_id, nombre, checklist_id, fecha_inicio, fecha_termino) VALUES(?,?,?,?,?)",
                    (int(mandante_id), nombre.strip(), int(checklist_id) if checklist_id else None, str(fi) if fi else None, str(ft) if ft else None),
                )
                st.success("Contrato creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df('''
        SELECT c.id, c.nombre, m.nombre AS mandante, c.checklist_id, c.fecha_inicio, c.fecha_termino
        FROM contratos c JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY c.id DESC
    ''')
    st.dataframe(df, use_container_width=True)

def page_faenas():
    st.subheader("Faenas")
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
               c.nombre AS contrato, m.nombre AS mandante
        FROM faenas f
        JOIN contratos c ON c.id=f.contrato_id
        JOIN mandantes m ON m.id=c.mandante_id
        ORDER BY f.id DESC
    ''')
    st.dataframe(df, use_container_width=True)

def page_trabajadores():
    st.subheader("Trabajadores")
    with st.expander("Crear trabajador", expanded=True):
        rut = st.text_input("RUT", placeholder="12.345.678-9")
        nombres = st.text_input("Nombres", placeholder="Juan")
        apellidos = st.text_input("Apellidos", placeholder="Pérez")
        cargo = st.text_input("Cargo", placeholder="Operador Harvester")
        if st.button("Guardar trabajador", type="primary", disabled=not (rut.strip() and nombres.strip() and apellidos.strip())):
            try:
                execute("INSERT INTO trabajadores(rut, nombres, apellidos, cargo) VALUES(?,?,?,?)", (rut.strip(), nombres.strip(), apellidos.strip(), cargo.strip()))
                st.success("Trabajador creado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo crear: {e}")

    df = fetch_df("SELECT * FROM trabajadores ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)

def page_asignaciones():
    st.subheader("Asignaciones (Trabajador ↔ Faena)")
    faenas = fetch_df("SELECT id, nombre, estado, fecha_inicio, fecha_termino FROM faenas ORDER BY id DESC")
    trab = fetch_df("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
    if faenas.empty or trab.empty:
        st.info("Crea faenas y trabajadores primero.")
        return

    with st.expander("Asignar trabajador a faena", expanded=True):
        faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['nombre']}")
        trabajador_id = st.selectbox("Trabajador", trab["id"].tolist(), format_func=lambda x: f"{trab[trab['id']==x].iloc[0]['apellidos']} {trab[trab['id']==x].iloc[0]['nombres']} ({trab[trab['id']==x].iloc[0]['rut']})")
        cargo_faena = st.text_input("Cargo en faena (opcional)", placeholder="Si difiere del cargo general")
        fi = st.date_input("Fecha ingreso", value=date.today())
        ft = st.date_input("Fecha egreso (opcional)", value=None)
        estado = st.selectbox("Estado asignación", ["ACTIVA", "CERRADA"], index=0)

        errors = []
        if ft and ft < fi:
            errors.append("La fecha egreso no puede ser anterior a la fecha ingreso.")
        if estado == "CERRADA" and not ft:
            errors.append("Si estado=CERRADA, debes indicar fecha egreso.")

        frow = faenas[faenas["id"] == faena_id].iloc[0]
        faena_inicio = parse_date(frow["fecha_inicio"])
        faena_termino = parse_date(frow["fecha_termino"])
        if faena_inicio and fi < faena_inicio:
            errors.append("Fecha ingreso no puede ser anterior al inicio de faena.")
        if faena_termino and ft and ft > faena_termino:
            errors.append("Fecha egreso no puede ser posterior al término de faena.")

        for e in errors:
            st.error(e)

        if st.button("Guardar asignación", type="primary", disabled=bool(errors)):
            try:
                execute(
                    "INSERT INTO asignaciones(faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado) VALUES(?,?,?,?,?,?)",
                    (int(faena_id), int(trabajador_id), cargo_faena.strip(), str(fi), str(ft) if ft else None, estado),
                )
                st.success("Asignación creada.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo asignar: {e}")

    st.divider()
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
    st.subheader("Documentos (subida por FAENA o TRABAJADOR)")
    dt = fetch_df("SELECT * FROM doc_types ORDER BY scope, nombre")
    if dt.empty:
        st.info("Primero crea tipos de documento.")
        return

    tab1, tab2 = st.tabs(["Subir documento", "Listado"])

    with tab1:
        scope = st.selectbox("Scope", SCOPES)
        scope_dt = dt[dt["scope"] == scope]
        if scope_dt.empty:
            st.info(f"No hay tipos de documento para scope {scope}.")
            return
        doc_type_id = st.selectbox("Tipo de documento", scope_dt["id"].tolist(), format_func=lambda x: scope_dt[scope_dt["id"]==x].iloc[0]["nombre"])

        if scope == "FAENA":
            owners = fetch_df("SELECT id, nombre FROM faenas ORDER BY id DESC")
            owner_label = lambda x: f"{x} - {owners[owners['id']==x].iloc[0]['nombre']}"
        else:
            owners = fetch_df("SELECT id, apellidos || ' ' || nombres AS nombre FROM trabajadores ORDER BY apellidos, nombres")
            owner_label = lambda x: f"{x} - {owners[owners['id']==x].iloc[0]['nombre']}"

        if owners.empty:
            st.info(f"No hay dueños disponibles para {scope}.")
            return

        owner_id = st.selectbox("Dueño", owners["id"].tolist(), format_func=owner_label)
        estado = st.selectbox("Estado", ESTADOS_DOC, index=ESTADOS_DOC.index("SUBIDO"))
        fe = st.date_input("Fecha emisión (opcional)", value=None)
        fv = st.date_input("Fecha vencimiento (opcional)", value=None)

        errors = []
        if fe and fv and fv < fe:
            errors.append("Fecha vencimiento no puede ser anterior a fecha emisión.")
        for e in errors:
            st.error(e)

        up = st.file_uploader("Archivo")
        if st.button("Guardar documento", type="primary", disabled=bool(errors) or up is None):
            file_bytes = up.getvalue()
            file_name = up.name
            file_path = save_uploaded_file(scope, int(owner_id), int(doc_type_id), file_name, file_bytes)
            h = sha256_bytes(file_bytes)

            execute(
                "INSERT INTO documentos(doc_type_id, scope, owner_id, estado, fecha_emision, fecha_vencimiento, file_path, sha256, version, created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (int(doc_type_id), scope, int(owner_id), estado, str(fe) if fe else None, str(fv) if fv else None, file_path, h, 1, datetime.utcnow().isoformat(timespec="seconds")),
            )
            st.success("Documento guardado.")
            st.rerun()

    with tab2:
        docs = fetch_df('''
            SELECT d.id, d.scope, d.owner_id, dt.nombre AS tipo_documento, d.estado, d.fecha_emision, d.fecha_vencimiento, d.created_at, d.file_path
            FROM documentos d
            JOIN doc_types dt ON dt.id=d.doc_type_id
            ORDER BY d.id DESC
        ''')
        st.dataframe(docs, use_container_width=True)

def page_export_zip():
    st.subheader("Export (ZIP) – Carpeta de Faena")
    faenas = fetch_df("SELECT id, nombre FROM faenas ORDER BY id DESC")
    if faenas.empty:
        st.info("Crea una faena primero.")
        return

    faena_id = st.selectbox("Faena", faenas["id"].tolist(), format_func=lambda x: f"{x} - {faenas[faenas['id']==x].iloc[0]['nombre']}")
    pend = compute_pendientes(int(faena_id))

    col1, col2 = st.columns(2)
    with col1:
        st.write("**Pendientes FAENA**")
        st.write(pend["FAENA"] if pend["FAENA"] else ["(sin pendientes)"])
    with col2:
        st.write("**Pendientes por TRABAJADOR**")
        if not pend["TRABAJADORES"]:
            st.write("(sin trabajadores asignados)")
        else:
            for k, v in pend["TRABAJADORES"].items():
                st.write(f"- {k}: {len(v)}")

    st.divider()
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
elif page == "Checklists":
    page_checklists()
elif page == "Tipos de documento":
    page_doc_types()
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
