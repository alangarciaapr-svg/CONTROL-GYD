import os
import re
import io
import zipfile
import hashlib
import base64
import sqlite3

# Postgres (Supabase)
try:
    import psycopg
except Exception:
    psycopg = None

try:
    from psycopg_pool import ConnectionPool
except Exception:
    ConnectionPool = None

import shutil
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import requests
from urllib.parse import quote
import json
import secrets
import unicodedata

# ----------------------------
# Config
# ----------------------------
LOCAL_BRAND_LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "branding", "segav_logo.png")
LOCAL_LOGIN_HERO_PATH = os.path.join(os.path.dirname(__file__), "assets", "branding", "login_hero_segav.svg")
LOCAL_LOGIN_PANEL_APPROVED_PATH = os.path.join(os.path.dirname(__file__), "assets", "branding", "login_right_approved.png")
if os.path.exists(LOCAL_BRAND_LOGO_PATH):
    st.set_page_config(page_title="SEGAV ERP", page_icon=LOCAL_BRAND_LOGO_PATH, layout="wide")
else:
    st.set_page_config(page_title="SEGAV ERP", layout="wide")

APP_NAME = "SEGAV ERP"
DB_PATH = "app.db"
UPLOAD_ROOT = "uploads"  # En Streamlit Community Cloud: filesystem NO es persistente garantizado entre reboots.
MAX_UPLOAD_FILE_BYTES = int(1.5 * 1024 * 1024)
UPLOAD_HELP_TEXT = (
    "Máximo por archivo: 1,5 MB. Si el archivo supera ese tamaño, la app intentará comprimirlo automáticamente. "
    "Si aun así excede el límite, redúcelo antes de subirlo. Sugerencia: puedes comprimirlo en iLovePDF."
)


# Fingerprints/cache helpers
def _fingerprint(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:12]

PG_DSN_FINGERPRINT = "none"
# ----------------------------
# Database backend selector (SQLite local vs Supabase Postgres)
# ----------------------------

def _get_cfg(name: str, default=None):
    v = os.environ.get(name)
    if v is not None and str(v).strip() != "":
        return v
    try:
        if name in st.secrets:
            return st.secrets.get(name)
    except Exception as exc:
        _record_soft_error("_get_cfg", exc)
    return default

def _normalize_pg_dsn(dsn: str) -> str:
    dsn = (dsn or "").strip().strip("'").strip('\"')
    if not dsn:
        return dsn
    dsn = dsn.replace("\n", "").replace("\r", "")
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    if "sslmode=" not in dsn:
        joiner = "&" if "?" in dsn else "?"
        dsn = dsn + f"{joiner}sslmode=require"
    if "connect_timeout=" not in dsn:
        joiner = "&" if "?" in dsn else "?"
        dsn = dsn + f"{joiner}connect_timeout=10"
    return dsn

def _build_pg_dsn_from_parts() -> str:
    host = str(_get_cfg("SUPABASE_DB_HOST", _get_cfg("PGHOST", "")) or "").strip().strip("'").strip('\"')
    port = str(_get_cfg("SUPABASE_DB_PORT", _get_cfg("PGPORT", "5432")) or "5432").strip()
    dbname = str(_get_cfg("SUPABASE_DB_NAME", _get_cfg("PGDATABASE", "postgres")) or "postgres").strip().strip("'").strip('\"')
    user = str(_get_cfg("SUPABASE_DB_USER", _get_cfg("PGUSER", "")) or "").strip().strip("'").strip('\"')
    password = str(_get_cfg("SUPABASE_DB_PASSWORD", _get_cfg("PGPASSWORD", "")) or "").strip().strip("'").strip('\"')
    if not (host and user and password):
        return ""
    parts = [
        f"host={host}",
        f"port={port or '5432'}",
        f"dbname={dbname or 'postgres'}",
        f"user={user}",
        f"password={password}",
        "sslmode=require",
        "connect_timeout=10",
    ]
    return " ".join(parts)

raw_pg_dsn = _get_cfg("SUPABASE_DB_URL", _get_cfg("PG_DSN", ""))
PG_DSN = _normalize_pg_dsn(raw_pg_dsn) or _build_pg_dsn_from_parts()
PG_DSN_FINGERPRINT = _fingerprint(PG_DSN) if PG_DSN else "none"
DB_BACKEND = "postgres" if (PG_DSN and psycopg is not None) else "sqlite"

@st.cache_resource(show_spinner=False)
def get_http_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

@st.cache_data(ttl=21600, show_spinner=False)
def get_brand_logo_bytes(url: str):
    if not url:
        return None
    try:
        resp = get_http_session().get(url, timeout=8)
        if resp.status_code == 200:
            return resp.content
    except Exception:
        return None
    return None

def storage_safe_segment(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    raw = raw.split("/")[-1]
    raw = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    if "." in raw:
        stem, ext = raw.rsplit(".", 1)
        ext = "." + re.sub(r"[^A-Za-z0-9]+", "", ext)[:12]
    else:
        stem, ext = raw, ""
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-") or "archivo"
    stem = stem[:120]
    return f"{stem}{ext}"

def _storage_object_path(folder_parts, file_name: str) -> str:
    safe_parts = []
    for part in (folder_parts or []):
        txt = str(part or "").strip().replace("\\", "/")
        for chunk in [c for c in txt.split("/") if c]:
            safe_parts.append(storage_safe_segment(chunk))
    safe_parts.append(storage_safe_segment(file_name))
    return "/".join(safe_parts)

def _cacheable_params(params):
    if params is None:
        return tuple()
    if isinstance(params, dict):
        return tuple(sorted((str(k), _cacheable_params(v)) for k, v in params.items()))
    if isinstance(params, (list, tuple, set)):
        return tuple(_cacheable_params(x) for x in params)
    if isinstance(params, (str, int, float, bool, bytes, type(None))):
        return params
    return str(params)


def clear_app_caches():
    try:
        _cached_fetch_df.clear()
    except Exception as exc:
        _record_soft_error("clear_app_caches", exc)

@st.cache_data(ttl=20, show_spinner=False)
def _cached_fetch_df(db_backend: str, dsn_fingerprint: str, q: str, params_cache):
    params = tuple(params_cache) if isinstance(params_cache, tuple) else params_cache
    if db_backend == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            return pd.read_sql_query(q2, c, params=params)
    with conn() as c:
        return pd.read_sql_query(q, c, params=params)

def _is_select_query(q: str) -> bool:
    txt = re.sub(r"/\*.*?\*/", " ", q or "", flags=re.S)
    txt = re.sub(r"--.*?$", " ", txt, flags=re.M).strip().lower()
    return txt.startswith("select") or txt.startswith("with")

@st.cache_resource(show_spinner=False)
def _bootstrap_once(db_backend: str, dsn_fingerprint: str):
    ensure_dirs()
    init_db()
    ensure_segav_erp_tables()
    ensure_segav_erp_seed_data()
    try:
        backfill_multiempresa_cliente_key()
    except Exception:
        pass
    try:
        ensure_user_client_access_table()
    except Exception:
        pass
    return True

def bootstrap_app_or_stop():
    """Inicializa la app. Si falla algo crítico, muestra error y detiene Streamlit."""
    try:
        _bootstrap_once(DB_BACKEND, PG_DSN_FINGERPRINT)
    except Exception as _boot_exc:
        st.error("❌ No se pudo iniciar SEGAV ERP. Revisa la conexión a base de datos.")
        st.code(str(_boot_exc))
        st.markdown("""
**Posibles causas:**
- Falta `SUPABASE_DB_URL` (o `PG_DSN`) en Secrets / ENV.
- Credenciales incorrectas o caducadas.
- Si usas SQLite local, verifica que el directorio de datos tenga permisos de escritura.
        """)
        st.stop()

def _qmark_to_pct(sql: str) -> str:
    # Convert SQLite '?' placeholders to psycopg '%s' (only outside single quotes)
    if "?" not in sql:
        return sql
    parts = sql.split("'")
    for i in range(0, len(parts), 2):
        parts[i] = parts[i].replace("?", "%s")
    return "'".join(parts)

# ----------------------------
# Supabase Storage (documentos online)
# ----------------------------
STORAGE_URL = (_get_cfg("SUPABASE_URL", "") or "").rstrip("/")
STORAGE_BUCKET = str(_get_cfg("SUPABASE_STORAGE_BUCKET", "docs") or "docs")
STORAGE_SERVICE_KEY = str(_get_cfg("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
STORAGE_ANON_KEY = str(_get_cfg("SUPABASE_ANON_KEY", "") or "").strip()
STORAGE_TIMEOUT = 30

def _is_jwt(token: str) -> bool:
    t = (token or "").strip()
    return (t.startswith("eyJ") and t.count(".") >= 2 and " " not in t)

def _is_secret_key(token: str) -> bool:
    t = (token or "").strip()
    return t.startswith("sb_secret_") or t.startswith("sb_publishable_")

def _is_publishable_key(token: str) -> bool:
    t = (token or "").strip()
    return t.startswith("sb_publishable_")

def storage_enabled() -> bool:
    return bool(STORAGE_URL and STORAGE_BUCKET and (STORAGE_SERVICE_KEY or STORAGE_ANON_KEY))

def storage_admin_enabled() -> bool:
    key = (STORAGE_SERVICE_KEY or "").strip()
    return bool(STORAGE_URL and STORAGE_BUCKET and key and not _is_publishable_key(key))

def _encode_storage_path(op: str) -> str:
    # Encode each segment to avoid errores por espacios/acentos/#/etc.
    op = (op or "").lstrip("/")
    return "/".join(quote(seg, safe="-_.~") for seg in op.split("/") if seg != "")

def _storage_headers(content_type: str | None = None, upsert: bool = False, for_multipart: bool = False, require_admin: bool = False):
    if require_admin:
        if not storage_admin_enabled():
            raise RuntimeError(
                "Storage administrativo no configurado. Para subir o eliminar archivos debes usar SUPABASE_URL, "
                "SUPABASE_SERVICE_ROLE_KEY (secret/service key real) y SUPABASE_STORAGE_BUCKET."
            )
        key = (STORAGE_SERVICE_KEY or "").strip()
    else:
        if not storage_enabled():
            raise RuntimeError("Storage no configurado. Configura SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY y SUPABASE_STORAGE_BUCKET en Secrets.")
        raw_service = (STORAGE_SERVICE_KEY or "").strip()
        raw_anon = (STORAGE_ANON_KEY or "").strip()
        key = raw_service or raw_anon
    h = {
        "Accept": "application/json",
    }
    if _is_jwt(key):
        h["Authorization"] = f"Bearer {key}"
        h["apikey"] = key
    else:
        # Las secret/publishable keys modernas van en apikey y no como Bearer JWT.
        h["apikey"] = key
    if content_type and not for_multipart:
        h["Content-Type"] = content_type
    if upsert:
        h["x-upsert"] = "true"
    return h

def _storage_set_last_error(resp=None, url: str | None = None, method: str | None = None, exc: Exception | None = None):
    try:
        payload = {}
        if resp is not None:
            payload.update({
                "status": int(getattr(resp, "status_code", 0) or 0),
                "body": (getattr(resp, "text", "") or "")[:1000],
            })
        if url:
            payload["url"] = str(url)[:250]
        if method:
            payload["method"] = method
        if exc is not None:
            payload["exception"] = str(exc)[:300]
        st.session_state["storage_last_error"] = payload
    except Exception:
        pass

def _storage_clear_last_error():
    try:
        st.session_state.pop("storage_last_error", None)
    except Exception:
        pass

def _storage_error_summary(resp=None):
    if resp is None:
        return ""
    try:
        body = resp.json()
        if isinstance(body, dict):
            code = str(body.get("code") or "").strip()
            msg = str(body.get("message") or body.get("error") or "").strip()
            if code and msg:
                return f"{code}: {msg}"
            if msg:
                return msg
    except Exception:
        pass
    return (getattr(resp, "text", "") or "").strip()[:300]

def _storage_should_try_put(resp) -> bool:
    if resp is None:
        return False
    if int(getattr(resp, "status_code", 0) or 0) in (400, 409):
        body = ((getattr(resp, "text", "") or "") + " " + str(getattr(resp, "reason", "") or "")).lower()
        markers = [
            "already exists",
            "asset already exists",
            "duplicate",
            "conflict",
            "exists",
        ]
        return any(m in body for m in markers)
    return False

def storage_upload(object_path: str, data: bytes, content_type: str = "application/octet-stream", upsert: bool = True):
    op = _encode_storage_path(object_path)
    if not op:
        raise RuntimeError("Ruta de Storage inválida.")
    url = f"{STORAGE_URL}/storage/v1/object/{STORAGE_BUCKET}/{op}"

    attempts = []
    http = get_http_session()

    # 1) POST multipart/form-data: Supabase lo documenta como el flujo estándar para subidas pequeñas.
    try:
        resp = http.post(
            url,
            headers=_storage_headers(upsert=upsert, for_multipart=True, require_admin=True),
            files={"file": (os.path.basename(object_path) or "archivo.bin", data, content_type)},
            timeout=STORAGE_TIMEOUT,
        )
        attempts.append(("POST-multipart", resp))
        if resp.status_code in (200, 201):
            _storage_clear_last_error()
            return True
    except Exception as e:
        _storage_set_last_error(url=url, method="POST-multipart", exc=e)
        attempts.append(("POST-multipart", e))

    # 2) Fallback POST binario.
    try:
        resp = http.post(
            url,
            headers=_storage_headers(content_type=content_type, upsert=upsert, require_admin=True),
            data=data,
            timeout=STORAGE_TIMEOUT,
        )
        attempts.append(("POST-binary", resp))
        if resp.status_code in (200, 201):
            _storage_clear_last_error()
            return True
    except Exception as e:
        _storage_set_last_error(url=url, method="POST-binary", exc=e)
        attempts.append(("POST-binary", e))

    # 3) Fallback PUT para reemplazo/upsert.
    try:
        should_try = bool(upsert)
        for _name, item in attempts:
            if hasattr(item, "status_code") and _storage_should_try_put(item):
                should_try = True
                break
        if should_try:
            resp = http.put(
                url,
                headers=_storage_headers(content_type=content_type, upsert=upsert, require_admin=True),
                data=data,
                timeout=STORAGE_TIMEOUT,
            )
            attempts.append(("PUT-binary", resp))
            if resp.status_code in (200, 201):
                _storage_clear_last_error()
                return True
    except Exception as e:
        _storage_set_last_error(url=url, method="PUT-binary", exc=e)
        attempts.append(("PUT-binary", e))

    last_resp = next((item for _name, item in reversed(attempts) if hasattr(item, "status_code")), None)
    if last_resp is not None:
        _storage_set_last_error(last_resp, url=url, method="storage_upload")
        raise RuntimeError(f"Storage upload failed (HTTP {last_resp.status_code}): {_storage_error_summary(last_resp)}")

    last_exc = next((item for _name, item in reversed(attempts) if isinstance(item, Exception)), None)
    _storage_set_last_error(url=url, method="storage_upload", exc=last_exc)
    raise RuntimeError(f"Storage upload failed: {last_exc}")

def storage_download(object_path: str) -> bytes:
    op = _encode_storage_path(object_path)
    urls = [
        f"{STORAGE_URL}/storage/v1/object/authenticated/{STORAGE_BUCKET}/{op}",
        f"{STORAGE_URL}/storage/v1/object/{STORAGE_BUCKET}/{op}",
    ]
    last_resp = None
    last_exc = None
    for idx, url in enumerate(urls, start=1):
        try:
            resp = get_http_session().get(url, headers=_storage_headers(), timeout=STORAGE_TIMEOUT)
        except Exception as e:
            last_exc = e
            _storage_set_last_error(url=url, method="storage_download", exc=e)
            continue
        if resp.status_code == 200:
            _storage_clear_last_error()
            return resp.content
        if resp.status_code == 404:
            last_resp = resp
            continue
        # Si el endpoint authenticated falla por bucket público o gateway, intenta el otro antes de abortar.
        last_resp = resp
    if last_resp is not None and last_resp.status_code == 404:
        raise FileNotFoundError("Archivo no encontrado en Storage.")
    if last_resp is not None:
        _storage_set_last_error(last_resp, url=urls[-1], method="storage_download")
        raise RuntimeError(
            f"Storage download failed (HTTP {last_resp.status_code}): {_storage_error_summary(last_resp)}"
        )
    if last_exc is not None:
        raise RuntimeError(f"Storage download failed: {last_exc}")
    raise RuntimeError("Storage download failed: sin respuesta del servidor.")


def storage_delete(object_path: str):
    op = _encode_storage_path(object_path)
    if not op:
        return False
    url = f"{STORAGE_URL}/storage/v1/object/{STORAGE_BUCKET}/{op}"
    try:
        resp = get_http_session().delete(url, headers=_storage_headers(require_admin=True), timeout=STORAGE_TIMEOUT)
    except Exception as e:
        _storage_set_last_error(url=url, method="storage_delete", exc=e)
        raise RuntimeError(f"Storage delete failed: {e}")

    if resp.status_code in (200, 204, 404):
        _storage_clear_last_error()
        return True

    _storage_set_last_error(resp, url=url, method="storage_delete")
    raise RuntimeError(f"Storage delete failed (HTTP {resp.status_code}): {_storage_error_summary(resp)}")

def human_file_size(num_bytes: int) -> str:
    size = float(max(int(num_bytes or 0), 0))
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} GB"


def render_upload_help():
    st.caption("💡 " + UPLOAD_HELP_TEXT)


def periodo_ym(anio: int | None, mes: int | None) -> str:
    try:
        return f"{int(anio):04d}-{int(mes):02d}"
    except Exception:
        return "SIN_PERIODO"


def periodo_label(anio: int | None, mes: int | None) -> str:
    try:
        anio_i = int(anio)
        mes_i = int(mes)
        return f"{anio_i:04d}-{mes_i:02d} · {MESES_ES.get(mes_i, str(mes_i))}"
    except Exception:
        return "SIN PERÍODO"


def periodo_folder_segment(anio: int | None, mes: int | None) -> str:
    return safe_name(periodo_label(anio, mes).replace(" · ", "_"))


def pendientes_empresa_faena_periodo(faena_id: int, anio: int, mes: int):
    df = fetch_df(
        "SELECT DISTINCT doc_tipo FROM faena_empresa_documentos WHERE faena_id=? AND COALESCE(periodo_anio,0)=? AND COALESCE(periodo_mes,0)=?",
        (int(faena_id), int(anio), int(mes)),
    )
    present = set(df["doc_tipo"].astype(str).tolist()) if not df.empty else set()
    return [d for d in get_empresa_monthly_doc_types() if d not in present]


def _zip_single_file_bytes(file_name: str, file_bytes: bytes) -> tuple[str, bytes]:
    zip_name = str(file_name or "archivo").strip() or "archivo"
    if not zip_name.lower().endswith('.zip'):
        zip_name = f"{zip_name}.zip"
    inner_name = str(file_name or "archivo").strip() or "archivo"
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.writestr(inner_name, file_bytes)
    return zip_name, mem.getvalue()


def prepare_upload_payload(file_name: str, file_bytes: bytes, content_type: str | None = None, size_limit: int = MAX_UPLOAD_FILE_BYTES):
    raw_name = str(file_name or "archivo").strip() or "archivo"
    raw_bytes = bytes(file_bytes or b"")
    raw_type = content_type or "application/octet-stream"
    raw_size = len(raw_bytes)
    payload = {
        "file_name": raw_name,
        "file_bytes": raw_bytes,
        "content_type": raw_type,
        "original_name": raw_name,
        "original_size": raw_size,
        "stored_size": raw_size,
        "compressed": False,
        "compression_note": None,
    }
    if raw_size <= int(size_limit):
        return payload

    zip_name, zip_bytes = _zip_single_file_bytes(raw_name, raw_bytes)
    zip_size = len(zip_bytes)
    if zip_size <= int(size_limit):
        payload.update({
            "file_name": zip_name,
            "file_bytes": zip_bytes,
            "content_type": "application/zip",
            "stored_size": zip_size,
            "compressed": True,
            "compression_note": (
                f"El archivo superaba 1,5 MB y se guardará comprimido como {zip_name} "
                f"({human_file_size(raw_size)} → {human_file_size(zip_size)})."
            ),
        })
        return payload

    st.error(
        f"El límite de carga por archivo es de 1,5 MB. El archivo pesa {human_file_size(raw_size)} y "
        f"aun comprimido queda en {human_file_size(zip_size)}. Reduce el tamaño antes de cargarlo. "
        f"Sugerencia: puedes comprimirlo en iLovePDF."
    )
    st.stop()


def save_file_online(folder_parts, file_name: str, file_bytes: bytes, content_type: str = "application/octet-stream"):
    # Guarda local (compatibilidad) + intenta subir a Storage (online).
    scoped_folder_parts = tenantize_folder_parts(folder_parts)
    local_path = save_file(scoped_folder_parts, file_name, file_bytes)
    object_path = _storage_object_path(scoped_folder_parts, file_name)

    bucket = STORAGE_BUCKET if storage_admin_enabled() else None
    if storage_admin_enabled():
        try:
            storage_upload(object_path, file_bytes, content_type=content_type, upsert=True)
        except Exception:
            # No romper el flujo: deja el archivo local, pero informa.
            bucket = None
            object_path = None
            try:
                last = st.session_state.get("storage_last_error", {})
                sc = last.get("status")
                extra = f" (HTTP {sc})" if sc else ""
                detail = str(last.get("body") or last.get("exception") or "").strip()[:220]
                hint = f" Detalle: {detail}." if detail else ""
                st.warning(
                    "No se pudo subir el archivo a Supabase Storage" + extra + ". "
                    "El documento quedó solo en almacenamiento local (puede perderse si Streamlit reinicia)." + hint + " "
                    "Revisa tus Secrets: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY y SUPABASE_STORAGE_BUCKET. "
                    "En Backup/Restore verás un diagnóstico, o revisa Manage app → Logs."
                )
            except Exception:
                pass
    else:
        # Storage administrativo no configurado: queda local
        bucket = None
        object_path = None
        if storage_enabled():
            try:
                st.warning(
                    "Storage está configurado solo en modo lectura o con una key sin privilegios de escritura. "
                    "El documento quedó solo en almacenamiento local. Revisa SUPABASE_SERVICE_ROLE_KEY."
                )
            except Exception:
                pass

    return local_path, bucket, object_path

def load_file_anywhere(file_path: str | None, bucket: str | None, object_path: str | None) -> bytes:
    if object_path and storage_enabled():
        try:
            return storage_download(object_path)
        except Exception:
            # Fallback local para no romper la app si Storage falla temporalmente.
            pass
    if file_path and os.path.exists(str(file_path)):
        with open(str(file_path), "rb") as fp:
            return fp.read()
    raise FileNotFoundError("Archivo no disponible (ni Storage ni disco local).")



ESTADOS_FAENA = ["ACTIVA", "TERMINADA"]
DOC_TIPO_LABELS = {
    "CONTRATO_TRABAJO": "CONTRATO",
    "REGISTRO_EPP": "REGISTRO DE EPP",
    "ENTREGA_RIOHS": "REGISTRO ENTREGA DE RIOHS",
    "IRL": "IRL",
    "LICENCIA_CONDUCIR": "LICENCIA DE CONDUCIR",
    "CEDULA_IDENTIDAD": "CÉDULA DE IDENTIDAD",
    "CERTIFICACION_CORMA": "CERTIFICACIÓN CORMA",
    "LIQUIDACIONES_SUELDO_MES": "LIQUIDACIONES DE SUELDO",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30": "CERTIFICADO DE ANTECEDENTES LABORALES F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1": "CERTIFICADO DE CUMPLIMIENTOS LABORALES Y PREVISIONALES F30-1",
    "CERTIFICADO_ACCIDENTABILIDAD": "CERTIFICADO DE ACCIDENTABILIDAD",
    "CERTIFICADO_CUMPLIMIENTO_LABORAL": "CERTIFICADO DE CUMPLIMIENTO LABORAL",
    "CERTIFICADO_ADHESION_A_MUTUALIDAD": "CERTIFICADO DE ADHESIÓN A MUTUALIDAD",
}
DOC_OBLIGATORIOS = [
    "CONTRATO_TRABAJO",
    "REGISTRO_EPP",
    "ENTREGA_RIOHS",
    "IRL",
]
DOCS_OPERARIO_MAQUINARIA_FORESTAL = [
    "LICENCIA_CONDUCIR",
    "CEDULA_IDENTIDAD",
]
DOCS_MOTOSIERRISTA = [
    "CERTIFICACION_CORMA",
    "CEDULA_IDENTIDAD",
]
CARGO_DOCS_RULES = {
    "OPERADOR DE MAQUINARIA FORESTAL": DOC_OBLIGATORIOS + DOCS_OPERARIO_MAQUINARIA_FORESTAL,
    "MOTOSIERRISTA": DOC_OBLIGATORIOS + DOCS_MOTOSIERRISTA,
    "ESTROBERO": list(DOC_OBLIGATORIOS),
    "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
    "MECANICO": list(DOC_OBLIGATORIOS),
    "ASERRADERO": list(DOC_OBLIGATORIOS),
    "PLANTA": list(DOC_OBLIGATORIOS),
}
CARGO_DOCS_ORDER = [
    "OPERADOR DE MAQUINARIA FORESTAL",
    "MOTOSIERRISTA",
    "ESTROBERO",
    "ADMINISTRATIVO",
    "MECANICO",
    "ASERRADERO",
    "PLANTA",
]
DOC_EMPRESA_SUGERIDOS = [
    "LIQUIDACIONES_SUELDO_MES",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1",
    "CERTIFICADO_ACCIDENTABILIDAD",
]
DOC_EMPRESA_REQUERIDOS = [
    "LIQUIDACIONES_SUELDO_MES",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1",
    "CERTIFICADO_ACCIDENTABILIDAD",
]
DOC_EMPRESA_MENSUALES = [
    "LIQUIDACIONES_SUELDO_MES",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1",
    "CERTIFICADO_ACCIDENTABILIDAD",
]

ERP_CLIENT_PARAM_DEFAULTS = {
    "usa_multi_faena": "SI",
    "usa_docs_empresa_mensuales": "SI",
    "usa_miper": "SI",
    "usa_ds594": "SI",
    "usa_ley_16744": "SI",
    "usa_capacitaciones_odi": "SI",
    "usa_auditoria": "SI",
    "branding_cliente": "ESTANDAR",
}

ERP_TEMPLATE_PRESETS = {
    "GENERAL": {
        "label": "General",
        "vertical": "General",
        "description": "Base comercial multipropósito para servicios, administración y operación documental.",
        "cargos": ["OPERARIO", "SUPERVISOR", "ADMINISTRATIVO", "MECANICO", "BODEGUERO", "PLANTA"],
        "cargo_rules": {
            "OPERARIO": list(DOC_OBLIGATORIOS),
            "SUPERVISOR": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "MECANICO": list(DOC_OBLIGATORIOS),
            "BODEGUERO": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "FORESTAL": {
        "label": "Forestal",
        "vertical": "Forestal",
        "description": "Plantilla base para faenas forestales, con cargos y documentos obligatorios por rol.",
        "cargos": list(CARGO_DOCS_ORDER) + ["SUPERVISOR DE FAENA"],
        "cargo_rules": {
            **{k: list(dict.fromkeys(v)) for k, v in CARGO_DOCS_RULES.items()},
            "SUPERVISOR DE FAENA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "CONSTRUCCION": {
        "label": "Construcción",
        "vertical": "Construcción",
        "description": "Plantilla para contratistas y subcontratistas con control de cuadrillas, conducción y documentación mensual.",
        "cargos": ["OPERARIO", "CAPATAZ", "CONDUCTOR", "MECANICO", "ADMINISTRATIVO", "BODEGUERO", "PLANTA"],
        "cargo_rules": {
            "OPERARIO": list(DOC_OBLIGATORIOS),
            "CAPATAZ": list(DOC_OBLIGATORIOS),
            "CONDUCTOR": list(dict.fromkeys(DOC_OBLIGATORIOS + ["LICENCIA_CONDUCIR", "CEDULA_IDENTIDAD"])),
            "MECANICO": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "BODEGUERO": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "TRANSPORTE": {
        "label": "Transporte",
        "vertical": "Transporte",
        "description": "Plantilla para operación con conductores, mantención y trazabilidad documental por servicio.",
        "cargos": ["CONDUCTOR", "PEONETA", "MECANICO", "ADMINISTRATIVO", "PLANTA"],
        "cargo_rules": {
            "CONDUCTOR": list(dict.fromkeys(DOC_OBLIGATORIOS + ["LICENCIA_CONDUCIR", "CEDULA_IDENTIDAD"])),
            "PEONETA": list(DOC_OBLIGATORIOS),
            "MECANICO": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "SERVICIOS": {
        "label": "Servicios",
        "vertical": "Servicios",
        "description": "Plantilla para empresas de servicios generales con configuración ligera y adaptable.",
        "cargos": ["TECNICO", "SUPERVISOR", "ADMINISTRATIVO", "AUXILIAR", "PLANTA"],
        "cargo_rules": {
            "TECNICO": list(DOC_OBLIGATORIOS),
            "SUPERVISOR": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "AUXILIAR": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
}
MESES_ES = {
    1: "ENERO",
    2: "FEBRERO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE",
}
REQ_DOCS_N = len(DOC_OBLIGATORIOS)

SGSST_NORMAS = ["DS 44", "Ley 16.744", "DS 594", "Ley Karin", "Interno"]
SGSST_ESTADOS = ["PENDIENTE", "EN CURSO", "CERRADO", "NO APLICA"]
SGSST_RESULTADOS = ["CUMPLE", "NO CUMPLE", "OBSERVACIÓN"]
SGSST_TIPOS_EVENTO = ["INCIDENTE", "ACCIDENTE DEL TRABAJO", "ACCIDENTE DE TRAYECTO", "ENFERMEDAD PROFESIONAL", "HALLAZGO"]
SGSST_GRAVEDADES = ["BAJA", "MEDIA", "ALTA", "GRAVE/FATAL"]
SGSST_TIPOS_CAP = ["ODI", "INDUCCIÓN", "CAPACITACIÓN", "CHARLA DE SEGURIDAD", "SIMULACRO"]
SGSST_MATRIZ_BASE = [
    {"norma": "DS 44", "articulo": "Sistema de gestión", "tema": "Implementación SGSST", "obligacion": "Mantener un sistema de gestión preventivo con instrumentos y seguimiento.", "aplica_a": "Empresa", "periodicidad": "Permanente", "responsable": "Gerencia / Prevención", "evidencia": "Manual SGSST, registros y seguimiento", "estado": "EN CURSO"},
    {"norma": "DS 44", "articulo": "MIPER", "tema": "Matriz de riesgos", "obligacion": "Mantener identificación de peligros y evaluación de riesgos por faena, tarea y cargo.", "aplica_a": "Faenas / Cargos", "periodicidad": "Anual o por cambio", "responsable": "Prevención", "evidencia": "MIPER vigente", "estado": "PENDIENTE"},
    {"norma": "DS 44", "articulo": "Programa preventivo", "tema": "Programa anual", "obligacion": "Planificar actividades preventivas con responsables, plazos y evidencias.", "aplica_a": "Empresa / Faenas", "periodicidad": "Anual", "responsable": "Gerencia / Prevención", "evidencia": "Programa anual y cierres", "estado": "PENDIENTE"},
    {"norma": "DS 44", "articulo": "Información y capacitación", "tema": "ODI y formación", "obligacion": "Entregar información de riesgos y capacitación preventiva a trabajadores.", "aplica_a": "Trabajadores", "periodicidad": "Ingreso y periódica", "responsable": "Jefaturas / Prevención", "evidencia": "Registros ODI y capacitaciones", "estado": "EN CURSO"},
    {"norma": "DS 44", "articulo": "Emergencias", "tema": "Plan de emergencia", "obligacion": "Disponer de plan de emergencias, simulacros y responsables.", "aplica_a": "Empresa / Faenas", "periodicidad": "Anual", "responsable": "Gerencia / Faenas", "evidencia": "Plan y registros de simulacro", "estado": "PENDIENTE"},
    {"norma": "Ley 16.744", "articulo": "Seguro", "tema": "Organismo administrador", "obligacion": "Mantener afiliación y coordinación preventiva con organismo administrador.", "aplica_a": "Empresa", "periodicidad": "Permanente", "responsable": "Gerencia", "evidencia": "Certificado de adhesión", "estado": "EN CURSO"},
    {"norma": "Ley 16.744", "articulo": "Accidentes y enfermedades", "tema": "Investigación", "obligacion": "Registrar, investigar y gestionar medidas correctivas de incidentes y accidentes.", "aplica_a": "Empresa / Faenas", "periodicidad": "Cada evento", "responsable": "Prevención / Jefatura", "evidencia": "Investigaciones y cierres", "estado": "PENDIENTE"},
    {"norma": "Ley 16.744", "articulo": "Participación", "tema": "CPHS / Monitoreo dotación", "obligacion": "Monitorear obligación de CPHS según dotación y mantener registros si aplica.", "aplica_a": "Empresa", "periodicidad": "Mensual", "responsable": "Gerencia", "evidencia": "Actas / control de dotación", "estado": "EN CURSO"},
    {"norma": "DS 594", "articulo": "Condiciones sanitarias", "tema": "Agua y servicios higiénicos", "obligacion": "Verificar agua potable, higiene, orden y aseo en lugares de trabajo.", "aplica_a": "Faenas / Planta", "periodicidad": "Mensual", "responsable": "Supervisor / Faena", "evidencia": "Checklist DS 594", "estado": "PENDIENTE"},
    {"norma": "DS 594", "articulo": "Condiciones ambientales", "tema": "Señalización, extintores y ambiente", "obligacion": "Controlar señalización, extintores, vías de circulación y condiciones ambientales.", "aplica_a": "Faenas / Planta", "periodicidad": "Mensual", "responsable": "Supervisor / Mantención", "evidencia": "Inspecciones y acciones", "estado": "PENDIENTE"},
]

ASSIGNACION_INSERT_SQL = """
INSERT INTO asignaciones(cliente_key, faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT DO NOTHING
"""

# ----------------------------
# Helpers
# ----------------------------
def doc_tipo_label(value: str) -> str:
    return DOC_TIPO_LABELS.get(str(value), str(value))


def doc_tipo_join(values) -> str:
    return ", ".join(doc_tipo_label(v) for v in values)


def normalize_text(value) -> str:
    s = str(value or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.strip().lower()


def make_erp_key(value: str, prefix: str = "") -> str:
    base = normalize_text(value)
    base = re.sub(r"[^a-z0-9]+", "_", base).strip("_") or "item"
    return f"{prefix}{base}" if prefix else base


def canonical_cargo_label(cargo: str | None) -> str:
    cargo_txt = str(cargo or "").strip()
    cargo_n = normalize_text(cargo_txt)
    labels = segav_cargo_labels(active_only=False)
    if not cargo_n:
        return "PLANTA" if "PLANTA" in labels else (labels[0] if labels else "PLANTA")
    for label in labels:
        if cargo_txt == label:
            return label
    alias_patterns = [
        ("motosierr", "MOTOSIERRISTA"),
        ("maquinaria forestal", "OPERADOR DE MAQUINARIA FORESTAL"),
        ("estrobero", "ESTROBERO"),
        ("administr", "ADMINISTRATIVO"),
        ("mecan", "MECANICO"),
        ("aserradero", "ASERRADERO"),
        ("planta", "PLANTA"),
    ]
    for patt, canon in alias_patterns:
        if patt in cargo_n and canon in labels:
            return canon
    for label in labels:
        if normalize_text(label) == cargo_n:
            return label
    return cargo_txt.upper()


def worker_required_docs(cargo: str | None) -> list[str]:
    cargo_key = canonical_cargo_label(cargo)
    docs = segav_cargo_rules().get(cargo_key, DOC_OBLIGATORIOS)
    return list(dict.fromkeys(docs))


def worker_required_docs_by_id(trabajador_id: int) -> list[str]:
    row = fetch_row("SELECT cargo FROM trabajadores WHERE id=?", (int(trabajador_id),))
    cargo = row[0] if row else None
    return worker_required_docs(cargo)


def worker_required_docs_for_record(rec) -> list[str]:
    cargo = None
    try:
        if isinstance(rec, dict):
            cargo = rec.get("cargo")
        else:
            cargo = rec["cargo"] if "cargo" in rec else None
    except Exception:
        cargo = None
    return worker_required_docs(cargo)


def cargo_docs_catalog_rows():
    rows = []
    rules = segav_cargo_rules()
    for cargo in segav_cargo_labels(active_only=False):
        rows.append({
            "Cargo": cargo,
            "Documentos obligatorios": doc_tipo_join(rules.get(cargo, DOC_OBLIGATORIOS)),
        })
    return rows


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



def fetch_df_uncached(q: str, params=()):
    """SELECT sin cache para flujos que deben reflejar cambios inmediatamente."""
    if DB_BACKEND == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            return pd.read_sql_query(q2, c, params=params)
    with conn() as c:
        return pd.read_sql_query(q, c, params=params)


def fetch_row(q: str, params=(), fresh: bool = False):
    df = fetch_df_uncached(q, params) if fresh else fetch_df(q, params)
    if df is None or df.empty:
        return None
    return df.iloc[0]


def fetch_value(q: str, params=(), default=None, fresh: bool = False):
    row = fetch_row(q, params=params, fresh=fresh)
    if row is None:
        return default
    try:
        return row.iloc[0]
    except Exception:
        try:
            return row[0]
        except Exception:
            return default


def fetch_assigned_workers(faena_id: int, fresh: bool = True):
    """Devuelve trabajadores asignados a una faena para la empresa activa."""
    tenant_key = current_tenant_key()
    q = '''
        SELECT DISTINCT
               t.id,
               t.rut,
               t.apellidos,
               t.nombres,
               COALESCE(a.cargo_faena,'') AS cargo_faena,
               COALESCE(t.cargo,'') AS cargo
        FROM asignaciones a
        JOIN trabajadores t ON t.id=a.trabajador_id
        WHERE a.faena_id=?
          AND COALESCE(a.cliente_key,'')=?
          AND COALESCE(t.cliente_key,'')=?
          AND COALESCE(NULLIF(TRIM(UPPER(a.estado)), ''), 'ACTIVA') <> 'CERRADA'
        ORDER BY t.apellidos, t.nombres, t.id
    '''
    reader = fetch_df_uncached if fresh else fetch_df
    return reader(q, (int(faena_id), tenant_key, tenant_key))


def get_global_counts():
    """Devuelve conteos básicos filtrados por empresa activa."""
    tenant_key = current_tenant_key()
    try:
        row = fetch_df(
            """
            SELECT
                (SELECT COUNT(*) FROM mandantes WHERE COALESCE(cliente_key,'')=?) AS mandantes,
                (SELECT COUNT(*) FROM contratos_faena WHERE COALESCE(cliente_key,'')=?) AS contratos_faena,
                (SELECT COUNT(*) FROM faenas WHERE COALESCE(cliente_key,'')=?) AS faenas,
                (SELECT COUNT(*) FROM faenas WHERE COALESCE(cliente_key,'')=? AND estado='ACTIVA') AS faenas_activas,
                (SELECT COUNT(*) FROM trabajadores WHERE COALESCE(cliente_key,'')=?) AS trabajadores,
                (SELECT COUNT(*) FROM asignaciones WHERE COALESCE(cliente_key,'')=?) AS asignaciones,
                (SELECT COUNT(*) FROM trabajador_documentos WHERE COALESCE(cliente_key,'')=?) AS docs,
                (SELECT COUNT(*) FROM empresa_documentos WHERE COALESCE(cliente_key,'')=?) AS docs_empresa,
                (SELECT COUNT(*) FROM faena_empresa_documentos WHERE COALESCE(cliente_key,'')=?) AS docs_empresa_faena,
                (SELECT COUNT(*) FROM export_historial WHERE COALESCE(cliente_key,'')=?) AS exports,
                (SELECT COUNT(*) FROM export_historial_mes WHERE COALESCE(cliente_key,'')=?) AS exports_mes
            """,
            (tenant_key, tenant_key, tenant_key, tenant_key, tenant_key, tenant_key, tenant_key, tenant_key, tenant_key, tenant_key, tenant_key),
        )
        if row.empty:
            return {}
        return {k: int(row.iloc[0].get(k, 0) or 0) for k in row.columns}
    except Exception:
        out = {}
        pairs = [
            ("mandantes", "SELECT COUNT(*) AS n FROM mandantes WHERE COALESCE(cliente_key,'')=?"),
            ("contratos_faena", "SELECT COUNT(*) AS n FROM contratos_faena WHERE COALESCE(cliente_key,'')=?"),
            ("faenas", "SELECT COUNT(*) AS n FROM faenas WHERE COALESCE(cliente_key,'')=?"),
            ("faenas_activas", "SELECT COUNT(*) AS n FROM faenas WHERE COALESCE(cliente_key,'')=? AND estado='ACTIVA'"),
            ("trabajadores", "SELECT COUNT(*) AS n FROM trabajadores WHERE COALESCE(cliente_key,'')=?"),
            ("asignaciones", "SELECT COUNT(*) AS n FROM asignaciones WHERE COALESCE(cliente_key,'')=?"),
            ("docs", "SELECT COUNT(*) AS n FROM trabajador_documentos WHERE COALESCE(cliente_key,'')=?"),
            ("docs_empresa", "SELECT COUNT(*) AS n FROM empresa_documentos WHERE COALESCE(cliente_key,'')=?"),
            ("docs_empresa_faena", "SELECT COUNT(*) AS n FROM faena_empresa_documentos WHERE COALESCE(cliente_key,'')=?"),
            ("exports", "SELECT COUNT(*) AS n FROM export_historial WHERE COALESCE(cliente_key,'')=?"),
            ("exports_mes", "SELECT COUNT(*) AS n FROM export_historial_mes WHERE COALESCE(cliente_key,'')=?"),
        ]
        for key, sql in pairs:
            try:
                out[key] = int(fetch_df(sql, (tenant_key,))["n"].iloc[0])
            except Exception:
                out[key] = 0
        return out


def norm_col(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _rut_parts(rut: str):
    raw = str(rut or "").strip().upper()
    raw = re.sub(r"[^0-9K]", "", raw)
    if not raw:
        return "", ""
    if len(raw) == 1:
        return "", raw
    body = raw[:-1].lstrip("0") or "0"
    dv = raw[-1]
    return body, dv


def format_rut_chileno(rut: str) -> str:
    body, dv = _rut_parts(rut)
    if not body and not dv:
        return ""
    if not body:
        return dv
    rev = body[::-1]
    grouped_rev = ".".join(rev[i:i+3] for i in range(0, len(rev), 3))
    return f"{grouped_rev[::-1]}-{dv}"


def clean_rut(rut: str) -> str:
    return format_rut_chileno(rut)


def _format_rut_session_value(key: str):
    st.session_state[key] = format_rut_chileno(st.session_state.get(key, ""))


def rut_input(label: str, *, key: str, value: str = "", placeholder: str = "12.345.678-9", help: str | None = None):
    current_value = st.session_state.get(key, value)
    formatted_value = format_rut_chileno(current_value)
    if st.session_state.get(key) != formatted_value:
        st.session_state[key] = formatted_value
    return st.text_input(
        label,
        key=key,
        placeholder=placeholder,
        help=help,
        on_change=_format_rut_session_value,
        args=(key,),
    )


@st.cache_data(show_spinner=False)
def _reset_trabajador_create_state():
    defaults = {
        "trabajador_create_rut": "",
        "trabajador_create_nombres": "",
        "trabajador_create_apellidos": "",
        "trabajador_create_cargo": "",
        "trabajador_create_cc": "",
        "trabajador_create_email": "",
        "trabajador_create_fc": None,
        "trabajador_create_ve": None,
    }
    for _k, _v in defaults.items():
        st.session_state[_k] = _v


def _apply_pending_trabajador_create_reset():
    if st.session_state.pop("_trabajador_create_reset_pending", False):
        _reset_trabajador_create_state()


def _show_pending_trabajador_create_flash():
    msg = st.session_state.pop("_trabajador_create_flash", None)
    if msg:
        st.success(msg)


def build_trabajadores_template_xlsx() -> bytes:
    ejemplo = pd.DataFrame([
        {
            "RUT": "12.345.678-5",
            "NOMBRE": "Juan Carlos Perez Soto",
            "CARGO": "Operador",
            "CENTRO_COSTO": "FAENA A",
            "EMAIL": "juan.perez@empresa.cl",
            "FECHA DE CONTRATO": "2026-03-30",
            "VIGENCIA_EXAMEN": "2026-12-31",
        }
    ])
    instrucciones = pd.DataFrame(
        {
            "Campo": [
                "RUT",
                "NOMBRE",
                "CARGO",
                "CENTRO_COSTO",
                "EMAIL",
                "FECHA DE CONTRATO",
                "VIGENCIA_EXAMEN",
            ],
            "Obligatorio": ["Sí", "Sí", "No", "No", "No", "No", "No"],
            "Detalle": [
                "RUT chileno. La app lo normaliza al formato XX.XXX.XXX-X.",
                "Nombre completo del trabajador.",
                "Cargo o función.",
                "Centro de costo o faena.",
                "Correo electrónico.",
                "Fecha en formato YYYY-MM-DD o fecha Excel.",
                "Fecha en formato YYYY-MM-DD o fecha Excel.",
            ],
        }
    )
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        ejemplo.to_excel(writer, sheet_name="Trabajadores", index=False)
        instrucciones.to_excel(writer, sheet_name="Instrucciones", index=False)
    return out.getvalue()

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

@st.cache_resource(show_spinner=False)
def get_pg_pool(dsn: str):
    if not dsn or psycopg is None or ConnectionPool is None:
        return None
    try:
        pool = ConnectionPool(
            conninfo=dsn,
            min_size=1,
            max_size=8,
            kwargs={"prepare_threshold": None},
            timeout=10,
        )
        pool.wait(timeout=10)
        return pool
    except Exception:
        return None

def conn():
    # Postgres (Supabase) if configured; otherwise SQLite local.
    if DB_BACKEND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg no está instalado, pero DB_BACKEND=postgres.")
        if not PG_DSN:
            raise RuntimeError("Falta SUPABASE_DB_URL (o PG_DSN) en Secrets/ENV.")
        try:
            pool = get_pg_pool(PG_DSN)
            if pool is not None:
                return pool.connection()
            return psycopg.connect(PG_DSN, prepare_threshold=None)
        except Exception as e:
            msg = str(e).strip() or e.__class__.__name__
            raise RuntimeError(
                "No se pudo conectar a Postgres/Supabase. "
                f"Detalle: {msg}. "
                "Revisa SUPABASE_DB_URL o usa secretos separados SUPABASE_DB_HOST, SUPABASE_DB_PORT, SUPABASE_DB_NAME, SUPABASE_DB_USER y SUPABASE_DB_PASSWORD."
            ) from e
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        c.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    return c

def migrate_add_columns_if_missing(c, table: str, cols_sql: dict):
    if DB_BACKEND == "postgres":
        return
    info = c.execute(f"PRAGMA table_info({table});").fetchall()
    existing = {row[1] for row in info}
    for col, coltype in cols_sql.items():
        if col not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")

def cursor_execute(cur, q: str, params=()):
    if DB_BACKEND == "postgres":
        q = _qmark_to_pct(q).replace("datetime('now')", "now()")
    return cur.execute(q, params)


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
        ensure_multiempresa_columns_postgres()
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
        ensure_multiempresa_columns_sqlite(c)
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
        tenant_key = current_tenant_key()
        if int(fetch_value("SELECT COUNT(*) FROM sgsst_empresa WHERE COALESCE(cliente_key,'')=?", (tenant_key,), default=0) or 0) == 0:
            execute(
                """
                INSERT INTO sgsst_empresa(cliente_key, razon_social, rut, direccion, actividad, organismo_admin, representantes, prevencionista, canal_denuncias, dotacion_total, politica_version, politica_fecha, observaciones, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    tenant_key,
                    segav_erp_value('cliente_actual', 'Empresa demo') if 'segav_erp_value' in globals() else 'Empresa demo',
                    '', '',
                    segav_erp_value('erp_vertical', 'General') if 'segav_erp_value' in globals() else 'General',
                    'Organismo administrador', '', '', '', 0, '1.0', date.today().isoformat(),
                    'Base inicial de SEGAV ERP / SGSST configurable para cualquier empresa.',
                    datetime.now().isoformat(timespec='seconds'), datetime.now().isoformat(timespec='seconds'),
                ),
            )
        existing = fetch_df("SELECT norma, tema, obligacion FROM sgsst_matriz_legal WHERE COALESCE(cliente_key,'')=?", (tenant_key,))
        existing_keys = set()
        if existing is not None and not existing.empty:
            existing_keys = set((str(r[0] or ''), str(r[1] or ''), str(r[2] or '')) for r in existing[["norma", "tema", "obligacion"]].itertuples(index=False, name=None))
        for item in SGSST_MATRIZ_BASE:
            key = (item['norma'], item['tema'], item['obligacion'])
            if key in existing_keys:
                continue
            execute(
                """
                INSERT INTO sgsst_matriz_legal(cliente_key, norma, articulo, tema, obligacion, aplica_a, periodicidad, responsable, evidencia, estado, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (tenant_key, item.get('norma'), item.get('articulo'), item.get('tema'), item.get('obligacion'), item.get('aplica_a'), item.get('periodicidad'), item.get('responsable'), item.get('evidencia'), item.get('estado'), datetime.now().isoformat(timespec='seconds'), datetime.now().isoformat(timespec='seconds')),
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
        'modo_implementacion': 'CONFIGURABLE',
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
        cliente_nombre = segav_erp_value('cliente_actual', razon) or razon
        cliente_key = make_erp_key(cliente_nombre, prefix='cli_')
        execute(
            "INSERT INTO segav_erp_clientes(cliente_key, cliente_nombre, rut, vertical, modo_implementacion, activo, contacto, email, observaciones, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (cliente_key, cliente_nombre, rut, segav_erp_value('erp_vertical', 'General'), segav_erp_value('modo_implementacion', 'CONFIGURABLE'), 1, '', '', 'Cliente inicial sembrado desde la configuración actual.', now, now),
        )
        for param_key, param_value in ERP_CLIENT_PARAM_DEFAULTS.items():
            execute(
                "INSERT INTO segav_erp_parametros_cliente(cliente_key, param_key, param_value, updated_at) VALUES(?,?,?,?)",
                (cliente_key, param_key, str(param_value), now),
            )
        if not segav_erp_value('current_client_key', ''):
            set_segav_erp_config_value('current_client_key', cliente_key)
            set_segav_erp_config_value('cliente_actual', cliente_nombre)

    # asegura cliente actual y parámetros base
    cliente_df = fetch_df("SELECT cliente_key, cliente_nombre FROM segav_erp_clientes WHERE COALESCE(activo,1)=1 ORDER BY cliente_nombre")
    if cliente_df is not None and not cliente_df.empty:
        current_key = segav_erp_value('current_client_key', '')
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


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_erp_config_map(_backend: str, _dsn: str):
    df = fetch_df("SELECT config_key, config_value FROM segav_erp_config ORDER BY config_key")
    if df is None or df.empty:
        return {}
    return {str(r['config_key']): str(r['config_value'] or '') for _, r in df.iterrows()}


def segav_erp_config_map():
    return get_segav_erp_config_map(DB_BACKEND, PG_DSN_FINGERPRINT)


def segav_erp_value(key: str, default: str = "") -> str:
    return str(segav_erp_config_map().get(key, default) or default)


def erp_brand_name() -> str:
    return segav_erp_value('erp_name', APP_NAME)


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_cargos_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT cargo_key, cargo_label, sort_order, activo FROM segav_erp_cargos ORDER BY sort_order, cargo_label")
    return df if df is not None else pd.DataFrame()


def segav_cargos_df():
    return get_segav_cargos_df(DB_BACKEND, PG_DSN_FINGERPRINT)


def segav_cargo_labels(active_only: bool = True) -> list[str]:
    df = segav_cargos_df()
    if df is None or df.empty:
        return list(CARGO_DOCS_ORDER)
    if active_only and 'activo' in df.columns:
        df = df[df['activo'].fillna(1).astype(int) == 1]
    labels = [str(v).strip() for v in df['cargo_label'].tolist() if str(v).strip()]
    return labels or list(CARGO_DOCS_ORDER)


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_cargo_rules(_backend: str, _dsn: str):
    df = fetch_df(
        """
        SELECT c.cargo_label, d.doc_tipo, d.sort_order
          FROM segav_erp_docs_cargo d
          LEFT JOIN segav_erp_cargos c ON c.cargo_key=d.cargo_key
         ORDER BY COALESCE(c.sort_order,9999), COALESCE(d.sort_order,9999), d.doc_tipo
        """
    )
    if df is None or df.empty:
        return {}
    rules = {}
    for _, r in df.iterrows():
        cargo = str(r.get('cargo_label') or '').strip()
        doc_tipo = str(r.get('doc_tipo') or '').strip()
        if not cargo or not doc_tipo:
            continue
        rules.setdefault(cargo, []).append(doc_tipo)
    return {k: list(dict.fromkeys(v)) for k, v in rules.items()}


def segav_cargo_rules():
    rules = get_segav_cargo_rules(DB_BACKEND, PG_DSN_FINGERPRINT)
    return rules or {k: list(dict.fromkeys(v)) for k, v in CARGO_DOCS_RULES.items()}


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_empresa_docs_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT doc_tipo, obligatorio, mensual, por_mandante, por_faena, sort_order FROM segav_erp_docs_empresa ORDER BY sort_order, doc_tipo")
    return df if df is not None else pd.DataFrame()


def segav_empresa_docs_df():
    return get_segav_empresa_docs_df(DB_BACKEND, PG_DSN_FINGERPRINT)


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_templates_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT template_key, template_label, vertical, description, payload_json, sort_order, activo FROM segav_erp_templates ORDER BY sort_order, template_label")
    return df if df is not None else pd.DataFrame()


def segav_templates_df():
    return get_segav_templates_df(DB_BACKEND, PG_DSN_FINGERPRINT)


def segav_template_payload(template_key: str) -> dict:
    df = segav_templates_df()
    if df is not None and not df.empty:
        row = df[df['template_key'].astype(str) == str(template_key)]
        if not row.empty:
            raw = str(row.iloc[0].get('payload_json') or '')
            try:
                return json.loads(raw) if raw else {}
            except Exception:
                return {}
    return dict(ERP_TEMPLATE_PRESETS.get(str(template_key), {}))


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_clientes_df(_backend: str, _dsn: str):
    df = fetch_df("SELECT cliente_key, cliente_nombre, rut, vertical, modo_implementacion, activo, contacto, email, observaciones, created_at, updated_at FROM segav_erp_clientes ORDER BY COALESCE(activo,1) DESC, cliente_nombre")
    return df if df is not None else pd.DataFrame()


def segav_clientes_df():
    return get_segav_clientes_df(DB_BACKEND, PG_DSN_FINGERPRINT)


def current_segav_client_key() -> str:
    session_key = str(st.session_state.get('active_cliente_key') or '').strip()
    if session_key:
        return session_key
    return segav_erp_value('current_client_key', '')


def current_tenant_key() -> str:
    key = str(current_segav_client_key() or '').strip()
    if key:
        return key
    try:
        cli_df = segav_clientes_df()
        if cli_df is not None and not cli_df.empty:
            active_df = cli_df
            if 'activo' in active_df.columns:
                active_df = active_df[active_df['activo'].fillna(1).astype(int) == 1]
            if not active_df.empty:
                key = str(active_df.iloc[0].get('cliente_key') or '').strip()
                if key:
                    return key
    except Exception:
        pass
    return ''


def tenantize_folder_parts(folder_parts):
    parts = list(folder_parts or [])
    tkey = current_tenant_key()
    if not tkey:
        return parts
    return ['clientes', storage_safe_segment(tkey), *parts]


MULTIEMPRESA_TABLES = [
    'mandantes', 'contratos_faena', 'faenas', 'faena_anexos', 'trabajadores', 'asignaciones',
    'trabajador_documentos', 'empresa_documentos', 'faena_empresa_documentos', 'export_historial',
    'export_historial_mes', 'sgsst_empresa', 'sgsst_matriz_legal', 'sgsst_programa_anual',
    'sgsst_miper', 'sgsst_inspecciones', 'sgsst_incidentes', 'sgsst_capacitaciones', 'sgsst_auditoria',
]


def ensure_multiempresa_columns_postgres():
    if DB_BACKEND != 'postgres':
        return
    for table in MULTIEMPRESA_TABLES:
        try:
            execute(f"ALTER TABLE IF EXISTS {table} ADD COLUMN IF NOT EXISTS cliente_key TEXT;")
        except Exception:
            pass


def ensure_multiempresa_columns_sqlite(c):
    if DB_BACKEND == 'postgres':
        return
    for table in MULTIEMPRESA_TABLES:
        try:
            migrate_add_columns_if_missing(c, table, {'cliente_key': 'TEXT'})
        except Exception:
            pass


def _resolve_cliente_key_by_patterns(df, patterns):
    if df is None or df.empty:
        return ''
    for _, row in df.iterrows():
        nm = normalize_text(row.get('cliente_nombre') or '')
        for pats in patterns:
            if all(p in nm for p in pats):
                return str(row.get('cliente_key') or '').strip()
    return ''


def resolve_legacy_owner_client_key() -> str:
    stored = str(segav_erp_value('legacy_owner_client_key', '') or '').strip()
    try:
        df = segav_clientes_df()
    except Exception:
        df = pd.DataFrame()
    if stored and df is not None and not df.empty and stored in df['cliente_key'].astype(str).tolist():
        return stored
    if df is None or df.empty:
        return stored
    patterns = [
        ('maderas', 'gyd'),
        ('maderas', 'galvez'),
        ('maderas', 'genova'),
        ('sociedad', 'maderera'),
        ('maderas',),
        ('gyd',),
    ]
    key = _resolve_cliente_key_by_patterns(df, patterns)
    if not key:
        non_segav = df[~df['cliente_nombre'].astype(str).map(normalize_text).str.contains('segav', na=False)]
        if not non_segav.empty:
            key = str(non_segav.iloc[0].get('cliente_key') or '').strip()
        else:
            key = str(df.iloc[0].get('cliente_key') or '').strip()
    return key


def resolve_segav_client_key() -> str:
    try:
        df = segav_clientes_df()
    except Exception:
        return ''
    if df is None or df.empty:
        return ''
    return _resolve_cliente_key_by_patterns(df, [('segav',)])


def backfill_multiempresa_cliente_key():
    legacy_owner_key = resolve_legacy_owner_client_key()
    if not legacy_owner_key:
        return
    try:
        if str(segav_erp_value('legacy_owner_client_key', '') or '').strip() != legacy_owner_key:
            set_segav_erp_config_value('legacy_owner_client_key', legacy_owner_key)
    except Exception:
        pass
    already_done = str(segav_erp_value('legacy_backfill_v2_done', 'NO') or 'NO').strip().upper() == 'SI'
    if already_done:
        return
    segav_key = resolve_segav_client_key()
    for table in MULTIEMPRESA_TABLES:
        try:
            execute(f"UPDATE {table} SET cliente_key=? WHERE cliente_key IS NULL OR TRIM(cliente_key)=''", (legacy_owner_key,))
        except Exception:
            pass
        if segav_key and segav_key != legacy_owner_key:
            try:
                segav_count = int(fetch_value(f"SELECT COUNT(*) FROM {table} WHERE COALESCE(cliente_key,'')=?", (segav_key,), default=0) or 0)
                owner_count = int(fetch_value(f"SELECT COUNT(*) FROM {table} WHERE COALESCE(cliente_key,'')=?", (legacy_owner_key,), default=0) or 0)
                if segav_count > 0 and owner_count == 0:
                    execute(f"UPDATE {table} SET cliente_key=? WHERE COALESCE(cliente_key,'')=?", (legacy_owner_key, segav_key))
            except Exception:
                pass
    try:
        clear_app_caches()
    except Exception:
        pass
    try:
        set_segav_erp_config_value('legacy_backfill_v2_done', 'SI')
    except Exception:
        pass


def ensure_active_tenant_scaffold():
    # Las empresas nuevas deben partir vacías: no reasignar datos al cambiar de tenant.
    return True


TENANT_SCOPE_TABLES = tuple(MULTIEMPRESA_TABLES)
TENANT_SCOPE_FILE_TABLES = (
    'contratos_faena', 'faena_anexos', 'trabajador_documentos', 'empresa_documentos',
    'faena_empresa_documentos', 'export_historial', 'export_historial_mes'
)


def _tenant_scope_target_table(sql: str) -> str | None:
    q = str(sql or '')
    patterns = [
        r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"\bDELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"\bINSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)",
    ]
    for patt in patterns:
        m = re.search(patt, q, flags=re.I)
        if m:
            table = str(m.group(1) or '').strip()
            if table in TENANT_SCOPE_TABLES:
                return table
    return None


def _inject_tenant_condition_sql(sql: str, alias_or_table: str) -> str:
    tenant_cond = f"COALESCE({alias_or_table}.cliente_key,'')=?"
    lower_sql = sql.lower()
    clause_positions = [p for p in [lower_sql.find(' order by '), lower_sql.find(' group by '), lower_sql.find(' limit '), lower_sql.find(' union ')] if p != -1]
    cut = min(clause_positions) if clause_positions else len(sql)
    head = sql[:cut]
    tail = sql[cut:]
    if re.search(r"\bwhere\b", head, flags=re.I):
        return head + f" AND {tenant_cond}" + tail
    return head + f" WHERE {tenant_cond}" + tail


def _scope_sql_to_tenant(sql: str, params=(), tenant_key: str | None = None):
    tenant_key = str(tenant_key or current_tenant_key() or '').strip()
    q = str(sql or '')
    if not tenant_key or not q.strip():
        return q, tuple(params or ())
    if 'cliente_key' in q.lower():
        return q, tuple(params or ())
    table = _tenant_scope_target_table(q)
    if not table:
        return q, tuple(params or ())

    params_t = tuple(params or ())
    # INSERT INTO table(cols) VALUES(...)
    m_ins = re.search(r"(\bINSERT\s+INTO\s+" + re.escape(table) + r"\s*\()([^)]*)(\)\s*VALUES\s*\()", q, flags=re.I | re.S)
    if m_ins:
        cols_txt = m_ins.group(2)
        cols = [c.strip() for c in cols_txt.split(',') if c.strip()]
        if not any(c.lower() == 'cliente_key' for c in cols):
            new_cols = 'cliente_key, ' + cols_txt.strip()
            start, end = m_ins.span(2)
            q2 = q[:start] + new_cols + q[end:]
            val_start = m_ins.end(3)
            q2 = q2[:val_start] + '?, ' + q2[val_start:]
            return q2, (tenant_key, *params_t)
        return q, params_t

    # UPDATE / DELETE / SELECT root table scoping
    m_root = re.search(r"\b(FROM|UPDATE|DELETE\s+FROM)\s+" + re.escape(table) + r"(?:\s+([A-Za-z_][A-Za-z0-9_]*))?", q, flags=re.I)
    alias = table
    if m_root:
        alias_candidate = str(m_root.group(2) or '').strip()
        if alias_candidate and alias_candidate.upper() not in {'SET', 'WHERE', 'ORDER', 'GROUP', 'LIMIT', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'ON'}:
            alias = alias_candidate
    q2 = _inject_tenant_condition_sql(q, alias)
    return q2, (*params_t, tenant_key)


def tenant_fetch_df(q: str, params=()):
    q2, p2 = _scope_sql_to_tenant(q, params)
    return fetch_df(q2, p2)


def tenant_fetch_df_uncached(q: str, params=()):
    q2, p2 = _scope_sql_to_tenant(q, params)
    return fetch_df_uncached(q2, p2)


def tenant_fetch_value(q: str, params=(), default=None, fresh: bool = False):
    q2, p2 = _scope_sql_to_tenant(q, params)
    return fetch_value(q2, p2, default=default, fresh=fresh)


def tenant_execute(q: str, params=()):
    q2, p2 = _scope_sql_to_tenant(q, params)
    return execute(q2, p2)


def tenant_execute_rowcount(q: str, params=()):
    q2, p2 = _scope_sql_to_tenant(q, params)
    return execute_rowcount(q2, p2)


def tenant_executemany(q: str, seq_params):
    scoped = []
    q2 = None
    for params in (seq_params or []):
        q2, p2 = _scope_sql_to_tenant(q, params)
        scoped.append(p2)
    if q2 is None:
        q2 = q
    return executemany(q2, scoped)


def tenant_fetch_file_refs(table_name: str, where_sql: str = "", params=()):
    if table_name in TENANT_SCOPE_TABLES and 'cliente_key' not in str(where_sql).lower():
        where_sql = (where_sql + " AND " if where_sql else "") + "COALESCE(cliente_key,'')=?"
        params = (*tuple(params or ()), current_tenant_key())
    return fetch_file_refs(table_name, where_sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_segav_cliente_params_df(_backend: str, _dsn: str, cliente_key: str):
    if not cliente_key:
        return pd.DataFrame(columns=['cliente_key','param_key','param_value'])
    df = fetch_df("SELECT cliente_key, param_key, param_value FROM segav_erp_parametros_cliente WHERE cliente_key=? ORDER BY param_key", (cliente_key,))
    return df if df is not None else pd.DataFrame()


def segav_cliente_params(cliente_key: str) -> dict:
    df = get_segav_cliente_params_df(DB_BACKEND, PG_DSN_FINGERPRINT, str(cliente_key or ''))
    if df is None or df.empty:
        return dict(ERP_CLIENT_PARAM_DEFAULTS)
    params = {str(r.get('param_key') or ''): str(r.get('param_value') or '') for _, r in df.iterrows()}
    merged = dict(ERP_CLIENT_PARAM_DEFAULTS)
    merged.update(params)
    return merged


def apply_segav_template(template_key: str):
    payload = segav_template_payload(template_key)
    if not payload:
        return False, 'Plantilla no disponible.'
    now = datetime.now().isoformat(timespec='seconds')
    cargos = [str(c).strip().upper() for c in payload.get('cargos', []) if str(c).strip()]
    cargo_rules = payload.get('cargo_rules', {}) or {}
    empresa_docs = [str(d).strip() for d in payload.get('empresa_docs', []) if str(d).strip()]

    for idx, cargo in enumerate(cargos, start=1):
        execute("DELETE FROM segav_erp_cargos WHERE cargo_key=?", (cargo,))
        execute("INSERT INTO segav_erp_cargos(cargo_key, cargo_label, sort_order, activo, updated_at) VALUES(?,?,?,?,?)", (cargo, cargo, idx, 1, now))
        docs = [str(d).strip() for d in cargo_rules.get(cargo, DOC_OBLIGATORIOS) if str(d).strip()]
        execute("DELETE FROM segav_erp_docs_cargo WHERE cargo_key=?", (cargo,))
        for d_idx, doc_tipo in enumerate(list(dict.fromkeys(docs)), start=1):
            execute("INSERT INTO segav_erp_docs_cargo(cargo_key, doc_tipo, sort_order, updated_at) VALUES(?,?,?,?)", (cargo, doc_tipo, d_idx, now))

    for idx, doc_tipo in enumerate(list(dict.fromkeys(empresa_docs)), start=1):
        execute("DELETE FROM segav_erp_docs_empresa WHERE doc_tipo=?", (doc_tipo,))
        execute("INSERT INTO segav_erp_docs_empresa(doc_tipo, obligatorio, mensual, por_mandante, por_faena, sort_order, updated_at) VALUES(?,?,?,?,?,?,?)", (doc_tipo, 1, 1, 1, 1, idx, now))

    set_segav_erp_config_value('template_actual', template_key)
    if payload.get('vertical'):
        set_segav_erp_config_value('erp_vertical', str(payload.get('vertical')))
    clear_app_caches()
    return True, f"Plantilla {payload.get('label') or template_key} aplicada al catálogo ERP."


def get_empresa_required_doc_types() -> list[str]:
    df = segav_empresa_docs_df()
    if df is None or df.empty:
        return list(DOC_EMPRESA_REQUERIDOS)
    df = df[df['obligatorio'].fillna(1).astype(int) == 1]
    docs = [str(v).strip() for v in df['doc_tipo'].tolist() if str(v).strip()]
    return docs or list(DOC_EMPRESA_REQUERIDOS)


def get_empresa_monthly_doc_types() -> list[str]:
    df = segav_empresa_docs_df()
    if df is None or df.empty:
        return list(DOC_EMPRESA_MENSUALES)
    df = df[df['mensual'].fillna(1).astype(int) == 1]
    docs = [str(v).strip() for v in df['doc_tipo'].tolist() if str(v).strip()]
    return docs or list(DOC_EMPRESA_MENSUALES)


def sgsst_log(modulo: str, accion: str, detalle: str = ""):
    try:
        user = current_user()["username"] if current_user() else "sistema"
    except Exception:
        user = "sistema"
    try:
        execute(
            "INSERT INTO sgsst_auditoria(cliente_key, modulo, accion, detalle, usuario, created_at) VALUES(?,?,?,?,?,?)",
            (current_tenant_key(), modulo, accion, detalle, user, datetime.now().isoformat(timespec='seconds')),
        )
    except Exception:
        pass

# ----------------------------
# Auth (usuarios/roles/permisos)
# ----------------------------

AUTH_ITERATIONS = 200_000
LOGIN_LOGO_URL = "https://www.maderasgyd.cl/wp-content/uploads/2024/02/logo-maderas-gd-1.png"

@st.cache_data(ttl=21600, show_spinner=False)
def get_login_logo_bytes():
    if os.path.exists(LOCAL_BRAND_LOGO_PATH):
        try:
            with open(LOCAL_BRAND_LOGO_PATH, "rb") as fp:
                return fp.read()
        except Exception:
            pass
    return get_brand_logo_bytes(LOGIN_LOGO_URL)

@st.cache_data(ttl=21600, show_spinner=False)
def get_login_panel_approved_bytes():
    if os.path.exists(LOCAL_LOGIN_PANEL_APPROVED_PATH):
        try:
            with open(LOCAL_LOGIN_PANEL_APPROVED_PATH, "rb") as fp:
                return fp.read()
        except Exception:
            return None
    return None

@st.cache_data(ttl=21600, show_spinner=False)
def get_login_hero_bytes():
    if os.path.exists(LOCAL_LOGIN_HERO_PATH):
        try:
            with open(LOCAL_LOGIN_HERO_PATH, "rb") as fp:
                return fp.read()
        except Exception:
            return None
    return None

def render_brand_logo(width: int = 220):
    logo = get_login_logo_bytes()
    if logo:
        st.image(logo, width=width)
    else:
        st.markdown(f"### {erp_brand_name()}")

DEFAULT_PERMS = {
    "view_dashboard": True,
    "view_sgsst": True,
    "view_mandantes": True,
    "view_contratos": True,
    "view_faenas": True,
    "view_trabajadores": True,
    "view_docs_empresa": True,
    "view_docs_empresa_faena": True,
    "view_asignaciones": True,
    "view_docs_trabajador": True,
    "view_export": True,
    "view_backup": True,
    "manage_users": False,
}

ALL_PERM_KEYS = list(DEFAULT_PERMS.keys())
SUPERADMIN_PERMS = {k: True for k in ALL_PERM_KEYS}
USER_ROLE_OPTIONS = ["SUPERADMIN", "ADMIN", "OPERADOR", "LECTOR"]

ROLE_TEMPLATES = {
    "SUPERADMIN": SUPERADMIN_PERMS.copy(),
    "ADMIN": {**DEFAULT_PERMS, "manage_users": True},
    "OPERADOR": {**DEFAULT_PERMS, "manage_users": False},
    "LECTOR": {
        "view_dashboard": True,
        "view_sgsst": True,
        "view_mandantes": True,
        "view_contratos": True,
        "view_faenas": True,
        "view_trabajadores": True,
        "view_docs_empresa": True,
        "view_docs_empresa_faena": True,
        "view_asignaciones": True,
        "view_docs_trabajador": True,
        "view_export": True,
        "view_backup": False,
        "manage_users": False,
    },
}

def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")

def _b64d(s: str) -> bytes:
    return base64.b64decode((s or "").encode("utf-8"))

def hash_password(password: str, salt_b64: str | None = None) -> tuple[str, str]:
    if not password:
        raise ValueError("Password vacío")
    salt = _b64d(salt_b64) if salt_b64 else secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, AUTH_ITERATIONS)
    return _b64e(salt), _b64e(dk)

def verify_password(password: str, salt_b64: str, hash_b64: str) -> bool:
    try:
        _, h = hash_password(password, salt_b64=salt_b64)
        return secrets.compare_digest(h, hash_b64)
    except Exception:
        return False

def perms_from_row(role: str, perms_json: str | None):
    role = (role or "OPERADOR").upper()
    if role == "SUPERADMIN":
        return SUPERADMIN_PERMS.copy()
    perms = ROLE_TEMPLATES.get(role, ROLE_TEMPLATES["OPERADOR"]).copy()
    if perms_json:
        try:
            extra = json.loads(perms_json)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k in perms:
                        perms[k] = bool(v)
        except Exception:
            pass
    return perms

def ensure_users_table():
    if DB_BACKEND == "postgres":
        execute(
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
            """
        )
        execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        return

    execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            salt_b64 TEXT NOT NULL,
            pass_hash_b64 TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'OPERADOR',
            perms_json TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")

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


def _trabajador_get_id(cur_or_conn, rut: str):
    row = cursor_execute(cur_or_conn, "SELECT id FROM trabajadores WHERE rut=? AND COALESCE(cliente_key,'')=? ORDER BY id LIMIT 1", (rut, current_tenant_key())).fetchone()
    return int(row[0]) if row else None


def _trabajador_insert_or_update(cur_or_conn, *, rut: str, nombres: str, apellidos: str, cargo: str = "", centro_costo: str = "", email: str = "", fecha_contrato=None, vigencia_examen=None, overwrite: bool = True, existing_id=None):
    rut = clean_rut(rut)
    tenant_key = current_tenant_key()
    existing_id = int(existing_id) if existing_id not in (None, "") else None
    if existing_id is None:
        existing_id = _trabajador_get_id(cur_or_conn, rut)

    payload = (nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen)

    if existing_id is not None:
        if overwrite:
            cursor_execute(cur_or_conn, "UPDATE trabajadores SET nombres=?, apellidos=?, cargo=?, centro_costo=?, email=?, fecha_contrato=?, vigencia_examen=? WHERE id=? AND COALESCE(cliente_key,'')=?", (*payload, int(existing_id), tenant_key))
            return 'updated', int(existing_id)
        return 'skipped', int(existing_id)

    if DB_BACKEND == 'postgres':
        cursor_execute(cur_or_conn, "SELECT pg_advisory_xact_lock(hashtext('trabajadores_manual_id_insert'));")
        existing_id = _trabajador_get_id(cur_or_conn, rut)
        if existing_id is not None:
            if overwrite:
                cursor_execute(cur_or_conn, "UPDATE trabajadores SET nombres=?, apellidos=?, cargo=?, centro_costo=?, email=?, fecha_contrato=?, vigencia_examen=? WHERE id=? AND COALESCE(cliente_key,'')=?", (*payload, int(existing_id), tenant_key))
                return 'updated', int(existing_id)
            return 'skipped', int(existing_id)
        row = cursor_execute(cur_or_conn, "SELECT COALESCE(MAX(id), 0) + 1 FROM trabajadores").fetchone()
        next_id = int(row[0]) if row and row[0] is not None else 1
        cursor_execute(cur_or_conn, "INSERT INTO trabajadores(id, cliente_key, rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen) VALUES(?,?,?,?,?,?,?,?,?,?)", (next_id, tenant_key, rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen))
        return 'inserted', next_id

    cursor_execute(cur_or_conn, "INSERT INTO trabajadores(cliente_key, rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen) VALUES(?,?,?,?,?,?,?,?,?)", (tenant_key, rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen))
    new_id = _trabajador_get_id(cur_or_conn, rut)
    return 'inserted', int(new_id) if new_id is not None else None


def ensure_storage_columns_sqlite(c):
    if DB_BACKEND == "postgres":
        return
    targets = {
        "contratos_faena": {"bucket": "TEXT", "object_path": "TEXT"},
        "faena_anexos": {"bucket": "TEXT", "object_path": "TEXT"},
        "trabajador_documentos": {"bucket": "TEXT", "object_path": "TEXT"},
        "empresa_documentos": {"bucket": "TEXT", "object_path": "TEXT"},
        "faena_empresa_documentos": {"bucket": "TEXT", "object_path": "TEXT", "mandante_id": "INTEGER", "periodo_anio": "INTEGER", "periodo_mes": "INTEGER"},
        "export_historial": {"bucket": "TEXT", "object_path": "TEXT"},
        "export_historial_mes": {"bucket": "TEXT", "object_path": "TEXT"},
    }
    for table, cols in targets.items():
        try:
            migrate_add_columns_if_missing(c, table, cols)
        except Exception:
            pass

def users_count() -> int:
    try:
        df = fetch_df("SELECT COUNT(*) AS n FROM users")
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0

def admins_count(active_only: bool = True) -> int:
    try:
        if active_only:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='ADMIN' AND is_active=1")
        else:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='ADMIN'")
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0

def superadmins_count(active_only: bool = True) -> int:
    try:
        if active_only:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='SUPERADMIN' AND is_active=1")
        else:
            df = fetch_df("SELECT COUNT(*) AS n FROM users WHERE role='SUPERADMIN'")
        return int(df["n"].iloc[0]) if not df.empty else 0
    except Exception:
        return 0

def ensure_superadmin_exists():
    try:
        ensure_users_table()
        if superadmins_count(active_only=False) > 0:
            return
        src = fetch_df("SELECT id FROM users WHERE role='ADMIN' ORDER BY is_active DESC, id ASC LIMIT 1")
        if src.empty:
            return
        uid = int(src.iloc[0]["id"])
        execute(
            "UPDATE users SET role=?, perms_json=?, updated_at=datetime('now') WHERE id=?",
            ("SUPERADMIN", json.dumps(SUPERADMIN_PERMS), uid),
        )
    except Exception:
        pass


def auth_set_session(user_row: dict):
    st.session_state["auth_user"] = {
        "id": int(user_row["id"]),
        "username": str(user_row["username"]),
        "role": str(user_row.get("role") or "OPERADOR"),
        "perms": perms_from_row(str(user_row.get("role") or "OPERADOR"), user_row.get("perms_json")),
    }

def auth_logout():
    st.session_state.pop("auth_user", None)
    st.rerun()

def current_user():
    return st.session_state.get("auth_user")

def has_perm(perm: str) -> bool:
    u = current_user()
    if not u:
        return False
    if str(u.get("role") or "").upper() == "SUPERADMIN":
        return True
    return bool(u.get("perms", {}).get(perm, False))

def require_perm(perm: str):
    if not has_perm(perm):
        st.error("No tienes permisos para acceder a esta sección.")
        st.stop()


def is_superadmin() -> bool:
    u = current_user()
    if not u:
        return False
    return str(u.get("role") or "").upper() == "SUPERADMIN"


def ensure_user_client_access_table():
    if DB_BACKEND == "postgres":
        execute(
            """
            CREATE TABLE IF NOT EXISTS user_client_access (
                user_id BIGINT NOT NULL,
                cliente_key TEXT NOT NULL,
                is_company_admin BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (user_id, cliente_key)
            );
            """
        )
        execute("CREATE INDEX IF NOT EXISTS idx_user_client_access_cliente ON user_client_access(cliente_key)")
        execute("CREATE INDEX IF NOT EXISTS idx_user_client_access_user ON user_client_access(user_id)")
        return
    execute(
        """
        CREATE TABLE IF NOT EXISTS user_client_access (
            user_id INTEGER NOT NULL,
            cliente_key TEXT NOT NULL,
            is_company_admin INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, cliente_key)
        );
        """
    )
    execute("CREATE INDEX IF NOT EXISTS idx_user_client_access_cliente ON user_client_access(cliente_key)")
    execute("CREATE INDEX IF NOT EXISTS idx_user_client_access_user ON user_client_access(user_id)")


@st.cache_resource(show_spinner=False)
def ensure_user_client_access_table_once(_db_backend: str, _dsn_fingerprint: str):
    ensure_user_client_access_table()
    return True


@st.cache_resource(show_spinner=False)
def ensure_active_tenant_scaffold_once(_db_backend: str, _dsn_fingerprint: str, tenant_key: str):
    ensure_active_tenant_scaffold()
    return True


@st.cache_data(ttl=120, show_spinner=False)
def get_sidebar_faena_context_df(_db_backend: str, _dsn_fingerprint: str, tenant_key: str):
    tkey = str(tenant_key or '').strip()
    try:
        if tkey:
            return fetch_df(
                """
                SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
                FROM faenas f
                JOIN mandantes m ON m.id=f.mandante_id
                WHERE COALESCE(f.cliente_key,'')=?
                ORDER BY f.id DESC
                """,
                (tkey,),
            )
        return fetch_df(
            """
            SELECT f.id, m.nombre AS mandante, f.nombre, f.estado
            FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
            ORDER BY f.id DESC
            """
        )
    except Exception:
        return pd.DataFrame()


def visible_clientes_df():
    try:
        ensure_user_client_access_table_once(DB_BACKEND, PG_DSN_FINGERPRINT)
    except Exception:
        pass
    df = segav_clientes_df()
    if df is None or df.empty:
        return df
    if "activo" in df.columns:
        df = df[df["activo"].fillna(1).astype(int) == 1]
    if is_superadmin():
        return df
    u = current_user() or {}
    user_id = int(u.get("id") or 0)
    if user_id <= 0:
        return df
    try:
        acc_df = fetch_df("SELECT cliente_key FROM user_client_access WHERE user_id=?", (user_id,))
        if acc_df is None or acc_df.empty:
            return df
        allowed = set(acc_df["cliente_key"].astype(str).tolist())
        if not allowed:
            return df.iloc[0:0]
        return df[df["cliente_key"].astype(str).isin(allowed)]
    except Exception:
        return df


def auth_gate_ui():
    """Pantalla de acceso corporativa, de una sola vista y sin scroll en escritorio."""

    st.markdown(
        """
        <style>
        header[data-testid="stHeader"], div[data-testid="stToolbar"], section[data-testid="stSidebar"] {display:none !important;}
        html, body, [data-testid="stAppViewContainer"], .stApp {height:100vh !important; overflow:hidden !important;}
        .main, .block-container {
            padding:0 !important;
            margin:0 !important;
            max-width:none !important;
        }
        [data-testid="stAppViewContainer"] > .main {padding:0 !important;}
        .segav-login-screen {
            height:100vh;
            display:flex;
            align-items:stretch;
            background:#f5f7fb;
            overflow:hidden;
        }
        .segav-login-left {
            height:100vh;
            display:flex;
            align-items:center;
            justify-content:center;
            padding:28px 24px;
        }
        .segav-login-card {
            width:min(460px, 100%);
            background:#ffffff;
            border:1px solid rgba(15,23,42,0.08);
            border-radius:24px;
            box-shadow:0 18px 52px rgba(15,23,42,0.10);
            padding:28px 28px 22px 28px;
        }
        .segav-chip {
            display:inline-flex;
            align-items:center;
            gap:8px;
            padding:6px 12px;
            border-radius:999px;
            background:#eff6ff;
            color:#1d4ed8;
            font-size:11px;
            font-weight:800;
            letter-spacing:0.03em;
        }
        .segav-login-title {
            margin:12px 0 0 0;
            color:#0f172a;
            font-size:30px;
            line-height:1.05;
            font-weight:800;
        }
        .segav-login-sub {
            margin-top:10px;
            color:#475569;
            font-size:13px;
            line-height:1.5;
        }
        .segav-login-card [data-testid="stForm"] {
            border:none !important;
            background:transparent !important;
            padding:0 !important;
            margin-top:14px;
        }
        .segav-login-card [data-testid="stForm"] > div:first-child {
            border:none !important;
            padding:0 !important;
        }
        .segav-login-card [data-testid="stTextInputRootElement"] {
            border-radius:14px !important;
        }
        .segav-login-card div[data-testid="stFormSubmitButton"] > button {
            min-height:46px;
            font-weight:700;
            border-radius:14px !important;
        }
        .segav-logo-wrap {
            display:flex;
            flex-direction:column;
            align-items:center;
            justify-content:center;
            margin-top:14px;
            gap:8px;
        }
        .segav-logo-note {
            text-align:center;
            color:#334155;
            font-size:15px;
            font-weight:700;
            letter-spacing:0.02em;
        }
        .segav-footnote {
            margin-top:10px;
            color:#64748b;
            font-size:11px;
            line-height:1.4;
            text-align:center;
        }
        .segav-login-right {
            height:100vh;
            padding:18px 18px 18px 0;
            display:flex;
            align-items:center;
            justify-content:center;
        }
        .segav-login-panel {
            width:100%;
            height:calc(100vh - 36px);
            border-radius:28px;
            overflow:hidden;
            box-shadow:0 18px 52px rgba(2,8,23,0.18);
            border:1px solid rgba(255,255,255,0.18);
            background:linear-gradient(135deg, #0b2540 0%, #10395f 100%);
        }
        .segav-login-panel img {
            width:100%;
            height:100%;
            object-fit:cover;
            display:block;
        }
        @media (max-width: 980px) {
            html, body, [data-testid="stAppViewContainer"], .stApp {height:auto !important; overflow:auto !important;}
            .segav-login-screen {height:auto;}
            .segav-login-left, .segav-login-right {height:auto; padding:16px;}
            .segav-login-panel {height:auto;}
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="segav-login-screen">', unsafe_allow_html=True)
    left_col, right_col = st.columns([0.42, 0.58], gap="small")

    with left_col:
        st.markdown('<div class="segav-login-left"><div class="segav-login-card">', unsafe_allow_html=True)
        st.markdown('<span class="segav-chip">SEGAV ERP · Acceso seguro</span>', unsafe_allow_html=True)
        st.markdown('<div class="segav-login-title">Iniciar sesión</div>', unsafe_allow_html=True)
        st.markdown('<div class="segav-login-sub">Accede para administrar empresas, faenas, documentación, prevención de riesgos y paneles ejecutivos.</div>', unsafe_allow_html=True)

        ensure_users_table()
        ensure_superadmin_exists()

        if users_count() == 0:
            DEFAULT_ADMIN_USER = os.environ.get("DEFAULT_ADMIN_USER", "a.garcia")
            DEFAULT_ADMIN_PASS = os.environ.get("DEFAULT_ADMIN_PASS", "225188")
            try:
                salt_b64, h_b64 = hash_password(DEFAULT_ADMIN_PASS)
                perms_json = json.dumps(SUPERADMIN_PERMS)
                execute(
                    "INSERT INTO users(username, salt_b64, pass_hash_b64, role, perms_json, is_active) VALUES(?,?,?,?,?,1)",
                    (DEFAULT_ADMIN_USER, salt_b64, h_b64, "SUPERADMIN", perms_json),
                )
                auto_backup_db("users_seed_default_superadmin")
            except Exception:
                pass

        with st.form("form_login"):
            username = st.text_input("Usuario", placeholder="ej: a.garcia")
            password = st.text_input("Contraseña", type="password")
            ok = st.form_submit_button("Ingresar al ERP", type="primary", use_container_width=True)

        if ok:
            u = (username or "").strip()
            if not u or not password:
                st.error("Usuario y contraseña son obligatorios.")
                st.stop()
            df = fetch_df("SELECT * FROM users WHERE username=? AND is_active=1", (u,))
            if df.empty:
                st.error("Usuario no existe o está desactivado.")
                st.stop()
            row = df.iloc[0].to_dict()
            if not verify_password(password, row["salt_b64"], row["pass_hash_b64"]):
                st.error("Contraseña incorrecta.")
                st.stop()
            auth_set_session(row)
            st.success("Ingreso exitoso.")
            st.rerun()

        st.markdown('<div class="segav-logo-wrap">', unsafe_allow_html=True)
        render_brand_logo(width=140)
        st.markdown('<div class="segav-logo-note">SEGAV ERP</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('<div class="segav-footnote">Si olvidaste tu contraseña, un administrador puede restablecerla desde Usuarios.</div>', unsafe_allow_html=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    with right_col:
        panel_bytes = get_login_panel_approved_bytes()
        st.markdown('<div class="segav-login-right"><div class="segav-login-panel">', unsafe_allow_html=True)
        if panel_bytes:
            panel_b64 = _b64e(panel_bytes)
            st.markdown(
                f'<img alt="SEGAV ERP Login Panel" src="data:image/png;base64,{panel_b64}">',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<div style="height:100%;display:flex;align-items:center;justify-content:center;color:#e2e8f0;font-size:20px;font-weight:700;">SEGAV ERP</div>', unsafe_allow_html=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)
# ----------------------------
# Init
# ----------------------------
bootstrap_app_or_stop()

inject_css()

def _record_soft_error(context: str, exc: Exception | None = None):
    """Guarda fallos no críticos en session_state para diagnóstico local sin romper la UI."""
    try:
        logs = st.session_state.setdefault("soft_errors", [])
        msg = str(exc).strip() if exc is not None else ""
        logs.append({
            "at": datetime.now().isoformat(timespec="seconds"),
            "context": str(context or "").strip(),
            "message": msg[:300],
        })
        # evita crecimiento infinito
        if len(logs) > 30:
            del logs[:-30]
    except Exception:
        pass


# ----------------------------
# Auth gate
# ----------------------------
if current_user() is None:
    auth_gate_ui()


# ----------------------------
# Sidebar navigation
# ----------------------------
PAGES = [
    "Dashboard",
    "Cumplimiento / Alertas",
    "Mi Empresa / SGSST",
    "Mandantes",
    "Contratos de Faena",
    "Faenas",
    "Trabajadores",
    "Documentos Empresa",
    "Documentos Empresa (Faena)",
    "Asignar Trabajadores",
    "Documentos Trabajador",
    "Export (ZIP)",
    "Backup / Restore",
]

VISIBLE_PAGES = list(PAGES)
if is_superadmin():
    VISIBLE_PAGES = ["SuperAdmin / Empresas", *VISIBLE_PAGES]
if has_perm("manage_users"):
    VISIBLE_PAGES.append("Admin Usuarios")


# Aplica navegación solicitada por botones (antes de crear el widget del sidebar)
if st.session_state.get("nav_request") is not None:
    _req = st.session_state.get("nav_request")
    if _req in VISIBLE_PAGES:
        st.session_state["nav_page"] = _req
    if st.session_state.get("nav_request_faena_id") is not None:
        st.session_state["selected_faena_id"] = int(st.session_state.get("nav_request_faena_id"))
    st.session_state.pop("nav_request", None)
    st.session_state.pop("nav_request_faena_id", None)

# Normaliza nav_page por si quedó un valor antiguo en session_state
if st.session_state.get("nav_page") not in VISIBLE_PAGES:
    st.session_state["nav_page"] = "Dashboard"


if "nav_page" not in st.session_state:
    st.session_state["nav_page"] = "Dashboard"
# Si quedó algo inválido tras login/permisos, fuerza el primero visible
if st.session_state.get("nav_page") not in VISIBLE_PAGES:
    st.session_state["nav_page"] = VISIBLE_PAGES[0] if VISIBLE_PAGES else "Dashboard"

with st.sidebar:
    # Branding compacto
    try:
        render_brand_logo(width=170)
    except Exception as exc:
        _record_soft_error("sidebar.render_brand_logo", exc)
    st.markdown("**SEGAV ERP**")
    u = current_user()
    if u:
        st.caption(f"👤 {u['username']} · {u['role']}")

    try:
        ensure_user_client_access_table_once(DB_BACKEND, PG_DSN_FINGERPRINT)
        _cli_df = visible_clientes_df()
        if _cli_df is not None and not _cli_df.empty:
            _cli_df = _cli_df[_cli_df["activo"].fillna(1).astype(int) == 1] if "activo" in _cli_df.columns else _cli_df
            _cli_keys = _cli_df["cliente_key"].astype(str).tolist()
            if _cli_keys:
                _current_cli = current_segav_client_key() or _cli_keys[0]
                if _current_cli not in _cli_keys:
                    _current_cli = _cli_keys[0]
                _cli_name_map = {str(r["cliente_key"]): str(r["cliente_nombre"]) for _, r in _cli_df.iterrows()}
                _cli_row_map = {str(r["cliente_key"]): r for _, r in _cli_df.iterrows()}
                _cli_selected = st.selectbox(
                    "Empresa activa",
                    _cli_keys,
                    index=_cli_keys.index(_current_cli),
                    key="sidebar_cliente_activo",
                    format_func=lambda x: _cli_name_map.get(str(x), str(x)),
                )
                if _cli_selected != _current_cli:
                    st.session_state['active_cliente_key'] = _cli_selected
                    clear_app_caches()
                    st.rerun()
                _current_row = _cli_row_map.get(str(_cli_selected), _cli_df.iloc[0])
                _vertical = str(_current_row.get("vertical") or segav_erp_value("erp_vertical", "General"))
                st.caption(f"🏢 {_current_row['cliente_nombre']} · {_vertical}")
    except Exception:
        pass

    # Navegación (simple)
    PAGE_LABELS = {
        "Dashboard": "📊 Dashboard",
        "Cumplimiento / Alertas": "🚨 Cumplimiento / Alertas",
        "Mi Empresa / SGSST": "🧭 ERP / SGSST",
        "Mandantes": "🏢 Mandantes",
        "Contratos de Faena": "📄 Contratos",
        "Faenas": "🛠️ Faenas",
        "Trabajadores": "👷 Trabajadores",
        "Documentos Empresa": "🏛️ Docs Empresa",
        "Documentos Empresa (Faena)": "🏛️ Docs Empresa (Faena)",
        "Asignar Trabajadores": "🧩 Asignar",
        "Documentos Trabajador": "📎 Docs Trabajador",
        "Export (ZIP)": "📦 Export",
        "Backup / Restore": "💾 Backup",
        "SuperAdmin / Empresas": "🌐 SuperAdmin / Empresas",
        "Admin Usuarios": "🔐 Usuarios",
    }

    st.radio(
        "Secciones",
        VISIBLE_PAGES,
        key="nav_page",
        format_func=lambda x: PAGE_LABELS.get(x, x),
        label_visibility="collapsed",
    )

    # Contexto (colapsable)
    with st.expander("🔎 Contexto (Faena)", expanded=False):
        try:
            _fa = get_sidebar_faena_context_df(DB_BACKEND, PG_DSN_FINGERPRINT, current_segav_client_key())
        except Exception:
            _fa = pd.DataFrame()

        if not _fa.empty:
            default_id = st.session_state.get("selected_faena_id", int(_fa["id"].iloc[0]))
            opts = _fa["id"].tolist()
            if default_id not in opts:
                default_id = int(opts[0])
            idx = opts.index(default_id)
            faena_id = st.selectbox(
                "Faena",
                opts,
                index=idx,
                format_func=lambda x: f"{int(x)} · {_fa[_fa['id']==x].iloc[0]['mandante']} / {_fa[_fa['id']==x].iloc[0]['nombre']} ({_fa[_fa['id']==x].iloc[0]['estado']})",
            )
            st.session_state["selected_faena_id"] = int(faena_id)

            b1, b2 = st.columns(2)
            with b1:
                if st.button("📎 Docs", use_container_width=True):
                    st.session_state["nav_page"] = "Documentos Trabajador"
                    st.rerun()
            with b2:
                if st.button("📦 Export", use_container_width=True):
                    st.session_state["nav_page"] = "Export (ZIP)"
                    st.rerun()
        else:
            st.caption("(Aún no hay faenas)")

    # Acciones rápidas (colapsable)
    with st.expander("⚡ Acciones", expanded=False):
        a1, a2 = st.columns(2)
        with a1:
            if st.button("Mandante", use_container_width=True):
                st.session_state["nav_page"] = "Mandantes"
                st.rerun()
            if st.button("Faena", use_container_width=True):
                st.session_state["nav_page"] = "Faenas"
                st.rerun()
        with a2:
            if st.button("Trabajador", use_container_width=True):
                st.session_state["nav_page"] = "Trabajadores"
                st.rerun()
            if st.button("Asignar", use_container_width=True):
                st.session_state["nav_page"] = "Asignar Trabajadores"
                st.rerun()

    # Respaldo (colapsable)
    with st.expander("💾 Respaldo", expanded=False):
        if "auto_backup_enabled" not in st.session_state:
            st.session_state["auto_backup_enabled"] = True
        st.checkbox("Auto-backup al guardar (app.db)", key="auto_backup_enabled")

        last = st.session_state.get("last_auto_backup")
        if last and last.get("bytes"):
            st.success("Auto-backup listo")
            st.download_button(
                "Descargar último auto-backup",
                data=last["bytes"],
                file_name=last["name"],
                mime="application/octet-stream",
                use_container_width=True,
            )
            if st.button("Limpiar aviso", use_container_width=True):
                st.session_state.pop("last_auto_backup", None)
                st.rerun()

        st.caption("En Streamlit Community Cloud, el disco puede perderse en reboots. Usa Backup/Restore para respaldos completos.")

    # Cerrar sesión al final (limpio)
    if u and st.button("Cerrar sesión", use_container_width=True):
        auth_logout()

current_section = st.session_state.get("nav_page", "Dashboard")
st.title(f"{erp_brand_name()} — {current_section}")
try:
    _clientes_top = segav_clientes_df()
    _cli_key_top = current_segav_client_key()
    if _clientes_top is not None and not _clientes_top.empty and _cli_key_top:
        _row_top = _clientes_top[_clientes_top["cliente_key"].astype(str) == str(_cli_key_top)]
        if not _row_top.empty:
            _row_top = _row_top.iloc[0]
            st.caption(f"Cliente activo: {_row_top.get('cliente_nombre') or 'Sin definir'} · Vertical: {_row_top.get('vertical') or segav_erp_value('erp_vertical', 'General')} · Modo: {_row_top.get('modo_implementacion') or segav_erp_value('modo_implementacion', 'CONFIGURABLE')}")
except Exception as exc:
    _record_soft_error("topbar.active_client_caption", exc)


try:
    ensure_active_tenant_scaffold_once(DB_BACKEND, PG_DSN_FINGERPRINT, current_tenant_key())
except Exception as exc:
    _record_soft_error("ensure_active_tenant_scaffold_once", exc)

# ----------------------------
# Pages
# ----------------------------
# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_dashboard`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_dashboard`.
# Se conserva la última definición activa basada en módulos segav_core.



# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_compliance_alerts`.
# Se conserva la última definición activa basada en módulos segav_core.



# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_mandantes`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_contratos_faena`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_faenas`.
# Se conserva la última definición activa basada en módulos segav_core.


def _page_trabajadores_impl():
    ui_header("Trabajadores", "Carga masiva por Excel o gestión manual. Puedes crear, editar o eliminar trabajadores. Luego asigna a faenas y adjunta documentos.")
    tab_list, tab_gestion, tab_import = st.tabs(["📋 Listado", "🧩 Gestión", "📥 Importar Excel"])

    # -------------------------
    # Tab 1: Importación Excel
    # -------------------------
    with tab_import:
        st.write("Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        st.download_button(
            "⬇️ Descargar plantilla Excel de trabajadores",
            data=build_trabajadores_template_xlsx(),
            file_name="plantilla_trabajadores.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_tpl_excel_trabajadores",
        )
        st.caption("Usa esta plantilla para la carga masiva. Mantén los encabezados tal como vienen en el Excel.")
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

                        def _to_text_date_import_excel(v):
                            if v is None or pd.isna(v):
                                return None
                            if isinstance(v, datetime):
                                return str(v.date())
                            if isinstance(v, date):
                                return str(v)
                            return str(v)

                        with conn() as c:
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
                                fecha_contrato = _to_text_date_import_excel(r.get(fc_col)) if fc_col else None
                                vigencia_examen = _to_text_date_import_excel(r.get("vigencia_examen")) if has_ve else None

                                action, _tid = _trabajador_insert_or_update(
                                    c,
                                    rut=rut,
                                    nombres=nombres,
                                    apellidos=apellidos,
                                    cargo=cargo,
                                    centro_costo=centro_costo,
                                    email=email,
                                    fecha_contrato=fecha_contrato,
                                    vigencia_examen=vigencia_examen,
                                    overwrite=overwrite,
                                    existing_id=None,
                                )
                                if action == "inserted":
                                    inserted += 1
                                elif action == "updated":
                                    updated += 1
                                else:
                                    skipped += 1
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
    with tab_gestion:
        t_create, t_edit = st.tabs(["➕ Crear", "✏️ Editar / 🗑️ Eliminar"])

        with t_create:
            _apply_pending_trabajador_create_reset()
            _show_pending_trabajador_create_flash()
            st.caption("El RUT se formatea automáticamente al estilo chileno: XX.XXX.XXX-X")
            rut = rut_input("RUT", key="trabajador_create_rut", placeholder="12.345.678-9", help="Escribe el RUT sin preocuparte por puntos o guion. La app lo formatea sola.")
            nombres = st.text_input("Nombres", placeholder="Juan", key="trabajador_create_nombres")
            apellidos = st.text_input("Apellidos", placeholder="Pérez", key="trabajador_create_apellidos")
            cargo = st.selectbox("Cargo", segav_cargo_labels(active_only=True), key="trabajador_create_cargo")
            centro_costo = st.text_input("Centro de costo (opcional)", placeholder="FAENA", key="trabajador_create_cc")
            email = st.text_input("Email (opcional)", key="trabajador_create_email")
            fecha_contrato = st.date_input("Fecha de contrato (opcional)", value=None, key="trabajador_create_fc")
            vigencia_examen = st.date_input("Vigencia examen (opcional)", value=None, key="trabajador_create_ve")
            ok = st.button("Guardar trabajador", type="primary", key="trabajador_create_btn")

            if ok:
                rut_norm = clean_rut(rut)
                nombres_v = nombres.strip()
                apellidos_v = apellidos.strip()
                cargo_v = cargo.strip()
                centro_costo_v = centro_costo.strip()
                email_v = email.strip()
                fecha_contrato_v = str(fecha_contrato) if fecha_contrato else None
                vigencia_examen_v = str(vigencia_examen) if vigencia_examen else None

                if not (rut_norm.strip() and nombres_v and apellidos_v):
                    st.error("Debes completar RUT, Nombres y Apellidos.")
                    st.stop()
                try:
                    with conn() as c:
                        _action, _tid = _trabajador_insert_or_update(
                            c,
                            rut=rut_norm,
                            nombres=nombres_v,
                            apellidos=apellidos_v,
                            cargo=cargo_v,
                            centro_costo=centro_costo_v,
                            email=email_v,
                            fecha_contrato=fecha_contrato_v,
                            vigencia_examen=vigencia_examen_v,
                            overwrite=True,
                            existing_id=None,
                        )
                        c.commit()
                    st.session_state["_trabajador_create_reset_pending"] = True
                    st.session_state["_trabajador_create_flash"] = "Trabajador guardado."
                    auto_backup_db("trabajador")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo guardar: {e}")

        with t_edit:
            df = fetch_df("SELECT id, rut, apellidos, nombres, cargo, centro_costo, email, fecha_contrato, vigencia_examen FROM trabajadores ORDER BY apellidos, nombres")
            if df.empty:
                st.info("No hay trabajadores aún.")
                return

            def _fmt_trab_edit(x):
                r = df[df["id"] == x].iloc[0]
                return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

            tid = st.selectbox("Selecciona trabajador", df["id"].tolist(), format_func=_fmt_trab_edit, key="trab_edit_sel")
            row = df[df["id"] == tid].iloc[0]

            st.markdown("### ✏️ Editar trabajador")
            edit_prefix = f"trabajador_edit_{int(tid)}"
            if st.session_state.get(f"{edit_prefix}_loaded_id") != int(tid):
                st.session_state[f"{edit_prefix}_loaded_id"] = int(tid)
                st.session_state[f"{edit_prefix}_rut"] = clean_rut(str(row["rut"] or ""))
                st.session_state[f"{edit_prefix}_nombres"] = str(row["nombres"] or "")
                st.session_state[f"{edit_prefix}_apellidos"] = str(row["apellidos"] or "")
                st.session_state[f"{edit_prefix}_cargo"] = str(row["cargo"] or "")
                st.session_state[f"{edit_prefix}_cc"] = str(row["centro_costo"] or "")
                st.session_state[f"{edit_prefix}_email"] = str(row["email"] or "")
                st.session_state[f"{edit_prefix}_fc"] = parse_date_maybe(row["fecha_contrato"])
                st.session_state[f"{edit_prefix}_ve"] = parse_date_maybe(row["vigencia_examen"])

            rut_new = rut_input("RUT", key=f"{edit_prefix}_rut", value=str(row["rut"] or ""), placeholder="12.345.678-9", help="Escribe el RUT sin preocuparte por puntos o guion. La app lo formatea sola.")
            nombres_new = st.text_input("Nombres", key=f"{edit_prefix}_nombres")
            apellidos_new = st.text_input("Apellidos", key=f"{edit_prefix}_apellidos")
            cargo_base_options = segav_cargo_labels(active_only=True)
            cargo_actual = str(st.session_state.get(f"{edit_prefix}_cargo", "") or "").strip()
            cargo_options = cargo_base_options.copy()
            if cargo_actual and cargo_actual not in cargo_options:
                cargo_options = [cargo_actual] + cargo_options
            cargo_default = cargo_options.index(cargo_actual) if cargo_actual in cargo_options else 0
            cargo_new = st.selectbox("Cargo", cargo_options, index=cargo_default, key=f"{edit_prefix}_cargo_select")
            st.session_state[f"{edit_prefix}_cargo"] = cargo_new
            cc_new = st.text_input("Centro de costo (opcional)", key=f"{edit_prefix}_cc")
            email_new = st.text_input("Email (opcional)", key=f"{edit_prefix}_email")
            fc_new = st.date_input("Fecha de contrato (opcional)", key=f"{edit_prefix}_fc")
            ve_new = st.date_input("Vigencia examen (opcional)", key=f"{edit_prefix}_ve")
            ok_upd = st.button("Guardar cambios", type="primary", key=f"{edit_prefix}_save")

            if ok_upd:
                if not (rut_new.strip() and nombres_new.strip() and apellidos_new.strip()):
                    st.error("Debes completar RUT, Nombres y Apellidos.")
                    st.stop()
                try:
                    rut_norm_new = clean_rut(rut_new)
                    execute(
                        "UPDATE trabajadores SET rut=?, nombres=?, apellidos=?, cargo=?, centro_costo=?, email=?, fecha_contrato=?, vigencia_examen=? WHERE id=?",
                        (
                            rut_norm_new,
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
            st.caption("Se eliminarán también sus asignaciones a faenas y sus documentos. La app intentará limpiar además los archivos físicos que ya no queden referenciados.")

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
                    refs = fetch_file_refs("trabajador_documentos", "trabajador_id=?", (int(tid),))
                    execute("DELETE FROM asignaciones WHERE trabajador_id=?", (int(tid),))
                    execute("DELETE FROM trabajador_documentos WHERE trabajador_id=?", (int(tid),))
                    execute("DELETE FROM trabajadores WHERE id=?", (int(tid),))
                    cleanup_issues = cleanup_deleted_file_refs(refs)
                    if cleanup_issues:
                        st.warning("Trabajador eliminado, pero hubo problemas al limpiar archivos asociados: " + " | ".join(cleanup_issues))
                    else:
                        st.success("Trabajador eliminado.")
                    auto_backup_db("trabajador_delete")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo eliminar: {e}")

    # -------------------------
    # Tab 3: Listado
    # -------------------------
    with tab_list:
        df = fetch_df(
            """
            SELECT
                t.id,
                t.rut,
                t.apellidos,
                t.nombres,
                t.cargo,
                COALESCE(
                    (
                        SELECT f.nombre
                        FROM asignaciones a
                        JOIN faenas f ON f.id = a.faena_id
                        WHERE a.trabajador_id = t.id
                          AND COALESCE(NULLIF(TRIM(UPPER(a.estado)), ''), 'ACTIVA') <> 'CERRADA'
                        ORDER BY a.id DESC
                        LIMIT 1
                    ),
                    'PLANTA'
                ) AS faena_actual,
                t.email,
                t.fecha_contrato,
                t.vigencia_examen
            FROM trabajadores t
            ORDER BY t.id DESC
            """
        )
        q = st.text_input("Buscar", placeholder="RUT, nombre, cargo o faena", key="q_trab_list")
        out = df.copy()
        if q.strip():
            qq = q.strip().lower()
            out = out[
                out["rut"].astype(str).str.lower().str.contains(qq, na=False) |
                out["apellidos"].astype(str).str.lower().str.contains(qq, na=False) |
                out["nombres"].astype(str).str.lower().str.contains(qq, na=False) |
                out["cargo"].astype(str).str.lower().str.contains(qq, na=False) |
                out["faena_actual"].astype(str).str.lower().str.contains(qq, na=False)
            ]
        show = out.rename(
            columns={
                "rut": "RUT",
                "apellidos": "Apellidos",
                "nombres": "Nombres",
                "cargo": "Cargo",
                "faena_actual": "Faena actual",
                "email": "Email",
                "fecha_contrato": "Fecha de contrato",
                "vigencia_examen": "Vigencia examen",
            }
        )
        st.dataframe(show, use_container_width=True, hide_index=True)
        st.caption("Para editar/eliminar: ve a la pestaña **Gestión → Editar / Eliminar**.")

def _page_asignar_trabajadores_impl():
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
                inserted_count = 0
                skipped_count = 0
                with conn() as c:
                    for tid in seleccion:
                        cur = cursor_execute(
                            c,
                            ASSIGNACION_INSERT_SQL,
                            (int(faena_id), int(tid), cargo_faena.strip(), str(fecha_ingreso), None, "ACTIVA"),
                        )
                        try:
                            rc = int(cur.rowcount or 0)
                        except Exception:
                            rc = 0
                        if rc > 0:
                            inserted_count += 1
                        else:
                            skipped_count += 1
                    c.commit()
                clear_app_caches()
                st.session_state["docs_scoped_toggle"] = True
                st.session_state.pop("docs_trabajador_pick", None)
                msg = f"Trabajadores asignados: {inserted_count}."
                if skipped_count:
                    msg += f" Omitidos por ya existir: {skipped_count}."
                st.success(msg)
                auto_backup_db("asignacion")
                st.rerun()

    # ---------------------------------
    # Tab 2: importar Excel y asignar
    # ---------------------------------
    with tab2:
        st.write("Sube Excel de trabajadores para **esta faena**. Columnas: **RUT, NOMBRE** (obligatorias) y opcionales: CARGO, CENTRO_COSTO, EMAIL, FECHA DE CONTRATO, VIGENCIA_EXAMEN.")
        st.download_button(
            "⬇️ Descargar plantilla Excel de trabajadores",
            data=build_trabajadores_template_xlsx(),
            file_name="plantilla_trabajadores.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_tpl_trab_faena",
        )
        st.caption("Puedes usar la misma plantilla para la carga masiva por faena.")
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

                        def _to_text_date_import_faena(v):
                            if v is None or pd.isna(v):
                                return None
                            if isinstance(v, datetime):
                                return str(v.date())
                            if isinstance(v, date):
                                return str(v)
                            return str(v)

                        with conn() as c:
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
                                fecha_contrato = _to_text_date_import_faena(r.get(fc_col)) if fc_col else None
                                vigencia_examen = _to_text_date_import_faena(r.get("vigencia_examen")) if has_ve else None

                                action, tid_saved = _trabajador_insert_or_update(
                                    c,
                                    rut=rut,
                                    nombres=nombres,
                                    apellidos=apellidos,
                                    cargo=cargo,
                                    centro_costo=centro_costo,
                                    email=email,
                                    fecha_contrato=fecha_contrato,
                                    vigencia_examen=vigencia_examen,
                                    overwrite=overwrite,
                                    existing_id=rut_to_id.get(rut),
                                )
                                if action == "inserted":
                                    inserted += 1
                                elif action == "updated":
                                    updated += 1
                                else:
                                    skipped += 1
                                    continue

                                # obtener id del trabajador
                                if rut not in rut_to_id:
                                    if tid_saved:
                                        rut_to_id[rut] = int(tid_saved)
                                    else:
                                        rid = cursor_execute(c, "SELECT id FROM trabajadores WHERE rut=?", (rut,)).fetchone()
                                        if rid:
                                            rut_to_id[rut] = int(rid[0])

                                tid = rut_to_id.get(rut)
                                if tid:
                                    cur_asg = cursor_execute(
                                        c,
                                        ASSIGNACION_INSERT_SQL,
                                        (int(faena_id), int(tid), cargo_faena_all.strip(), str(fecha_ingreso), None, "ACTIVA"),
                                    )
                                    try:
                                        assigned += int(cur_asg.rowcount or 0)
                                    except Exception:
                                        pass

                            c.commit()

                        clear_app_caches()
                        st.session_state["docs_scoped_toggle"] = True
                        st.session_state.pop("docs_trabajador_pick", None)
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
def _page_documentos_empresa_impl():
    ui_header("Documentos Empresa", "Carga documentos corporativos (valen para todas las faenas) y se incluyen en el ZIP de exportación.")
    st.caption("Puedes subir múltiples archivos por tipo. Los tipos requeridos base son liquidaciones de sueldo, F30, F30-1 y certificado de accidentabilidad; además puedes crear tus propios tipos con OTRO.")

    df = fetch_df("SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path, created_at FROM empresa_documentos ORDER BY id DESC")
    tipos_presentes = set(df["doc_tipo"].astype(str).tolist()) if not df.empty else set()
    faltan = [d for d in get_empresa_required_doc_types() if d not in tipos_presentes]

    c1, c2, c3 = st.columns([1, 1, 2])
    c1.metric("Tipos requeridos", len(get_empresa_required_doc_types()))
    c2.metric("Tipos presentes", len(set(tipos_presentes)))
    c3.metric("Faltan requeridos", len(faltan))

    if faltan:
        st.warning("Faltan requeridos: " + doc_tipo_join(faltan))
    else:
        st.success("Requeridos completos (si aplica).")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos requeridos base:")
        st.code("\n".join(doc_tipo_label(d) for d in get_empresa_required_doc_types()))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", get_empresa_required_doc_types() + ["OTRO"], format_func=lambda x: "OTRO" if x == "OTRO" else doc_tipo_label(x))
        with colx2:
            tipo_otro = st.text_input("Si eliges OTRO, escribe el nombre", placeholder="Ej: Política SST, Organigrama, Procedimiento crítico...")

        up = st.file_uploader("Archivo", key="up_doc_empresa", type=None)
        render_upload_help()
        if st.button("Guardar documento empresa", type="primary"):
            if up is None:
                st.error("Debes subir un archivo.")
                st.stop()
            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, 'type', None) or 'application/octet-stream')
            folder = ["empresa", safe_name(doc_tipo)]
            file_path, bucket, object_path = save_file_online(folder, payload["file_name"], payload["file_bytes"], content_type=payload["content_type"])
            sha = sha256_bytes(payload["file_bytes"])
            execute(
                "INSERT INTO empresa_documentos(doc_tipo, nombre_archivo, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?)",
                (doc_tipo, payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])
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
            st.markdown("#### 🔎 Gestionar documento")
            pick_id = st.selectbox(
                "Documento",
                docs["id"].tolist(),
                format_func=lambda x: f"{docs[docs['id']==x].iloc[0]['doc_tipo']} — {docs[docs['id']==x].iloc[0]['nombre_archivo']}",
                key="emp_pick_doc",
            )
            row = docs[docs["id"] == pick_id].iloc[0]
            fpath = row.get("file_path", "")
            bucket = row.get("bucket", None)
            object_path = row.get("object_path", None)
            fname = row.get("nombre_archivo", "documento")
            try:
                b = load_file_anywhere(fpath, bucket, object_path)
                st.download_button(
                    "Descargar documento",
                    data=b,
                    file_name=fname,
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="emp_dl_btn",
                )
            except Exception:
                st.warning(
                    "El archivo no está disponible (Storage/disco). "
                    "Verifica configuración de Storage o vuelve a cargar el documento."
                )

            confirm_del = st.checkbox(
                "Confirmo que quiero eliminar este documento cargado.",
                key="emp_del_confirm",
            )
            if st.button("Eliminar documento", type="secondary", use_container_width=True, key="emp_del_btn"):
                if not confirm_del:
                    st.error("Debes confirmar la eliminación.")
                    st.stop()
                result = delete_uploaded_document_record("empresa_documentos", int(pick_id))
                if result["shared_refs"]:
                    st.info("El registro fue eliminado de la base de datos. El archivo físico se conservó porque está referenciado en otro registro.")
                elif result["cleanup_issues"]:
                    st.warning("El registro fue eliminado de la base de datos, pero hubo un problema al limpiar el archivo: " + " | ".join(result["cleanup_issues"]))
                st.success(f"Documento eliminado: {result['file_name']}")
                auto_backup_db("doc_empresa_delete")
                st.rerun()
def _page_documentos_empresa_faena_impl():
    ui_header(
        "Documentos Empresa (Faena)",
        "Carga documentos de empresa POR FAENA, POR MANDANTE y POR MES. Cada período mensual puede tener varios archivos por tipo.",
    )

    faenas = fetch_df(
        """
        SELECT f.id, f.mandante_id, m.nombre AS mandante, f.nombre, f.estado, f.fecha_inicio
        FROM faenas f JOIN mandantes m ON m.id=f.mandante_id
        ORDER BY f.id DESC
        """
    )
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
    faena_row = faenas[faenas["id"] == faena_id].iloc[0]
    mandante_id = int(faena_row["mandante_id"])
    mandante_nombre = str(faena_row["mandante"])

    hoy = datetime.now()
    anios = sorted({int(hoy.year) - 1, int(hoy.year), int(hoy.year) + 1})
    meses = list(range(1, 13))
    ctop1, ctop2, ctop3 = st.columns([1.3, 1, 2.2])
    with ctop1:
        anio_sel = st.selectbox("Año del período", anios, index=anios.index(int(hoy.year)), key="emp_faena_periodo_anio")
    with ctop2:
        mes_sel = st.selectbox(
            "Mes del período",
            meses,
            index=max(0, min(11, int(hoy.month) - 1)),
            format_func=lambda x: f"{x:02d} · {MESES_ES.get(int(x), str(x))}",
            key="emp_faena_periodo_mes",
        )
    with ctop3:
        st.info(
            f"Mandante seleccionado: **{mandante_nombre}**\n"
            f"Faena: **{faena_row['nombre']}**\n"
            f"Período mensual: **{periodo_label(anio_sel, mes_sel)}**"
        )

    st.caption(
        "Documentación mensual requerida por mandante/faena: Liquidaciones de sueldo, Certificado de antecedentes laborales F30, "
        "Certificado de cumplimientos laborales y previsionales F30-1, y Certificado de accidentabilidad del período."
    )

    docs_periodo = fetch_df(
        "SELECT id, mandante_id, periodo_anio, periodo_mes, doc_tipo, nombre_archivo, file_path, bucket, object_path, created_at FROM faena_empresa_documentos WHERE faena_id=? AND COALESCE(periodo_anio,0)=? AND COALESCE(periodo_mes,0)=? ORDER BY id DESC",
        (int(faena_id), int(anio_sel), int(mes_sel)),
    )
    tipos_presentes = set(docs_periodo["doc_tipo"].astype(str).tolist()) if not docs_periodo.empty else set()
    faltan = [d for d in get_empresa_monthly_doc_types() if d not in tipos_presentes]

    c1, c2, c3 = st.columns([1, 1, 2])
    c1.metric("Requeridos mensuales", len(get_empresa_monthly_doc_types()))
    c2.metric("Tipos cargados en el período", len([d for d in get_empresa_monthly_doc_types() if d in tipos_presentes]))
    c3.metric("Faltan en el período", len(faltan))

    if faltan:
        st.warning("Faltan en este período: " + doc_tipo_join(faltan))
    else:
        st.success("Período mensual completo para esta faena/mandante.")

    tab1, tab2, tab3 = st.tabs(["📎 Cargar documento mensual", "📋 Documentos del período", "🗂️ Historial de la faena"])

    with tab1:
        st.caption("Tipos mensuales requeridos:")
        st.code("\n".join(doc_tipo_label(d) for d in get_empresa_monthly_doc_types()))
        st.caption("Para LIQUIDACIONES_SUELDO_MES puedes subir uno o varios archivos del mismo período.")

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", get_empresa_monthly_doc_types() + ["OTRO"], key="emp_faena_tipo", format_func=lambda x: "OTRO" if x == "OTRO" else doc_tipo_label(x))
        with colx2:
            tipo_otro = st.text_input(
                "Si eliges OTRO, escribe el nombre",
                placeholder="Ej: respaldo adicional mensual, informe complementario, control interno",
                key="emp_faena_otro",
            )

        up = st.file_uploader("Archivo", key="up_doc_emp_faena", type=None)
        render_upload_help()
        if st.button("Guardar documento mensual (empresa por faena)", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()

            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, 'type', None) or 'application/octet-stream')
            folder = [
                "mandantes",
                mandante_id,
                safe_name(mandante_nombre),
                "faenas",
                faena_id,
                safe_name(str(faena_row['nombre'])),
                periodo_ym(anio_sel, mes_sel),
                "empresa_mensual",
                safe_name(doc_tipo),
            ]
            file_path, bucket, object_path = save_file_online(folder, payload["file_name"], payload["file_bytes"], content_type=payload["content_type"])
            sha = sha256_bytes(payload["file_bytes"])

            execute(
                "INSERT INTO faena_empresa_documentos(faena_id, mandante_id, periodo_anio, periodo_mes, doc_tipo, nombre_archivo, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (int(faena_id), int(mandante_id), int(anio_sel), int(mes_sel), doc_tipo, payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),
            )
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])
            st.success(f"Documento guardado para {mandante_nombre} / {faena_row['nombre']} / {periodo_label(anio_sel, mes_sel)}.")
            auto_backup_db("doc_empresa_faena_mensual")
            st.rerun()

    with tab2:
        if docs_periodo.empty:
            st.info("(sin documentos cargados para este período)")
        else:
            show = docs_periodo[["doc_tipo", "nombre_archivo", "created_at"]].copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("#### 🔎 Gestionar documento del período")
            pick_id = st.selectbox(
                "Documento",
                docs_periodo["id"].tolist(),
                format_func=lambda x: f"{docs_periodo[docs_periodo['id']==x].iloc[0]['doc_tipo']} — {docs_periodo[docs_periodo['id']==x].iloc[0]['nombre_archivo']}",
                key="empf_pick_doc",
            )
            row = docs_periodo[docs_periodo["id"] == pick_id].iloc[0]
            fpath = row.get("file_path", "")
            bucket = row.get("bucket", None)
            object_path = row.get("object_path", None)
            fname = row.get("nombre_archivo", "documento")
            try:
                b = load_file_anywhere(fpath, bucket, object_path)
                st.download_button(
                    "Descargar documento",
                    data=b,
                    file_name=fname,
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="empf_dl_btn",
                )
            except Exception:
                st.warning(
                    "El archivo no está disponible (Storage/disco). "
                    "Verifica configuración de Storage o vuelve a cargar el documento."
                )

            confirm_del = st.checkbox(
                "Confirmo que quiero eliminar este documento cargado.",
                key="empf_del_confirm",
            )
            if st.button("Eliminar documento", type="secondary", use_container_width=True, key="empf_del_btn"):
                if not confirm_del:
                    st.error("Debes confirmar la eliminación.")
                    st.stop()
                result = delete_uploaded_document_record("faena_empresa_documentos", int(pick_id))
                if result["shared_refs"]:
                    st.info("El registro fue eliminado de la base de datos. El archivo físico se conservó porque está referenciado en otro registro.")
                elif result["cleanup_issues"]:
                    st.warning("El registro fue eliminado de la base de datos, pero hubo un problema al limpiar el archivo: " + " | ".join(result["cleanup_issues"]))
                st.success(f"Documento eliminado: {result['file_name']}")
                auto_backup_db("doc_empresa_faena_delete")
                st.rerun()

    with tab3:
        historial = fetch_df(
            "SELECT id, periodo_anio, periodo_mes, doc_tipo, nombre_archivo, created_at FROM faena_empresa_documentos WHERE faena_id=? ORDER BY COALESCE(periodo_anio,0) DESC, COALESCE(periodo_mes,0) DESC, id DESC",
            (int(faena_id),),
        )
        if historial.empty:
            st.info("(sin historial de documentos empresa por faena)")
        else:
            historial = historial.copy()
            historial["periodo"] = historial.apply(lambda r: periodo_label(r.get("periodo_anio"), r.get("periodo_mes")), axis=1)
            st.dataframe(historial[["periodo", "doc_tipo", "nombre_archivo", "created_at"]], use_container_width=True, hide_index=True)


def _page_documentos_trabajador_impl():
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

    last_scope_key = "_docs_last_scope_signature"
    current_scope_sig = (None if faena_pick is None else int(faena_pick), bool(scoped))
    if st.session_state.get(last_scope_key) != current_scope_sig:
        st.session_state[last_scope_key] = current_scope_sig
        st.session_state.pop("docs_trabajador_pick", None)

    # Fuente de trabajadores: por faena o global
    if scoped:
        if faena_pick is None:
            st.error("Activa 'Solo esta faena' pero no has seleccionado una faena.")
            st.stop()

        trab = fetch_assigned_workers(int(faena_pick), fresh=True)
        assigned_count = len(trab.index)
        st.caption(f"Trabajadores asignados detectados en esta faena: {assigned_count}")
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
                        st.error(f"{k} — faltan: {doc_tipo_join(missing)}")
                    else:
                        st.success(f"{k} — OK")
    else:
        trab = fetch_df_uncached("SELECT id, rut, apellidos, nombres, cargo FROM trabajadores ORDER BY apellidos, nombres")
        if trab.empty:
            ui_tip("Crea trabajadores primero.")
            return

    # Selector de trabajador (solo asignados si scoped)
    def _fmt_trab_docs(x):
        r = trab[trab["id"] == x].iloc[0]
        return f"{r['apellidos']} {r['nombres']} ({r['rut']})"

    tid = st.selectbox("Trabajador", trab["id"].tolist(), format_func=_fmt_trab_docs, key="docs_trabajador_pick")

    # Estado documental del trabajador (global: se reutiliza entre faenas)
    docs = fetch_df(
        "SELECT id, doc_tipo, nombre_archivo, file_path, bucket, object_path, created_at FROM trabajador_documentos WHERE trabajador_id=? ORDER BY id DESC",
        (int(tid),),
    )
    trabajador_row = trab[trab["id"] == tid].iloc[0]
    req_docs = worker_required_docs_for_record(trabajador_row)
    tipos_presentes = set(docs["doc_tipo"].astype(str).tolist()) if not docs.empty else set()
    faltan = [d for d in req_docs if d not in tipos_presentes]

    col1, col2, col3 = st.columns([1, 1, 2])
    col1.metric("Obligatorios", len(req_docs))
    col2.metric("Cargados", len([d for d in req_docs if d in tipos_presentes]))
    col3.metric("Faltan", len(faltan))

    cargo_label = canonical_cargo_label(trabajador_row.get("cargo"))
    st.caption(f"Cargo del trabajador: **{cargo_label}**")

    with st.expander("Ver documentos obligatorios por cargo", expanded=False):
        st.dataframe(pd.DataFrame(cargo_docs_catalog_rows()), use_container_width=True, hide_index=True)

    if faltan:
        st.warning("Faltan obligatorios: " + doc_tipo_join(faltan))
    else:
        st.success("Trabajador completo (obligatorios OK).")

    tab1, tab2 = st.tabs(["📎 Cargar documento", "📋 Documentos cargados"])

    with tab1:
        st.caption("Tipos obligatorios configurados para este trabajador:")
        st.code("\n".join(doc_tipo_label(d) for d in req_docs))

        colx1, colx2 = st.columns([1, 2])
        with colx1:
            tipo = st.selectbox("Tipo", req_docs + ["OTRO"], key="doc_tipo_pick", format_func=lambda x: "OTRO" if x == "OTRO" else doc_tipo_label(x))
        with colx2:
            tipo_otro = st.text_input(
                "Si eliges OTRO, escribe el nombre",
                placeholder="Ej: Certificación operador, Licencia, Examen ocupacional",
                key="doc_tipo_otro",
            )

        up = st.file_uploader("Archivo", key="up_doc_trabajador", type=None)
        render_upload_help()
        if st.button("Guardar documento", type="primary"):
            if up is None:
                st.error("Debes subir un archivo primero.")
                st.stop()

            doc_tipo = tipo if tipo != "OTRO" else (tipo_otro.strip() or "OTRO")
            payload = prepare_upload_payload(up.name, up.getvalue(), getattr(up, 'type', None) or 'application/octet-stream')
            folder = ["trabajadores", tid, safe_name(doc_tipo)]
            file_path, bucket, object_path = save_file_online(folder, payload["file_name"], payload["file_bytes"], content_type=payload["content_type"])
            sha = sha256_bytes(payload["file_bytes"])
            if payload["compressed"] and payload.get("compression_note"):
                st.info(payload["compression_note"])

            try:

                execute(

                    "INSERT INTO trabajador_documentos(trabajador_id, doc_tipo, nombre_archivo, file_path, bucket, object_path, sha256, created_at) VALUES(?,?,?,?,?,?,?,?)",

                    (int(tid), doc_tipo, payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds")),

                )

            except Exception:

                # Manejo de duplicados (UniqueViolation): actualiza el registro existente sin romper la app

                if DB_BACKEND == "postgres":

                    rc = execute_rowcount(

                        "UPDATE trabajador_documentos SET file_path=?, bucket=?, object_path=?, sha256=?, created_at=? "

                        "WHERE trabajador_id=? AND doc_tipo=? AND nombre_archivo=?",

                        (file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(tid), doc_tipo, payload["file_name"]),

                    )

                    if rc == 0:

                        execute_rowcount(

                            "UPDATE trabajador_documentos SET nombre_archivo=?, file_path=?, bucket=?, object_path=?, sha256=?, created_at=? "

                            "WHERE trabajador_id=? AND doc_tipo=?",

                            (payload["file_name"], file_path, bucket, object_path, sha, datetime.utcnow().isoformat(timespec="seconds"), int(tid), doc_tipo),

                        )

                else:

                    raise
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
            st.markdown("#### 🔎 Gestionar documento")

            ids = docs["id"].tolist()
            cur = st.session_state.get("trab_pick_doc", None)
            if cur not in ids:
                st.session_state["trab_pick_doc"] = ids[0]

            def _fmt_doc(x):
                try:
                    r = docs.loc[docs["id"] == x].iloc[0]
                    return f"{r.get('doc_tipo','DOC')} — {r.get('nombre_archivo','archivo')}"
                except Exception:
                    return f"ID {x}"

            pick_id = st.selectbox(
                "Documento",
                ids,
                format_func=_fmt_doc,
                key="trab_pick_doc",
            )

            sel = docs.loc[docs["id"] == pick_id]
            if sel.empty:
                st.warning("El documento seleccionado ya no está disponible en la lista. Vuelve a seleccionar.")
                return

            row = sel.iloc[0]
            fpath = row.get("file_path", "")
            bucket = row.get("bucket", None)
            object_path = row.get("object_path", None)
            fname = row.get("nombre_archivo", "documento")

            try:
                b = load_file_anywhere(fpath, bucket, object_path)
                st.download_button(
                    "Descargar documento",
                    data=b,
                    file_name=fname,
                    mime="application/octet-stream",
                    use_container_width=True,
                    key="trab_dl_btn",
                )
            except Exception:
                st.warning(
                    "El archivo no está disponible (Storage/disco). "
                    "Verifica configuración de Storage o vuelve a cargar el documento."
                )

            confirm_del = st.checkbox(
                "Confirmo que quiero eliminar este documento cargado.",
                key="trab_del_confirm",
            )
            if st.button("Eliminar documento", type="secondary", use_container_width=True, key="trab_del_btn"):
                if not confirm_del:
                    st.error("Debes confirmar la eliminación.")
                    st.stop()
                result = delete_uploaded_document_record("trabajador_documentos", int(pick_id))
                if result["shared_refs"]:
                    st.info("El registro fue eliminado de la base de datos. El archivo físico se conservó porque está referenciado en otro registro.")
                elif result["cleanup_issues"]:
                    st.warning("El registro fue eliminado de la base de datos, pero hubo un problema al limpiar el archivo: " + " | ".join(result["cleanup_issues"]))
                st.success(f"Documento eliminado: {result['file_name']}")
                auto_backup_db("doc_trabajador_delete")
                st.rerun()

def _get_bytes_impl(file_path, bucket, object_path):
    try:
        return load_file_anywhere(file_path, bucket, object_path)
    except Exception:
        return None


def _page_export_zip_impl():
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
                    st.error(f"{k} — faltan: {doc_tipo_join(missing)}")
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

        emp_global_types = fetch_df("SELECT DISTINCT doc_tipo FROM empresa_documentos ORDER BY doc_tipo")
        emp_global_list = emp_global_types["doc_tipo"].dropna().astype(str).tolist() if not emp_global_types.empty else []

        emp_faena_types = fetch_df("SELECT DISTINCT doc_tipo FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo", (int(faena_id),))
        emp_faena_list = emp_faena_types["doc_tipo"].dropna().astype(str).tolist() if not emp_faena_types.empty else []

        trab_types = fetch_df('''
            SELECT DISTINCT td.doc_tipo AS doc_tipo
            FROM trabajador_documentos td
            JOIN asignaciones a ON a.trabajador_id = td.trabajador_id
            WHERE a.faena_id=?
              AND COALESCE(NULLIF(TRIM(a.estado), ''), 'ACTIVA')='ACTIVA'
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
        st.markdown("#### 🎯 Selección específica de documentos (opcional)")
        st.caption("Si no activas una selección específica, el ZIP incluirá todos los documentos que cumplan los filtros anteriores.")

        emp_faena_doc_sel_ids = None
        selected_trab_ids = None
        selected_trab_doc_map = None

        if inc_emp_faena:
            emp_docs = fetch_df(
                "SELECT id, doc_tipo, nombre_archivo, file_path, object_path FROM faena_empresa_documentos WHERE faena_id=? ORDER BY doc_tipo, nombre_archivo, id",
                (int(faena_id),),
            )
            use_specific_emp_docs = st.checkbox(
                "Elegir documentos específicos de empresa para esta faena",
                value=False,
                key="exp_use_specific_emp_docs",
            )
            if use_specific_emp_docs:
                if emp_docs.empty:
                    st.caption("No hay documentos empresa por faena cargados.")
                    emp_faena_doc_sel_ids = []
                else:
                    emp_doc_labels = {}
                    for _, row in emp_docs.iterrows():
                        did = int(row["id"])
                        nombre = str(row.get("nombre_archivo") or row.get("file_path") or row.get("object_path") or f"documento_{did}")
                        nombre = os.path.basename(nombre)
                        venc = ""
                        emp_doc_labels[did] = f"{did} · {row.get('doc_tipo', '-')} · {nombre}{venc}"
                    emp_ids = list(emp_doc_labels.keys())
                    emp_faena_doc_sel_ids = st.multiselect(
                        "Documentos empresa por faena a exportar",
                        emp_ids,
                        default=emp_ids,
                        format_func=lambda x, labels=emp_doc_labels: labels.get(int(x), str(x)),
                        key="exp_emp_faena_doc_ids",
                    )
                    if not emp_faena_doc_sel_ids:
                        st.warning("No hay documentos empresa por faena seleccionados; esa carpeta quedará vacía en el ZIP.")

        if inc_trab:
            asign_docs = fetch_df('''
                SELECT t.id AS trabajador_id, t.rut, t.nombres, t.apellidos
                FROM asignaciones a
                JOIN trabajadores t ON t.id=a.trabajador_id
                WHERE a.faena_id=?
                  AND COALESCE(NULLIF(TRIM(a.estado), ''), 'ACTIVA')='ACTIVA'
                ORDER BY t.apellidos, t.nombres
            ''', (int(faena_id),))
            use_specific_workers = st.checkbox(
                "Elegir trabajadores específicos y sus documentos",
                value=False,
                key="exp_use_specific_workers",
            )
            if use_specific_workers:
                if asign_docs.empty:
                    st.caption("No hay trabajadores asignados a esta faena.")
                    selected_trab_ids = []
                    selected_trab_doc_map = {}
                else:
                    worker_labels = {}
                    for _, row in asign_docs.iterrows():
                        tid = int(row["trabajador_id"])
                        worker_labels[tid] = f"{row['apellidos']}, {row['nombres']} · {row['rut']}"
                    worker_ids = list(worker_labels.keys())
                    selected_trab_ids = st.multiselect(
                        "Trabajadores a incluir en el ZIP",
                        worker_ids,
                        default=worker_ids,
                        format_func=lambda x, labels=worker_labels: labels.get(int(x), str(x)),
                        key="exp_selected_trab_ids",
                    )
                    selected_trab_doc_map = {}
                    for tid in selected_trab_ids:
                        docs_worker = fetch_df(
                            "SELECT id, doc_tipo, nombre_archivo, file_path, object_path FROM trabajador_documentos WHERE trabajador_id=? ORDER BY doc_tipo, nombre_archivo, id",
                            (int(tid),),
                        )
                        with st.expander(f"Documentos de {worker_labels.get(int(tid), tid)}", expanded=False):
                            if docs_worker.empty:
                                st.caption("Este trabajador no tiene documentos cargados.")
                                selected_trab_doc_map[int(tid)] = []
                            else:
                                doc_labels = {}
                                for _, row in docs_worker.iterrows():
                                    did = int(row["id"])
                                    nombre = str(row.get("nombre_archivo") or row.get("file_path") or row.get("object_path") or f"documento_{did}")
                                    nombre = os.path.basename(nombre)
                                    venc = ""
                                    doc_labels[did] = f"{did} · {row.get('doc_tipo', '-')} · {nombre}{venc}"
                                doc_ids = list(doc_labels.keys())
                                selected_trab_doc_map[int(tid)] = st.multiselect(
                                    "Documentos a exportar",
                                    doc_ids,
                                    default=doc_ids,
                                    format_func=lambda x, labels=doc_labels: labels.get(int(x), str(x)),
                                    key=f"exp_trab_doc_ids_{int(faena_id)}_{int(tid)}",
                                )
                                if not selected_trab_doc_map[int(tid)]:
                                    st.warning("No hay documentos seleccionados para este trabajador; no se exportarán archivos de este trabajador.")

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
                        selected_empresa_faena_doc_ids=emp_faena_doc_sel_ids,
                        selected_trabajador_ids=selected_trab_ids,
                        selected_trabajador_doc_ids=selected_trab_doc_map,
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
        hist = fetch_df(
            """
            SELECT eh.id, eh.faena_id, f.nombre AS faena_nombre, eh.file_path, eh.bucket, eh.object_path,
                   eh.size_bytes, eh.created_at
            FROM export_historial eh
            LEFT JOIN faenas f ON f.id = eh.faena_id
            ORDER BY eh.id DESC
            """
        )
        if hist.empty:
            st.info("Aún no hay ZIPs exportados.")
        else:
            view = hist.copy()
            view["archivo"] = view.apply(
                lambda r: os.path.basename(str(r.get("file_path") or r.get("object_path") or f"export_{int(r['id'])}.zip")),
                axis=1,
            )
            view["tamaño"] = view["size_bytes"].apply(human_file_size)
            show_cols = ["id", "faena_id", "faena_nombre", "archivo", "tamaño", "created_at"]
            st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

            hid = st.selectbox(
                "ZIP del historial",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} - {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['created_at']})",
                key="exp_hist_pick",
            )
            row = view[view["id"] == hid].iloc[0]
            try:
                b = load_file_anywhere(row.get("file_path"), row.get("bucket"), row.get("object_path"))
                st.download_button(
                    "Descargar ZIP del historial",
                    data=b,
                    file_name=row["archivo"],
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_hist_dl",
                )
            except Exception as e:
                st.warning(f"No se pudo abrir el ZIP guardado: {e}")

    with tab4:
        st.markdown("### 📅 Export por mes")
        c1, c2 = st.columns(2)
        with c1:
            year = st.number_input("Año", min_value=2020, max_value=2100, value=date.today().year, step=1, key="exp_mes_year")
        with c2:
            month = st.number_input("Mes", min_value=1, max_value=12, value=date.today().month, step=1, key="exp_mes_month")

        inc_mes_emp_global = st.checkbox(
            "Incluir documentos empresa global en export mensual",
            value=True,
            key="exp_mes_inc_emp_global",
        )

        if st.button("Generar ZIP mensual y guardar en historial", type="primary", use_container_width=True, key="exp_mes_btn"):
            try:
                zip_bytes, ym = export_zip_for_mes(int(year), int(month), include_global_empresa_docs=inc_mes_emp_global)
                path_export = persist_export_mes(ym, zip_bytes)
                st.success(f"ZIP mensual generado y guardado: {os.path.basename(path_export)}")
                auto_backup_db("export_zip_mes")
                st.download_button(
                    "Descargar ZIP mensual (recién generado)",
                    data=zip_bytes,
                    file_name=os.path.basename(path_export),
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_mes_dl_now",
                )
            except Exception as e:
                st.error(f"No se pudo generar export mensual: {e}")

        st.divider()
        hist_mes = fetch_df(
            """
            SELECT id, year_month, file_path, bucket, object_path, size_bytes, created_at
            FROM export_historial_mes
            ORDER BY id DESC
            """
        )
        if hist_mes.empty:
            st.caption("Aún no hay exportaciones mensuales guardadas.")
        else:
            view = hist_mes.copy()
            view["archivo"] = view.apply(
                lambda r: os.path.basename(str(r.get("file_path") or r.get("object_path") or f"mes_{r.get('year_month','export')}.zip")),
                axis=1,
            )
            view["tamaño"] = view["size_bytes"].apply(human_file_size)
            st.dataframe(view[["id", "year_month", "archivo", "tamaño", "created_at"]], use_container_width=True, hide_index=True)

            mid = st.selectbox(
                "ZIP mensual del historial",
                view["id"].tolist(),
                format_func=lambda x: f"{int(x)} - {view[view['id']==x].iloc[0]['archivo']} ({view[view['id']==x].iloc[0]['year_month']})",
                key="exp_mes_hist_pick",
            )
            row = view[view["id"] == mid].iloc[0]
            try:
                b = load_file_anywhere(row.get("file_path"), row.get("bucket"), row.get("object_path"))
                st.download_button(
                    "Descargar ZIP mensual del historial",
                    data=b,
                    file_name=row["archivo"],
                    mime="application/zip",
                    use_container_width=True,
                    key="exp_mes_hist_dl",
                )
            except Exception as e:
                st.warning(f"No se pudo abrir el ZIP mensual guardado: {e}")

# Consolidación definitiva: se mantiene una sola implementación real por pantalla
# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_sgsst`.
# Se conserva la última definición activa basada en módulos segav_core.




# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_trabajadores`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_asignar_trabajadores`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_documentos_empresa`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_documentos_empresa_faena`.
# Se conserva la última definición activa basada en módulos segav_core.


# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_documentos_trabajador`.
# Se conserva la última definición activa basada en módulos segav_core.


def _get_bytes(file_path, bucket, object_path):
    return _get_bytes_impl(file_path, bucket, object_path)

# Legado saneado: definición antigua eliminada para evitar duplicidad de `page_export_zip`.
# Se conserva la última definición activa basada en módulos segav_core.


def page_backup_restore():
    ui_header("Backup / Restore", "Diagnostica el backend activo y gestiona respaldos locales o heredados sin confundirlos con la persistencia real online.")
    st.warning(
        "En Streamlit Community Cloud, los archivos locales (incluyendo SQLite y uploads) pueden perderse en reboots/redeploy. "
        "Si trabajas con Supabase/Postgres, la fuente de verdad está online y este módulo sirve sobre todo para diagnóstico y compatibilidad local heredada."
    )
    if DB_BACKEND == "postgres":
        st.info(
            "Modo actual: **Postgres/Supabase**. La base online es la fuente de verdad; por eso las opciones sobre **app.db** quedan solo como compatibilidad local heredada. "
            "Usa principalmente el diagnóstico de Storage y las exportaciones/documentos online."
        )

    tab1, tab2, tab3 = st.tabs(["🧪 Diagnóstico backend", "🗄️ Base local heredada (app.db)", "📦 Backup completo (ZIP)"])

    with tab1:
        cdiag1, cdiag2, cdiag3 = st.columns(3)
        cdiag1.metric("Backend activo", DB_BACKEND.upper())
        cdiag2.metric("Storage lectura", "Sí" if storage_enabled() else "No")
        cdiag3.metric("Storage admin", "Sí" if storage_admin_enabled() else "No")
        if DB_BACKEND == "postgres":
            st.info("Modo Postgres/Supabase activo. La persistencia real vive online. Los auto-backups/app.db de abajo se mantienen como compatibilidad local heredada.")
        else:
            st.info("Modo SQLite local activo. En este modo app.db sí es la fuente principal de datos.")
        if storage_enabled() and not storage_admin_enabled():
            st.warning("Storage está solo en modo lectura o con key débil. Para subir/eliminar archivos usa una secret/service key real en SUPABASE_SERVICE_ROLE_KEY.")
        st.caption("Auto-backups generados al guardar (solo app.db). Se guardan localmente y conviene descargarlos si sigues usando SQLite local.")
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
        if DB_BACKEND == "postgres":
            st.info("Esta pestaña aplica solo a respaldo/restauración de **SQLite local (app.db)**. En Supabase la persistencia real vive en Postgres; úsala solo como compatibilidad o diagnóstico local.")
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

        st.divider()
        st.markdown("### 🧪 Diagnóstico Storage (solo admin)")
        if not storage_enabled():
            st.info("Storage no está activo. Revisa Secrets: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY y SUPABASE_STORAGE_BUCKET. (SUPABASE_ANON_KEY es solo opcional para lectura)")
        else:
            st.success(f"Storage activo: bucket **{STORAGE_BUCKET}** · admin={'Sí' if storage_admin_enabled() else 'No'}")
            last = st.session_state.get("storage_last_error")
            if last:
                st.warning(f"Último error Storage: HTTP {last.get('status')} · {str(last.get('body',''))[:120]}")
                with st.expander("Ver detalle último error"):
                    st.write(last)
            if st.button("Probar subida Storage (archivo de prueba)", use_container_width=True):
                try:
                    test_path = f"_diagnostico/test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
                    storage_upload(test_path, b"ok", content_type="text/plain", upsert=True)
                    st.success(f"Subida OK: {test_path}")
                except Exception as e:
                    st.error(f"Falló prueba: {e}")
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

def page_admin_usuarios():
    ui_header("Administración de Usuarios", "Como SUPERADMIN puedes ver y gestionar todas las funciones. Más adelante podrás decidir qué ve cada usuario.")
    require_perm("manage_users")
    ensure_users_table()

    tab1, tab2 = st.tabs(["👥 Usuarios", "➕ Crear usuario"])

    with tab1:
        df = fetch_df("SELECT id, username, role, is_active, created_at, updated_at FROM users ORDER BY id DESC")
        if df.empty:
            st.info("No hay usuarios.")
            return
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()
        uid = st.selectbox(
            "Selecciona usuario",
            df["id"].tolist(),
            format_func=lambda x: df[df["id"]==x].iloc[0]["username"],
            key="adm_user_sel",
        )
        row = fetch_df("SELECT * FROM users WHERE id=?", (int(uid),)).iloc[0].to_dict()

        c1, c2, c3 = st.columns(3)
        with c1:
            role_options = list(USER_ROLE_OPTIONS)
            current_role = (row.get("role") or "OPERADOR").upper()
            if current_role not in role_options:
                role_options.append(current_role)
            new_role = st.selectbox(
                "Rol",
                role_options,
                index=role_options.index(current_role),
                key="adm_role_sel",
            )
            active = st.checkbox("Activo", value=bool(int(row.get("is_active", 1))), key="adm_active")
        with c2:
            st.markdown("**Reset contraseña**")
            pw1 = st.text_input("Nueva contraseña", type="password", key="adm_pw1")
            pw2 = st.text_input("Repetir", type="password", key="adm_pw2")
        with c3:
            st.markdown("**Eliminar**")
            del_confirm = st.checkbox("Confirmo eliminar usuario", key="adm_del_confirm")
            del_btn = st.button("Eliminar usuario", use_container_width=True, key="adm_del_btn")

        st.divider()
        st.markdown("### Poderes")
        current_perms = perms_from_row(new_role, row.get("perms_json"))
        cols = st.columns(3)
        keys = list(DEFAULT_PERMS.keys())
        new_perms = {}
        super_mode = (new_role or "").upper() == "SUPERADMIN"
        if super_mode:
            st.info("El rol SUPERADMIN ve todas las funciones del ERP por defecto.")
        for i, k in enumerate(keys):
            with cols[i % 3]:
                new_perms[k] = st.checkbox(k, value=bool(current_perms.get(k, False)), key=f"perm_{uid}_{k}", disabled=super_mode)

        if st.button("Guardar cambios", type="primary", use_container_width=True, key="adm_save_btn"):
            try:
                # Seguridad: SUPERADMIN y ADMIN conservan acceso de administración
                if (new_role or "").upper() == "SUPERADMIN":
                    new_perms = SUPERADMIN_PERMS.copy()
                elif (new_role or "").upper() == "ADMIN":
                    new_perms["manage_users"] = True

                # Evita desactivar al último SUPERADMIN activo
                if (row.get("role") or "").upper() == "SUPERADMIN" and (not active) and superadmins_count(active_only=True) <= 1:
                    st.error("No puedes desactivar al último SUPERADMIN activo.")
                    st.stop()

                # Evita desactivar al último ADMIN activo cuando no es SUPERADMIN
                if (row.get("role") or "").upper() == "ADMIN" and (new_role or "").upper() == "ADMIN" and (not active) and admins_count(active_only=True) <= 1:
                    st.error("No puedes desactivar al último ADMIN activo.")
                    st.stop()

                execute(
                    "UPDATE users SET role=?, perms_json=?, is_active=?, updated_at=datetime('now') WHERE id=?",
                    (new_role, json.dumps(new_perms), 1 if active else 0, int(uid)),
                )
                if pw1 or pw2:
                    if not pw1 or pw1 != pw2:
                        st.error("Contraseñas no coinciden o están vacías.")
                        st.stop()
                    salt_b64, h_b64 = hash_password(pw1)
                    execute(
                        "UPDATE users SET salt_b64=?, pass_hash_b64=?, updated_at=datetime('now') WHERE id=?",
                        (salt_b64, h_b64, int(uid)),
                    )
                auto_backup_db("users_update")
                st.success("Cambios guardados.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar: {e}")

        if del_btn:
            if not del_confirm:
                st.error("Debes confirmar antes de eliminar.")
                st.stop()
            cu = current_user()
            if cu and int(cu["id"]) == int(uid):
                st.error("No puedes eliminar tu propio usuario.")
                st.stop()
            # Evita eliminar al último SUPERADMIN activo
            if (row.get("role") or "").upper() == "SUPERADMIN" and superadmins_count(active_only=True) <= 1:
                st.error("No puedes eliminar al último SUPERADMIN activo.")
                st.stop()
            # Evita eliminar al último ADMIN activo
            if (row.get("role") or "").upper() == "ADMIN" and admins_count(active_only=True) <= 1:
                st.error("No puedes eliminar al último ADMIN activo.")
                st.stop()
            try:
                execute("DELETE FROM users WHERE id=?", (int(uid),))
                auto_backup_db("users_delete")
                st.success("Usuario eliminado.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo eliminar: {e}")

    with tab2:
        with st.form("form_create_user", clear_on_submit=True):
            username = st.text_input("Usuario", placeholder="ej: operador1")
            role = st.selectbox("Rol", USER_ROLE_OPTIONS)
            pw1 = st.text_input("Contraseña", type="password")
            pw2 = st.text_input("Repetir contraseña", type="password")
            st.markdown("#### Poderes")
            base = ROLE_TEMPLATES.get(role, ROLE_TEMPLATES["OPERADOR"])
            cols = st.columns(3)
            perms = {}
            keys = list(DEFAULT_PERMS.keys())
            for i, k in enumerate(keys):
                with cols[i % 3]:
                    perms[k] = st.checkbox(k, value=bool(base.get(k, False)), key=f"new_perm_{k}")
            ok = st.form_submit_button("Crear usuario", type="primary", use_container_width=True)

        if ok:
            # Seguridad: si creas un SUPERADMIN o ADMIN, asegúrate de dejar sus poderes correctos
            if (role or "").upper() == "SUPERADMIN":
                perms = SUPERADMIN_PERMS.copy()
            elif (role or "").upper() == "ADMIN":
                perms["manage_users"] = True

            u = (username or "").strip()
            if not u:
                st.error("Usuario requerido.")
                st.stop()
            if not pw1 or pw1 != pw2:
                st.error("Contraseñas no coinciden o están vacías.")
                st.stop()
            try:
                salt_b64, h_b64 = hash_password(pw1)
                execute(
                    "INSERT INTO users(username, salt_b64, pass_hash_b64, role, perms_json, is_active) VALUES(?,?,?,?,?,1)",
                    (u, salt_b64, h_b64, role, json.dumps(perms)),
                )
                auto_backup_db("users_create")
                st.success("Usuario creado.")
                st.rerun()
            except Exception as e:
                msg = str(e).upper()
                if "UNIQUE" in msg:
                    st.error("Ese usuario ya existe.")
                else:
                    st.error(f"No se pudo crear: {e}")

# ----------------------------
# Route
# ----------------------------
p = st.session_state.get("nav_page", "Dashboard")

from segav_core import ops_faenas as _ops_faenas
from segav_core import ops_personal as _ops_personal
from segav_core import ops_docs as _ops_docs
from segav_core import ops_exports as _ops_exports
from segav_core import ops_sgsst as _ops_sgsst
from segav_core import ops_compliance as _ops_compliance
from segav_core import ops_dashboard as _ops_dashboard
from segav_core import ops_superadmin as _ops_superadmin


def page_dashboard():
    return _ops_dashboard.page_dashboard(st=st, ui_header=ui_header, ui_tip=ui_tip, get_global_counts=get_global_counts, fetch_df=fetch_df, fetch_value=fetch_value, DB_BACKEND=DB_BACKEND, conn=conn, execute=execute, PG_DSN_FINGERPRINT=PG_DSN_FINGERPRINT, current_segav_client_key=current_segav_client_key, segav_clientes_df=segav_clientes_df, current_user=current_user, get_empresa_monthly_doc_types=get_empresa_monthly_doc_types, worker_required_docs=worker_required_docs, doc_tipo_label=doc_tipo_label, go=go, clear_app_caches=clear_app_caches)


def page_compliance_alerts():
    return _ops_compliance.page_compliance_alerts(DB_BACKEND=DB_BACKEND, PG_DSN_FINGERPRINT=PG_DSN_FINGERPRINT, conn=conn, execute=execute, fetch_df=fetch_df, fetch_value=fetch_value, clear_app_caches=clear_app_caches, current_segav_client_key=current_segav_client_key, segav_clientes_df=segav_clientes_df, get_empresa_monthly_doc_types=get_empresa_monthly_doc_types, worker_required_docs=worker_required_docs, doc_tipo_label=doc_tipo_label, sgsst_log=sgsst_log)


def page_mandantes():
    return _ops_faenas.page_mandantes(fetch_df=tenant_fetch_df, execute=tenant_execute, auto_backup_db=auto_backup_db)


def page_contratos_faena():
    return _ops_faenas.page_contratos_faena(fetch_df=tenant_fetch_df, execute=tenant_execute, auto_backup_db=auto_backup_db, render_upload_help=render_upload_help, prepare_upload_payload=prepare_upload_payload, save_file_online=save_file_online, sha256_bytes=sha256_bytes, parse_date_maybe=parse_date_maybe, fetch_file_refs=tenant_fetch_file_refs, cleanup_deleted_file_refs=cleanup_deleted_file_refs, load_file_anywhere=load_file_anywhere)


def page_faenas():
    return _ops_faenas.page_faenas(fetch_df=tenant_fetch_df, execute=tenant_execute, auto_backup_db=auto_backup_db, render_upload_help=render_upload_help, prepare_upload_payload=prepare_upload_payload, save_file_online=save_file_online, sha256_bytes=sha256_bytes, parse_date_maybe=parse_date_maybe, validate_faena_dates=validate_faena_dates, fetch_file_refs=tenant_fetch_file_refs, cleanup_deleted_file_refs=cleanup_deleted_file_refs, faena_progress_table=faena_progress_table, ESTADOS_FAENA=ESTADOS_FAENA)


def page_trabajadores():
    return _ops_personal.page_trabajadores(fetch_df=tenant_fetch_df, conn=conn, execute=tenant_execute, auto_backup_db=auto_backup_db, build_trabajadores_template_xlsx=build_trabajadores_template_xlsx, clean_rut=clean_rut, split_nombre_completo=split_nombre_completo, norm_col=norm_col, rut_input=rut_input, segav_cargo_labels=segav_cargo_labels, parse_date_maybe=parse_date_maybe, fetch_file_refs=tenant_fetch_file_refs, cleanup_deleted_file_refs=cleanup_deleted_file_refs, trabajador_insert_or_update=_trabajador_insert_or_update, apply_pending_trabajador_create_reset=_apply_pending_trabajador_create_reset, show_pending_trabajador_create_flash=_show_pending_trabajador_create_flash)


def page_asignar_trabajadores():
    return _ops_personal.page_asignar_trabajadores(fetch_df=tenant_fetch_df, conn=conn, cursor_execute=cursor_execute, ASSIGNACION_INSERT_SQL=ASSIGNACION_INSERT_SQL, clear_app_caches=clear_app_caches, auto_backup_db=auto_backup_db, build_trabajadores_template_xlsx=build_trabajadores_template_xlsx, clean_rut=clean_rut, split_nombre_completo=split_nombre_completo, norm_col=norm_col, executemany=tenant_executemany, go=go, trabajador_insert_or_update=_trabajador_insert_or_update)


def page_documentos_empresa():
    return _ops_docs.page_documentos_empresa(fetch_df=tenant_fetch_df, get_empresa_required_doc_types=get_empresa_required_doc_types, doc_tipo_join=doc_tipo_join, doc_tipo_label=doc_tipo_label, render_upload_help=render_upload_help, prepare_upload_payload=prepare_upload_payload, safe_name=safe_name, save_file_online=save_file_online, sha256_bytes=sha256_bytes, execute=tenant_execute, datetime=datetime, auto_backup_db=auto_backup_db, load_file_anywhere=load_file_anywhere, delete_uploaded_document_record=delete_uploaded_document_record)


def page_documentos_empresa_faena():
    return _ops_docs.page_documentos_empresa_faena(fetch_df=tenant_fetch_df, ui_tip=ui_tip, periodo_label=periodo_label, periodo_ym=periodo_ym, get_empresa_monthly_doc_types=get_empresa_monthly_doc_types, doc_tipo_join=doc_tipo_join, doc_tipo_label=doc_tipo_label, render_upload_help=render_upload_help, prepare_upload_payload=prepare_upload_payload, safe_name=safe_name, save_file_online=save_file_online, sha256_bytes=sha256_bytes, execute=tenant_execute, datetime=datetime, auto_backup_db=auto_backup_db, load_file_anywhere=load_file_anywhere, delete_uploaded_document_record=delete_uploaded_document_record, MESES_ES=MESES_ES)


def page_documentos_trabajador():
    return _ops_personal.page_documentos_trabajador(DB_BACKEND=DB_BACKEND, fetch_df=tenant_fetch_df, fetch_df_uncached=tenant_fetch_df_uncached, execute=tenant_execute, execute_rowcount=tenant_execute_rowcount, auto_backup_db=auto_backup_db, fetch_assigned_workers=fetch_assigned_workers, prepare_upload_payload=prepare_upload_payload, render_upload_help=render_upload_help, save_file_online=save_file_online, sha256_bytes=sha256_bytes, load_file_anywhere=load_file_anywhere, worker_required_docs_for_record=worker_required_docs_for_record, doc_tipo_label=doc_tipo_label, doc_tipo_join=doc_tipo_join, safe_name=safe_name, canonical_cargo_label=canonical_cargo_label, cargo_docs_catalog_rows=cargo_docs_catalog_rows, pendientes_obligatorios=pendientes_obligatorios, delete_uploaded_document_record=delete_uploaded_document_record)


def page_export_zip():
    return _ops_exports.page_export_zip(st=st, ui_header=ui_header, ui_tip=ui_tip, fetch_df=tenant_fetch_df, pendientes_obligatorios=pendientes_obligatorios, pendientes_empresa_faena=pendientes_empresa_faena, doc_tipo_join=doc_tipo_join, export_zip_for_faena=export_zip_for_faena, persist_export=persist_export, auto_backup_db=auto_backup_db, load_file_anywhere=load_file_anywhere, human_file_size=human_file_size, export_zip_for_mes=export_zip_for_mes, persist_export_mes=persist_export_mes, os=os, date=date)


def page_sgsst():
    return _ops_sgsst.page_sgsst(fetch_df=tenant_fetch_df, fetch_value=tenant_fetch_value, execute=tenant_execute, clear_app_caches=clear_app_caches, ensure_sgsst_seed_data=lambda: None, segav_erp_config_map=segav_erp_config_map, segav_clientes_df=segav_clientes_df, current_segav_client_key=current_segav_client_key, segav_cargos_df=segav_cargos_df, get_empresa_required_doc_types=get_empresa_required_doc_types, clean_rut=clean_rut, go=go, segav_templates_df=segav_templates_df, ERP_TEMPLATE_PRESETS=ERP_TEMPLATE_PRESETS, apply_segav_template=apply_segav_template, sgsst_log=sgsst_log, make_erp_key=make_erp_key, segav_erp_value=segav_erp_value, ERP_CLIENT_PARAM_DEFAULTS=ERP_CLIENT_PARAM_DEFAULTS, set_segav_erp_config_value=set_segav_erp_config_value, segav_cliente_params=segav_cliente_params, segav_cargo_labels=segav_cargo_labels, segav_cargo_rules=segav_cargo_rules, DOC_OBLIGATORIOS=DOC_OBLIGATORIOS, DOC_TIPO_LABELS=DOC_TIPO_LABELS, doc_tipo_label=doc_tipo_label, segav_empresa_docs_df=segav_empresa_docs_df, get_empresa_monthly_doc_types=get_empresa_monthly_doc_types, parse_date_maybe=parse_date_maybe, SGSST_NORMAS=SGSST_NORMAS, SGSST_ESTADOS=SGSST_ESTADOS, SGSST_GRAVEDADES=SGSST_GRAVEDADES, SGSST_RESULTADOS=SGSST_RESULTADOS, SGSST_TIPOS_EVENTO=SGSST_TIPOS_EVENTO, SGSST_TIPOS_CAP=SGSST_TIPOS_CAP, doc_tipo_join=doc_tipo_join, current_user=current_user)


def page_superadmin_empresas():
    return _ops_superadmin.page_superadmin_empresas(
        st=st,
        ui_header=ui_header,
        fetch_df=fetch_df,
        fetch_value=fetch_value,
        execute=execute,
        clear_app_caches=clear_app_caches,
        segav_clientes_df=segav_clientes_df,
        visible_clientes_df=visible_clientes_df,
        current_segav_client_key=current_segav_client_key,
        make_erp_key=make_erp_key,
        clean_rut=clean_rut,
        ERP_CLIENT_PARAM_DEFAULTS=ERP_CLIENT_PARAM_DEFAULTS,
        set_segav_erp_config_value=set_segav_erp_config_value,
        sgsst_log=sgsst_log,
        current_user=current_user,
        is_superadmin=is_superadmin,
        ensure_user_client_access_table=lambda: ensure_user_client_access_table_once(DB_BACKEND, PG_DSN_FINGERPRINT),
        fetch_file_refs=fetch_file_refs,
        cleanup_deleted_file_refs=cleanup_deleted_file_refs,
        set_active_cliente_key=lambda key: st.session_state.__setitem__('active_cliente_key', key),
    )


PAGE_PERM_ROUTE = {
    "Dashboard": "view_dashboard",
    "Cumplimiento / Alertas": "view_sgsst",
    "Mi Empresa / SGSST": "view_sgsst",
    "Mandantes": "view_mandantes",
    "Contratos de Faena": "view_contratos",
    "Faenas": "view_faenas",
    "Trabajadores": "view_trabajadores",
    "Documentos Empresa": "view_docs_empresa",
    "Documentos Empresa (Faena)": "view_docs_empresa_faena",
    "Asignar Trabajadores": "view_asignaciones",
    "Documentos Trabajador": "view_docs_trabajador",
    "Export (ZIP)": "view_export",
    "Backup / Restore": "view_backup",
    "Admin Usuarios": "manage_users",
}
if p in PAGE_PERM_ROUTE:
    require_perm(PAGE_PERM_ROUTE[p])

if p == "Dashboard":
    page_dashboard()
elif p == "Cumplimiento / Alertas":
    page_compliance_alerts()
elif p == "Mi Empresa / SGSST":
    page_sgsst()
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
elif p == "SuperAdmin / Empresas":
    if not is_superadmin():
        st.error("Esta sección es exclusiva para SUPERADMIN.")
        st.stop()
    page_superadmin_empresas()
elif p == "Admin Usuarios":
    page_admin_usuarios()
else:
    # Si el estado quedó con un valor inesperado, vuelve a Dashboard
    st.session_state["nav_page"] = "Dashboard"
    st.rerun()
