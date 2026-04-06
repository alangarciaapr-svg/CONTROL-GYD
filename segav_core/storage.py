from __future__ import annotations

import io
import hashlib
import os
import re
import zipfile
from urllib.parse import quote
import unicodedata

import requests
import streamlit as st

from core_db import DB_BACKEND, _get_cfg, fetch_df, execute, migrate_add_columns_if_missing
from segav_core.app_config import UPLOAD_ROOT, MAX_UPLOAD_FILE_BYTES, UPLOAD_HELP_TEXT

STORAGE_URL = (_get_cfg("SUPABASE_URL", "") or "").rstrip("/")
STORAGE_BUCKET = str(_get_cfg("SUPABASE_STORAGE_BUCKET", "docs") or "docs")
STORAGE_SERVICE_KEY = str(_get_cfg("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
STORAGE_ANON_KEY = str(_get_cfg("SUPABASE_ANON_KEY", "") or "").strip()
STORAGE_TIMEOUT = 30

FILE_REF_TABLES = [
    "contratos_faena",
    "faena_anexos",
    "trabajador_documentos",
    "empresa_documentos",
    "faena_empresa_documentos",
    "export_historial",
    "export_historial_mes",
]

DOCUMENT_TABLES = {
    "trabajador_documentos",
    "empresa_documentos",
    "faena_empresa_documentos",
}


def ensure_dirs_local():
    os.makedirs(UPLOAD_ROOT, exist_ok=True)
    os.makedirs(os.path.join(UPLOAD_ROOT, "exports"), exist_ok=True)
    os.makedirs(os.path.join(UPLOAD_ROOT, "auto_backups"), exist_ok=True)


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


def _is_jwt(token: str) -> bool:
    t = (token or "").strip()
    return (t.startswith("eyJ") and t.count(".") >= 2 and " " not in t)


def _is_publishable_key(token: str) -> bool:
    t = (token or "").strip()
    return t.startswith("sb_publishable_")


def storage_enabled() -> bool:
    return bool(STORAGE_URL and STORAGE_BUCKET and (STORAGE_SERVICE_KEY or STORAGE_ANON_KEY))


def storage_admin_enabled() -> bool:
    key = (STORAGE_SERVICE_KEY or "").strip()
    return bool(STORAGE_URL and STORAGE_BUCKET and key and not _is_publishable_key(key))


def _encode_storage_path(op: str) -> str:
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
        key = (STORAGE_SERVICE_KEY or "").strip() or (STORAGE_ANON_KEY or "").strip()
    h = {"Accept": "application/json"}
    if _is_jwt(key):
        h["Authorization"] = f"Bearer {key}"
        h["apikey"] = key
    else:
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
        return any(m in body for m in ["already exists", "asset already exists", "duplicate", "conflict", "exists"])
    return False


def storage_upload(object_path: str, data: bytes, content_type: str = "application/octet-stream", upsert: bool = True):
    op = _encode_storage_path(object_path)
    if not op:
        raise RuntimeError("Ruta de Storage inválida.")
    url = f"{STORAGE_URL}/storage/v1/object/{STORAGE_BUCKET}/{op}"
    attempts = []
    http = get_http_session()
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
    for url in urls:
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
        last_resp = resp
    if last_resp is not None and last_resp.status_code == 404:
        raise FileNotFoundError("Archivo no encontrado en Storage.")
    if last_resp is not None:
        _storage_set_last_error(last_resp, url=urls[-1], method="storage_download")
        raise RuntimeError(f"Storage download failed (HTTP {last_resp.status_code}): {_storage_error_summary(last_resp)}")
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


def render_upload_help():
    st.caption("💡 " + UPLOAD_HELP_TEXT)


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
                f"({raw_size} bytes → {zip_size} bytes)."
            ),
        })
        return payload
    st.error(
        f"El límite de carga por archivo es de 1,5 MB. El archivo pesa {raw_size} bytes y aun comprimido queda en {zip_size} bytes. Reduce el tamaño antes de cargarlo."
    )
    st.stop()


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _safe_path_parts(parts):
    safe = []
    for part in (parts or []):
        txt = str(part or "").strip().replace("\\", "/")
        for chunk in [c for c in txt.split("/") if c]:
            safe.append(storage_safe_segment(chunk))
    return safe


def save_file(folder_parts, file_name: str, file_bytes: bytes):
    ensure_dirs_local()
    folder = os.path.join(UPLOAD_ROOT, *_safe_path_parts(folder_parts))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, storage_safe_segment(file_name))
    with open(path, "wb") as f:
        f.write(file_bytes)
    return path


def save_file_online(folder_parts, file_name: str, file_bytes: bytes, content_type: str = "application/octet-stream"):
    local_path = save_file(folder_parts, file_name, file_bytes)
    object_path = _storage_object_path(folder_parts, file_name)
    bucket = STORAGE_BUCKET if storage_admin_enabled() else None
    if storage_admin_enabled():
        try:
            storage_upload(object_path, file_bytes, content_type=content_type, upsert=True)
        except Exception:
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
            pass
    if file_path and os.path.exists(str(file_path)):
        with open(str(file_path), "rb") as fp:
            return fp.read()
    raise FileNotFoundError("Archivo no disponible (ni Storage ni disco local).")


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


def _local_delete_file(file_path: str | None):
    path = str(file_path or "").strip()
    if not path:
        return False
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


def _count_file_references(file_path: str | None, bucket: str | None, object_path: str | None, *, exclude_table: str | None = None, exclude_id: int | None = None) -> int:
    total = 0
    for tbl in FILE_REF_TABLES:
        where = []
        params = []
        if object_path:
            where.append("object_path=?")
            params.append(str(object_path))
            if bucket:
                where.append("bucket=?")
                params.append(str(bucket))
        elif file_path:
            where.append("file_path=?")
            params.append(str(file_path))
        else:
            continue
        if exclude_table == tbl and exclude_id is not None:
            where.append("id<>?")
            params.append(int(exclude_id))
        q = f"SELECT COUNT(*) AS n FROM {tbl} WHERE " + " AND ".join(where)
        df = fetch_df(q, tuple(params))
        if not df.empty:
            total += int(df.iloc[0]["n"] or 0)
    return int(total)


def cleanup_file_assets(file_path: str | None, bucket: str | None, object_path: str | None):
    issues = []
    if object_path and storage_admin_enabled():
        try:
            storage_delete(str(object_path))
        except FileNotFoundError:
            pass
        except Exception as e:
            issues.append(f"Storage: {e}")
    try:
        _local_delete_file(file_path)
    except Exception as e:
        issues.append(f"Local: {e}")
    return issues


def fetch_file_refs(table_name: str, where_sql: str = "", params=()):
    q = f"SELECT file_path, bucket, object_path FROM {table_name}"
    if where_sql:
        q += f" WHERE {where_sql}"
    df = fetch_df(q, params)
    if df.empty:
        return []
    return [row.to_dict() for _, row in df.iterrows()]


def cleanup_deleted_file_refs(file_refs):
    issues = []
    seen = set()
    for ref in (file_refs or []):
        file_path = ref.get("file_path")
        bucket = ref.get("bucket")
        object_path = ref.get("object_path")
        key = (str(file_path or ""), str(bucket or ""), str(object_path or ""))
        if key in seen:
            continue
        seen.add(key)
        if _count_file_references(file_path, bucket, object_path) == 0:
            issues.extend(cleanup_file_assets(file_path, bucket, object_path))
    return issues


def delete_uploaded_document_record(table_name: str, row_id: int):
    if table_name not in DOCUMENT_TABLES:
        raise ValueError("Tabla no permitida para eliminación.")
    df = fetch_df(f"SELECT id, nombre_archivo, file_path, bucket, object_path FROM {table_name} WHERE id=?", (int(row_id),))
    if df.empty:
        raise FileNotFoundError("El documento ya no existe en la base de datos.")
    row = df.iloc[0]
    file_path = row.get("file_path", None)
    bucket = row.get("bucket", None)
    object_path = row.get("object_path", None)
    file_name = row.get("nombre_archivo", "documento")
    refs = _count_file_references(file_path, bucket, object_path, exclude_table=table_name, exclude_id=int(row_id))
    execute(f"DELETE FROM {table_name} WHERE id=?", (int(row_id),))
    cleanup_issues = []
    if refs == 0:
        cleanup_issues = cleanup_file_assets(file_path, bucket, object_path)
    return {"file_name": file_name, "cleanup_issues": cleanup_issues, "shared_refs": refs}
