import os
import re
import sqlite3
import hashlib
import pandas as pd
from segav_core.rut_utils import clean_rut as _segav_clean_rut

try:
    import streamlit as st
except Exception:
    class _FallbackStreamlit:
        session_state = {}
        secrets = {}

        @staticmethod
        def cache_resource(*args, **kwargs):
            def deco(fn):
                fn.clear = lambda: None
                return fn
            return deco

        @staticmethod
        def cache_data(*args, **kwargs):
            def deco(fn):
                fn.clear = lambda: None
                return fn
            return deco

    st = _FallbackStreamlit()

# Manejo seguro de dependencias Postgres (Supabase)
try:
    import psycopg
except ImportError:
    psycopg = None

try:
    from psycopg_pool import ConnectionPool
except ImportError:
    ConnectionPool = None

DB_PATH = "app.db"

def _fingerprint(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:12]

def _get_cfg(name: str, default=None):
    v = os.environ.get(name)
    if v is not None and str(v).strip() != "":
        return v
    try:
        if name in st.secrets:
            return st.secrets.get(name)
    except Exception:
        pass
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

# Configuración y Detección de Backend
raw_pg_dsn = _get_cfg("SUPABASE_DB_URL", _get_cfg("PG_DSN", ""))
PG_DSN = _normalize_pg_dsn(raw_pg_dsn) or _build_pg_dsn_from_parts()
PG_DSN_FINGERPRINT = _fingerprint(PG_DSN) if PG_DSN else "none"
DB_BACKEND_PREF = str(_get_cfg("SEGAV_DB_BACKEND", "postgres") or "postgres").strip().lower()
if DB_BACKEND_PREF not in {"postgres", "sqlite"}:
    DB_BACKEND_PREF = "postgres"
if DB_BACKEND_PREF == "sqlite":
    DB_BACKEND = "sqlite"
elif PG_DSN and psycopg is not None:
    DB_BACKEND = "postgres"
else:
    DB_BACKEND = "sqlite"

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
                f"Detalle: {msg}. Revisa SUPABASE_DB_URL o secretos separados."
            ) from e
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        c.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    return c

def _qmark_to_pct(sql: str) -> str:
    if "?" not in sql:
        return sql
    parts = sql.split("'")
    for i in range(0, len(parts), 2):
        parts[i] = parts[i].replace("?", "%s")
    return "'".join(parts)


def _canonical_rut_for_storage(value):
    if value is None:
        return value
    txt = str(value).strip()
    if not txt:
        return ''
    return _segav_clean_rut(txt) or txt


def _sql_table_name(q: str, verb: str) -> str:
    try:
        if verb == 'insert':
            m = re.search(r"insert\s+into\s+([\w\.\"']+)", q or '', flags=re.I)
        elif verb == 'update':
            m = re.search(r"update\s+([\w\.\"']+)", q or '', flags=re.I)
        else:
            m = None
        if not m:
            return ''
        return m.group(1).replace('"','').replace("'",'').split('.')[-1].strip().lower()
    except Exception:
        return ''


def _sql_clean_col(col: str) -> str:
    return str(col or '').strip().split('.')[-1].replace('"','').replace("'",'').replace('`','').strip().lower()


def _split_sql_csv(txt: str) -> list[str]:
    out, buf, depth, quote = [], [], 0, None
    for ch in str(txt or ''):
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch; buf.append(ch); continue
        if ch == '(':
            depth += 1
        elif ch == ')' and depth > 0:
            depth -= 1
        if ch == ',' and depth == 0:
            out.append(''.join(buf).strip()); buf=[]
        else:
            buf.append(ch)
    if buf:
        out.append(''.join(buf).strip())
    return out


def _rut_column_needs_canonical(table: str, col: str) -> bool:
    col = _sql_clean_col(col)
    table = str(table or '').lower()
    return col in {'rut', 'rut_empresa'} or (table == 'users' and col == 'username')


def _normalize_rut_params_for_sql(q: str, params=()):
    if params is None or isinstance(params, dict) or not isinstance(params, (list, tuple)):
        return params
    p = list(params)
    sql = str(q or '')
    sql_l = sql.lstrip().lower()
    try:
        if sql_l.startswith('insert'):
            table = _sql_table_name(sql, 'insert')
            m = re.search(r"insert\s+into\s+[\w\.\"']+\s*\((.*?)\)\s*values", sql, flags=re.I|re.S)
            if m:
                cols = [_sql_clean_col(c) for c in _split_sql_csv(m.group(1))]
                for i, col in enumerate(cols[:len(p)]):
                    if _rut_column_needs_canonical(table, col):
                        p[i] = _canonical_rut_for_storage(p[i])
        elif sql_l.startswith('update'):
            table = _sql_table_name(sql, 'update')
            m = re.search(r"update\s+[\w\.\"']+\s+set\s+(.*?)(\s+where\s+|$)", sql, flags=re.I|re.S)
            if m:
                param_idx = 0
                for part in _split_sql_csv(m.group(1)):
                    if '?' not in part:
                        continue
                    col = _sql_clean_col(part.split('=')[0])
                    if param_idx < len(p) and _rut_column_needs_canonical(table, col):
                        p[param_idx] = _canonical_rut_for_storage(p[param_idx])
                    param_idx += part.count('?')
    except Exception:
        return params
    return tuple(p) if isinstance(params, tuple) else p

def cursor_execute(cur, q: str, params=()):
    params = _normalize_rut_params_for_sql(q, params)
    if DB_BACKEND == "postgres":
        q = _qmark_to_pct(q).replace("datetime('now')", "now()")
    return cur.execute(q, params)

def migrate_add_columns_if_missing(c, table: str, cols_sql: dict):
    if DB_BACKEND == "postgres":
        return
    info = c.execute(f"PRAGMA table_info({table});").fetchall()
    existing = {row[1] for row in info}
    for col, coltype in cols_sql.items():
        if col not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")

def _is_select_query(q: str) -> bool:
    txt = re.sub(r"/\*.*?\*/", " ", q or "", flags=re.S)
    txt = re.sub(r"--.*?$", " ", txt, flags=re.M).strip().lower()
    return txt.startswith("select") or txt.startswith("with")

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
    except Exception:
        pass

@st.cache_data(ttl=20, show_spinner=False)
def _cached_fetch_df(db_backend: str, dsn_fingerprint: str, q: str, params_cache):
    params = tuple(params_cache) if isinstance(params_cache, tuple) else params_cache
    if db_backend == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            return pd.read_sql_query(q2, c, params=params)
    with conn() as c:
        return pd.read_sql_query(q, c, params=params)

def fetch_df(q: str, params=()):
    params_cache = _cacheable_params(params)
    if _is_select_query(q):
        return _cached_fetch_df(DB_BACKEND, PG_DSN_FINGERPRINT, q, params_cache)
    if DB_BACKEND == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            return pd.read_sql_query(q2, c, params=params)
    with conn() as c:
        return pd.read_sql_query(q, c, params=params)

def fetch_df_uncached(q: str, params=()):
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

def execute(q: str, params=()):
    clear_app_caches()
    params = _normalize_rut_params_for_sql(q, params)
    if DB_BACKEND == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            c.execute(q2, params)
            c.commit()
            return
    with conn() as c:
        c.execute(q, params)
        c.commit()

def execute_rowcount(q: str, params=()):
    clear_app_caches()
    params = _normalize_rut_params_for_sql(q, params)
    if DB_BACKEND == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            cur = c.execute(q2, params)
            c.commit()
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0
    with conn() as c:
        cur = c.execute(q, params)
        c.commit()
        try:
            return int(cur.rowcount or 0)
        except Exception:
            return 0

def executemany(q: str, seq_params):
    clear_app_caches()
    seq_params = [_normalize_rut_params_for_sql(q, p) for p in (seq_params or [])]
    if DB_BACKEND == "postgres":
        q2 = _qmark_to_pct(q).replace("datetime('now')", "now()")
        with conn() as c:
            with c.cursor() as cur:
                cur.executemany(q2, seq_params)
            c.commit()
            return
    with conn() as c:
        c.executemany(q, seq_params)
        c.commit()
