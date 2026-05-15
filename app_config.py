import re
import unicodedata
from .catalogs import MESES_ES


def safe_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "item"


def human_file_size(num_bytes: int) -> str:
    size = float(max(int(num_bytes or 0), 0))
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} GB"


def normalize_text(value) -> str:
    s = str(value or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.strip().lower()


def make_erp_key(value: str, prefix: str = "") -> str:
    base = normalize_text(value)
    base = re.sub(r"[^a-z0-9]+", "_", base).strip("_") or "item"
    return f"{prefix}{base}" if prefix else base


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
