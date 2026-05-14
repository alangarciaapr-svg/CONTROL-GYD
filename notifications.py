from __future__ import annotations

import re


RUT_CLEAN_RE = re.compile(r"[^0-9kK]")


def rut_parts(rut: str) -> tuple[str, str]:
    txt = RUT_CLEAN_RE.sub("", str(rut or "")).strip()
    if len(txt) < 2:
        return "", ""
    return txt[:-1], txt[-1].lower()


def format_rut_chileno(rut: str) -> str:
    body, dv = rut_parts(rut)
    if not body:
        return ""
    rev = body[::-1]
    grouped = ".".join(rev[i : i + 3] for i in range(0, len(rev), 3))[::-1]
    return f"{grouped}-{dv}" if dv else grouped


def validate_rut_dv(rut: str) -> bool:
    body, dv = rut_parts(rut)
    if not body or not dv:
        return False
    try:
        n = int(body)
    except ValueError:
        return False
    mult, total = 2, 0
    while n:
        total += (n % 10) * mult
        n //= 10
        mult = 2 if mult == 7 else mult + 1
    expected = 11 - (total % 11)
    if expected == 11:
        expected_dv = "0"
    elif expected == 10:
        expected_dv = "k"
    else:
        expected_dv = str(expected)
    return dv.lower() == expected_dv


def clean_rut(rut: str) -> str:
    return format_rut_chileno(rut)
