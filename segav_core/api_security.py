from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time


def get_api_secret() -> str:
    return os.environ.get("SEGAV_API_SECRET") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "segav-dev-secret-change-me"


def issue_token(payload: dict, ttl_seconds: int = 8 * 3600, secret: str | None = None) -> str:
    body = dict(payload or {})
    body["exp"] = int(time.time()) + int(ttl_seconds)
    raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    sig = hmac.new((secret or get_api_secret()).encode("utf-8"), raw, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=") + "." + base64.urlsafe_b64encode(sig).decode("utf-8").rstrip("=")


def verify_token(token: str, secret: str | None = None) -> dict | None:
    try:
        part_raw, part_sig = str(token or "").split(".", 1)
        raw = base64.urlsafe_b64decode(_pad_b64(part_raw))
        sig = base64.urlsafe_b64decode(_pad_b64(part_sig))
        expected = hmac.new((secret or get_api_secret()).encode("utf-8"), raw, hashlib.sha256).digest()
        if not secrets.compare_digest(sig, expected):
            return None
        payload = json.loads(raw.decode("utf-8"))
        if int(payload.get("exp") or 0) < int(time.time()):
            return None
        return payload
    except Exception:
        return None


def _pad_b64(value: str) -> bytes:
    value = str(value or "")
    padding = "=" * (-len(value) % 4)
    return (value + padding).encode("utf-8")
