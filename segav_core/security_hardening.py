from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC = timezone.utc
MAX_LOGIN_FAILURES = 5
LOCK_MINUTES = 15


def _now(now: datetime | None = None) -> datetime:
    return now.astimezone(UTC) if now else datetime.now(UTC)


def ensure_user_security_columns(execute, db_backend: str):
    backend = str(db_backend or "sqlite").lower()
    if backend == "postgres":
        stmts = [
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS password_must_change BIGINT NOT NULL DEFAULT 0;",
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS password_changed_at TIMESTAMPTZ;",
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS failed_login_attempts BIGINT NOT NULL DEFAULT 0;",
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;",
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;",
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS last_login_ip TEXT;",
            "ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS force_password_reset_reason TEXT;",
        ]
        for stmt in stmts:
            execute(stmt)
        return

    stmts = [
        "ALTER TABLE users ADD COLUMN password_must_change INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE users ADD COLUMN password_changed_at TEXT;",
        "ALTER TABLE users ADD COLUMN failed_login_attempts INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE users ADD COLUMN locked_until TEXT;",
        "ALTER TABLE users ADD COLUMN last_login_at TEXT;",
        "ALTER TABLE users ADD COLUMN last_login_ip TEXT;",
        "ALTER TABLE users ADD COLUMN force_password_reset_reason TEXT;",
    ]
    for stmt in stmts:
        try:
            execute(stmt)
        except Exception:
            pass


def is_password_strong(password: str) -> tuple[bool, str]:
    pwd = str(password or "")
    if len(pwd) < 10:
        return False, "La contraseña debe tener al menos 10 caracteres."
    if not any(c.islower() for c in pwd):
        return False, "La contraseña debe incluir una minúscula."
    if not any(c.isupper() for c in pwd):
        return False, "La contraseña debe incluir una mayúscula."
    if not any(c.isdigit() for c in pwd):
        return False, "La contraseña debe incluir un número."
    if not any(not c.isalnum() for c in pwd):
        return False, "La contraseña debe incluir un símbolo."
    return True, "ok"


def next_failure_state(current_failures: int, now: datetime | None = None) -> tuple[int, str | None]:
    failures = int(current_failures or 0) + 1
    when = _now(now)
    if failures >= MAX_LOGIN_FAILURES:
        return failures, (when + timedelta(minutes=LOCK_MINUTES)).isoformat(timespec="seconds")
    return failures, None


def is_account_locked(row: dict, now: datetime | None = None) -> tuple[bool, str | None]:
    value = str((row or {}).get("locked_until") or "").strip()
    if not value:
        return False, None
    when = _now(now)
    try:
        locked_until = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return False, None
    if locked_until.tzinfo is None:
        locked_until = locked_until.replace(tzinfo=UTC)
    else:
        locked_until = locked_until.astimezone(UTC)
    return locked_until > when, locked_until.isoformat(timespec="seconds")


def reset_security_state_payload(now: datetime | None = None) -> dict:
    when = _now(now)
    return {
        "failed_login_attempts": 0,
        "locked_until": None,
        "last_login_at": when.isoformat(timespec="seconds"),
    }
