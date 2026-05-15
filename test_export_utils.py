"""SEGAV ERP – Structured Logging.

Provides a centralized logger with rotating file output and optional
console output. Every module should import ``get_logger`` and use a
module-level logger instance::

    from segav_core.logger import get_logger
    log = get_logger(__name__)
    log.info("Faena creada", extra={"faena_id": 42, "user": "admin"})

Log files are written to ``logs/segav_erp.log`` with daily rotation
(keeps 30 days).  The format includes timestamp, level, module, and
message – suitable for ingestion by Datadog, ELK, or any log aggregator.
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_LOG_DIR = os.environ.get("SEGAV_LOG_DIR", "logs")
_LOG_LEVEL = os.environ.get("SEGAV_LOG_LEVEL", "INFO").upper()
_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_MAX_BACKUP_COUNT = 30  # days

_CONFIGURED = False


def _ensure_log_dir() -> str:
    """Create log directory if it doesn't exist."""
    try:
        Path(_LOG_DIR).mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return _LOG_DIR


def _configure_root() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    root = logging.getLogger("segav")
    root.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE_FMT)

    # File handler with daily rotation
    try:
        log_dir = _ensure_log_dir()
        log_path = os.path.join(log_dir, "segav_erp.log")
        fh = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=_MAX_BACKUP_COUNT,
            encoding="utf-8",
        )
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        root.addHandler(fh)
    except OSError:
        # If we can't write to disk (e.g. Streamlit Cloud), skip file handler
        pass

    # Console handler (stderr) – only WARNING+ to avoid Streamlit noise
    ch = logging.StreamHandler(sys.stderr)
    ch.setFormatter(formatter)
    ch.setLevel(logging.WARNING)
    root.addHandler(ch)


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger under the ``segav`` hierarchy."""
    _configure_root()
    if not name.startswith("segav"):
        name = f"segav.{name}"
    return logging.getLogger(name)


# Convenience aliases
def log_action(action: str, entity: str = "", detail: str = "", user: str = "", **kwargs) -> None:
    """High-level action logger for audit-related events."""
    logger = get_logger("segav.actions")
    parts = [action]
    if entity:
        parts.append(f"entity={entity}")
    if detail:
        parts.append(f"detail={detail}")
    if user:
        parts.append(f"user={user}")
    for k, v in kwargs.items():
        parts.append(f"{k}={v}")
    logger.info(" | ".join(parts))


def log_security(event: str, detail: str = "", user: str = "", ip: str = "", **kwargs) -> None:
    """Security-related event logger (login, brute-force, permission denied)."""
    logger = get_logger("segav.security")
    parts = [event]
    if detail:
        parts.append(f"detail={detail}")
    if user:
        parts.append(f"user={user}")
    if ip:
        parts.append(f"ip={ip}")
    for k, v in kwargs.items():
        parts.append(f"{k}={v}")
    logger.warning(" | ".join(parts))


def log_error(context: str, exc: Exception | None = None, **kwargs) -> None:
    """Error logger for exceptions and unexpected conditions."""
    logger = get_logger("segav.errors")
    parts = [context]
    if exc:
        parts.append(f"exc={exc.__class__.__name__}: {exc}")
    for k, v in kwargs.items():
        parts.append(f"{k}={v}")
    logger.error(" | ".join(parts), exc_info=exc is not None)
