from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from segav_core.security_hardening import ensure_user_security_columns


@dataclass(frozen=True)
class Migration:
    version_key: str
    description: str
    apply_fn: Callable


def builtin_migrations(ensure_growth_tables, seed_builtin_templates) -> list[Migration]:
    return [
        Migration(
            version_key="2026.04.21.001_user_security_columns",
            description="Asegura columnas de seguridad en users para endurecimiento de acceso.",
            apply_fn=lambda execute, fetch_value, db_backend: ensure_user_security_columns(execute, db_backend),
        ),
        Migration(
            version_key="2026.04.21.002_growth_tables",
            description="Crea tablas base de integraciones, reglas, firma, móvil y marketplace.",
            apply_fn=lambda execute, fetch_value, db_backend: ensure_growth_tables(execute, db_backend),
        ),
        Migration(
            version_key="2026.04.21.003_builtin_templates",
            description="Carga plantillas base por rubro para expansión comercial.",
            apply_fn=lambda execute, fetch_value, db_backend: seed_builtin_templates(execute, fetch_value),
        ),
    ]
