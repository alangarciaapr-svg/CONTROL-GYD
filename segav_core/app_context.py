"""SEGAV ERP – Application Context (Service Container).

Instead of passing 20-40 keyword arguments to every ops module function,
modules can receive a single ``AppContext`` instance that holds all shared
services and utilities.

Usage in an ops module::

    def page_mi_perfil(ctx: AppContext):
        u = ctx.current_user()
        df = ctx.fetch_df("SELECT * FROM users WHERE id=?", (u['id'],))
        ...

The context is built once per request cycle in ``streamlit_app.py`` and
passed down.  Existing ops modules continue to work with kwargs – this is
additive, not a replacement of the old pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class AppContext:
    """Central service container for SEGAV ERP modules.

    Attributes mirror the most commonly injected dependencies.  New modules
    should accept ``ctx: AppContext`` instead of 20+ kwargs.
    """

    # --- Database layer ---
    db_backend: str = "sqlite"
    pg_dsn_fingerprint: str = ""
    conn: Callable | None = None
    execute: Callable | None = None
    execute_rowcount: Callable | None = None
    executemany: Callable | None = None
    fetch_df: Callable | None = None
    fetch_df_uncached: Callable | None = None
    fetch_value: Callable | None = None
    fetch_row: Callable | None = None
    clear_app_caches: Callable | None = None
    auto_backup_db: Callable | None = None

    # --- Tenant-scoped DB ---
    tenant_fetch_df: Callable | None = None
    tenant_fetch_df_uncached: Callable | None = None
    tenant_fetch_value: Callable | None = None
    tenant_execute: Callable | None = None
    tenant_execute_rowcount: Callable | None = None
    tenant_executemany: Callable | None = None

    # --- Auth / User ---
    current_user: Callable | None = None
    current_tenant_key: Callable | None = None
    current_segav_client_key: Callable | None = None
    is_superadmin: Callable | None = None
    has_perm: Callable | None = None
    is_company_admin_for_active_tenant: Callable | None = None
    current_user_mandante_scope_ids: Callable | None = None

    # --- UI helpers ---
    ui_header: Callable | None = None
    ui_tip: Callable | None = None
    go: Callable | None = None

    # --- File / Storage ---
    load_file_anywhere: Callable | None = None
    save_file_online: Callable | None = None
    prepare_upload_payload: Callable | None = None
    render_upload_help: Callable | None = None
    sha256_bytes: Callable | None = None
    delete_uploaded_document_record: Callable | None = None
    fetch_file_refs: Callable | None = None
    cleanup_deleted_file_refs: Callable | None = None

    # --- Formatting / RUT ---
    clean_rut: Callable | None = None
    format_rut_chileno: Callable | None = None
    safe_name: Callable | None = None
    parse_date_maybe: Callable | None = None
    human_file_size: Callable | None = None

    # --- Docs / Compliance ---
    doc_tipo_label: Callable | None = None
    doc_tipo_join: Callable | None = None
    worker_required_docs: Callable | None = None
    worker_required_docs_for_record: Callable | None = None
    get_empresa_required_doc_types: Callable | None = None
    get_empresa_monthly_doc_types: Callable | None = None
    render_legal_doc_inline: Callable | None = None
    pendientes_obligatorios: Callable | None = None

    # --- Catalogs / Config ---
    segav_clientes_df: Callable | None = None
    visible_clientes_df: Callable | None = None
    segav_cargo_labels: Callable | None = None

    # --- Audit ---
    audit_log: Callable | None = None

    # --- Notifications ---
    notify: Callable | None = None

    # --- Extra (open dict for module-specific needs) ---
    extra: dict = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-like access for dynamic lookups."""
        return getattr(self, key, self.extra.get(key, default))

    def set(self, key: str, value: Any) -> None:
        """Set an attribute dynamically."""
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            self.extra[key] = value
