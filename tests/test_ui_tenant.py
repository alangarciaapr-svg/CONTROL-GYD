import pandas as pd

from segav_core.ui_tenant import (
    allowed_client_keys_for_user,
    client_key_is_visible,
    company_caps_for_user,
    company_role_for_user,
    filter_visible_clientes_df,
    resolve_active_client_key,
    tenant_object_path_allowed,
)


def test_filter_visible_clientes_returns_empty_when_user_has_no_assignments():
    df = pd.DataFrame([
        {"cliente_key": "cli_a", "cliente_nombre": "A", "activo": 1},
        {"cliente_key": "cli_b", "cliente_nombre": "B", "activo": 1},
    ])
    out = filter_visible_clientes_df(df, [], is_superadmin=False)
    assert out.empty


def test_resolve_active_client_key_prefers_authorized_key_then_first_active():
    df = pd.DataFrame([
        {"cliente_key": "cli_a", "cliente_nombre": "A", "activo": 1},
        {"cliente_key": "cli_b", "cliente_nombre": "B", "activo": 1},
    ])
    assert resolve_active_client_key(df, "cli_b", "cli_a") == "cli_b"
    assert resolve_active_client_key(df, "cli_x") == "cli_a"


def test_client_key_is_visible_checks_membership():
    df = pd.DataFrame([
        {"cliente_key": "cli_a", "cliente_nombre": "A", "activo": 1},
    ])
    assert client_key_is_visible(df, "cli_a") is True
    assert client_key_is_visible(df, "cli_b") is False


def test_allowed_client_keys_for_user_uses_assignments():
    def _fetch_df(sql, params):
        assert params == (7,)
        return pd.DataFrame([{"cliente_key": "cli_a"}, {"cliente_key": "cli_b"}])

    assert allowed_client_keys_for_user(_fetch_df, 7, "OPERADOR") == ["cli_a", "cli_b"]
    assert allowed_client_keys_for_user(_fetch_df, 7, "SUPERADMIN") is None



def test_company_role_promotes_company_admin_even_if_role_empresa_is_reader():
    def _fetch_df(sql, params):
        return pd.DataFrame([{"is_company_admin": 1, "role_empresa": "LECTOR"}])

    assert company_role_for_user(_fetch_df, 7, "cli_a", "OPERADOR") == "ADMIN"


def test_company_caps_define_manage_and_delete_permissions():
    def _fetch_df(sql, params):
        return pd.DataFrame([{"is_company_admin": 0, "role_empresa": "OPERADOR"}])

    caps = company_caps_for_user(_fetch_df, 7, "cli_a", "OPERADOR")
    assert caps["can_write"] is True
    assert caps["can_delete_files"] is False
    assert caps["can_manage_users"] is False


def test_tenant_object_path_allowed_requires_matching_prefix():
    assert tenant_object_path_allowed("clientes/cli_a/carpeta/doc.pdf", "cli_a") is True
    assert tenant_object_path_allowed("clientes/cli_b/carpeta/doc.pdf", "cli_a") is False
    assert tenant_object_path_allowed("clientes/cli_b/carpeta/doc.pdf", "cli_a", is_superadmin=True) is True
