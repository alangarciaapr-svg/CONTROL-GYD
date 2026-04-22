import pandas as pd

from segav_core.api_tenant import resolve_tenant_for_user


def test_resolve_tenant_for_regular_user_uses_allowed_access():
    def fake_fetch_df(sql, params=()):
        return pd.DataFrame({"cliente_key": ["cli_a", "cli_b"]})

    user = {"sub": 10, "role": "OPERADOR"}
    assert resolve_tenant_for_user(fake_fetch_df, user, "cli_b") == "cli_b"
    assert resolve_tenant_for_user(fake_fetch_df, user, None) == "cli_a"
    assert resolve_tenant_for_user(fake_fetch_df, user, "cli_x") is None
