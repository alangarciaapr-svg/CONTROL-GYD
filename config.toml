import pandas as pd
from fastapi.testclient import TestClient

import api_rest


def test_api_list_faenas_respects_tenant(monkeypatch):
    monkeypatch.setattr(api_rest, "verify_token", lambda token: {"sub": 1, "username": "alan", "role": "OPERADOR"})
    monkeypatch.setattr(api_rest, "resolve_tenant_for_user", lambda fetch_df, user, requested_tenant, allow_none_for_superadmin=True: requested_tenant if requested_tenant == "cli_a" else None)

    def fake_fetch_df(sql, params=()):
        if "FROM faenas" in sql:
            tenant = params[0] if params else ""
            if tenant == "cli_a":
                return pd.DataFrame([{"id": 1, "cliente_key": "cli_a", "nombre": "Faena A", "ubicacion": "Osorno", "fecha_inicio": "2026-04-01", "fecha_termino": "", "estado": "ACTIVA"}])
            return pd.DataFrame()
        return pd.DataFrame()

    monkeypatch.setattr(api_rest, "fetch_df", fake_fetch_df)
    monkeypatch.setattr(api_rest, "apply_runtime_migrations", lambda *args, **kwargs: None)

    with TestClient(api_rest.app) as client:
        res = client.get("/api/v1/faenas", headers={"Authorization": "Bearer ok", "x-segav-cliente-key": "cli_a"})
        assert res.status_code == 200
        assert res.json()["count"] == 1


def test_api_update_faena_cross_tenant_returns_404(monkeypatch):
    monkeypatch.setattr(api_rest, "verify_token", lambda token: {"sub": 1, "username": "alan", "role": "OPERADOR"})
    monkeypatch.setattr(api_rest, "resolve_tenant_for_user", lambda fetch_df, user, requested_tenant, allow_none_for_superadmin=True: requested_tenant)
    monkeypatch.setattr(api_rest, "execute_rowcount", lambda sql, params=(): 0)
    monkeypatch.setattr(api_rest, "apply_runtime_migrations", lambda *args, **kwargs: None)

    with TestClient(api_rest.app) as client:
        res = client.put(
            "/api/v1/faenas/99",
            headers={"Authorization": "Bearer ok", "x-segav-cliente-key": "cli_a"},
            json={"nombre": "X", "ubicacion": "", "fecha_inicio": "2026-04-01", "fecha_termino": None, "estado": "ACTIVA", "mandante_id": None, "contrato_faena_id": None},
        )
        assert res.status_code == 404


def test_api_superadmin_readiness(monkeypatch):
    monkeypatch.setattr(api_rest, "verify_token", lambda token: {"sub": 1, "username": "root", "role": "SUPERADMIN"})
    monkeypatch.setattr(api_rest, "fetch_value", lambda sql, params=(), default=0: 1)
    monkeypatch.setattr(api_rest, "apply_runtime_migrations", lambda *args, **kwargs: None)

    with TestClient(api_rest.app) as client:
        res = client.get("/api/v1/admin/production/readiness", headers={"Authorization": "Bearer ok"})
        assert res.status_code == 200
        assert res.json()["requested_by"] == "root"
