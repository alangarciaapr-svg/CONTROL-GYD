import json

import pandas as pd

from segav_core import backup_restore as br


def test_build_json_backup_and_manifest(tmp_path, monkeypatch):
    def fake_fetch_df(sql):
        if "segav_erp_clientes" in sql:
            return pd.DataFrame([{"cliente_key": "cli_a", "cliente_nombre": "Demo"}])
        return pd.DataFrame()

    monkeypatch.setattr(br, "fetch_df", fake_fetch_df)
    artifact = br.build_json_backup(out_dir=tmp_path, tables=["segav_erp_clientes", "users"])
    manifest = br.build_backup_manifest(artifact, mode="json", tables=["segav_erp_clientes", "users"])

    payload = json.loads(artifact.read_text(encoding="utf-8"))
    meta = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["tables"]["segav_erp_clientes"]["count"] == 1
    assert meta["tables"]["segav_erp_clientes"]["count"] == 1


def test_restore_json_backup_runs_inserts(tmp_path, monkeypatch):
    backup = tmp_path / "sample.json"
    backup.write_text(
        json.dumps(
            {
                "tables": {
                    "segav_erp_clientes": {
                        "rows": [
                            {
                                "cliente_key": "cli_a",
                                "cliente_nombre": "Demo",
                                "rut": "",
                                "vertical": "Forestal",
                                "modo_implementacion": "CONFIGURABLE",
                                "activo": 1,
                                "contacto": "",
                                "email": "",
                                "observaciones": "",
                                "created_at": "2026-04-21T00:00:00+00:00",
                                "updated_at": "2026-04-21T00:00:00+00:00",
                            }
                        ]
                    }
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    executed = []
    monkeypatch.setattr(br, "execute", lambda sql, params=(): executed.append((sql, params)))
    result = br.restore_json_backup(backup)
    assert result["status"] == "restore_completed"
    assert executed and "INSERT INTO segav_erp_clientes" in executed[0][0]
