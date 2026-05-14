import pandas as pd

from segav_core.compliance_logic import pendientes_empresa_faena_logic, pendientes_obligatorios_logic


class FakeFetch:
    def __init__(self, mapping):
        self.mapping = mapping

    def __call__(self, query, params=()):
        for key, value in self.mapping.items():
            if key in query:
                return value.copy()
        return pd.DataFrame()


def test_pendientes_obligatorios_logic():
    fetch = FakeFetch(
        {
            "FROM asignaciones": pd.DataFrame([
                {"id": 1, "rut": "12.345.678-5", "nombre": "Perez Juan", "cargo": "Chofer"}
            ]),
            "FROM trabajador_documentos": pd.DataFrame([
                {"doc_tipo": "Contrato"}
            ]),
        }
    )

    def rules(cargo):
        assert cargo == "Chofer"
        return ["Contrato", "Licencia"]

    result = pendientes_obligatorios_logic(fetch, rules, 99)
    assert result == {"Perez Juan": ["Licencia"]}


def test_pendientes_empresa_faena_logic():
    fetch = FakeFetch(
        {
            "FROM faena_empresa_documentos": pd.DataFrame([
                {"doc_tipo": "F30"}
            ])
        }
    )
    result = pendientes_empresa_faena_logic(fetch, lambda: ["F30", "F31"], 10)
    assert result == ["F31"]
