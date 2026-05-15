import pandas as pd

from segav_core.ops_compliance import legal_docs_status_summary
from segav_core.ops_exports import _legal_export_blockers


def test_legal_docs_status_summary_counts():
    def fake_fetch_df(query, params=()):
        if 'FROM legal_doc_approvals' in query:
            return pd.DataFrame([
                {"id": 3, "entity_table": "faena_empresa_documentos", "entity_id": 10, "doc_tipo": "F30", "nombre_archivo": "f30.pdf", "criticality": "ALTA", "status": "APROBADO", "legal_status": "APROBADO", "effective_from": "2026-01-01", "expires_at": "2026-01-15", "renewal_period_days": 15, "renewal_status": "VENCIDO", "reviewed_at": "2026-01-01", "updated_at": "2026-01-02"},
                {"id": 2, "entity_table": "trabajador_documentos", "entity_id": 20, "doc_tipo": "ODI", "nombre_archivo": "odi.pdf", "criticality": "CRITICA", "status": "PENDIENTE", "legal_status": "EN_REVISION", "effective_from": "", "expires_at": "2026-12-20", "renewal_period_days": 30, "renewal_status": "POR_VENCER", "reviewed_at": "", "updated_at": "2026-01-02"},
            ])
        return pd.DataFrame()

    out = legal_docs_status_summary(fetch_df=fake_fetch_df, client_key='cli1')
    assert out['criticos_vencidos'] == 1
    assert out['criticos_por_vencer'] == 1
    assert out['criticos_pend_aprob'] == 1
    assert not out['agenda'].empty


def test_export_blockers_for_expired_critical_docs():
    def fake_fetch_df(query, params=()):
        if 'FROM legal_doc_approvals' in query:
            return pd.DataFrame([
                {"id": 9, "entity_table": "FAENA_EMPRESA_DOCUMENTOS", "entity_id": 101, "doc_tipo": "F30", "nombre_archivo": "f30.pdf", "criticality": "ALTA", "legal_status": "APROBADO", "renewal_status": "VENCIDO", "expires_at": "2026-01-15"},
                {"id": 8, "entity_table": "TRABAJADOR_DOCUMENTOS", "entity_id": 202, "doc_tipo": "ODI", "nombre_archivo": "odi.pdf", "criticality": "CRITICA", "legal_status": "EN_REVISION", "renewal_status": "POR_VENCER", "expires_at": "2026-12-20"},
            ])
        if 'FROM faena_empresa_documentos' in query:
            return pd.DataFrame([{"id": 101}])
        if 'FROM trabajador_documentos td' in query:
            return pd.DataFrame([{"id": 202}])
        return pd.DataFrame()

    blockers, warnings = _legal_export_blockers(fake_fetch_df, 55)
    assert len(blockers) == 1
    assert len(warnings) >= 1
