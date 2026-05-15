import pandas as pd
from segav_core.module_perms import effective_company_perms

def test_effective_company_perms_admin_can_approve():
    def fetch_df(q, params):
        return pd.DataFrame()
    perms = effective_company_perms(fetch_df, 2, 'empresa1', 'OPERADOR', {'approve_legal_docs': False, 'view_legal_audit': False, 'view_dashboard': True}, ['approve_legal_docs','view_legal_audit','view_dashboard'], 'ADMIN')
    assert perms['approve_legal_docs'] is True
    assert perms['view_legal_audit'] is True

def test_effective_company_perms_overrides_apply():
    def fetch_df(q, params):
        return pd.DataFrame([{'perms_json': '{"view_dashboard": false, "approve_legal_docs": true}'}])
    perms = effective_company_perms(fetch_df, 2, 'empresa1', 'OPERADOR', {'approve_legal_docs': False, 'view_legal_audit': False, 'view_dashboard': True}, ['approve_legal_docs','view_legal_audit','view_dashboard'], 'LECTOR')
    assert perms['approve_legal_docs'] is True
    assert perms['view_dashboard'] is False
