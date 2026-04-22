from __future__ import annotations

import re


CLAUSE_TERMINATORS = (" order by ", " group by ", " limit ", " union ")


def tenant_scope_target_table(sql: str, tenant_scope_tables) -> str | None:
    q = str(sql or "")
    depth = 0
    outer = []
    for ch in q:
        if ch == '(':
            depth += 1
            outer.append(' ')
        elif ch == ')':
            depth = max(0, depth - 1)
            outer.append(' ')
        elif depth == 0:
            outer.append(ch)
        else:
            outer.append(' ')
    main_sql = ''.join(outer)
    patterns = [
        r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"\bDELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"\bINSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)",
    ]
    scope_tables = set(tenant_scope_tables or [])
    for patt in patterns:
        m = re.search(patt, main_sql, flags=re.I)
        if m:
            table = str(m.group(1) or '').strip()
            if table in scope_tables:
                return table
    return None


def inject_tenant_condition_sql(sql: str, alias_or_table: str) -> str:
    tenant_cond = f"COALESCE({alias_or_table}.cliente_key,'')=?"
    depth = 0
    outer_mask = []
    for ch in sql:
        if ch == '(':
            depth += 1
            outer_mask.append(' ')
        elif ch == ')':
            depth = max(0, depth - 1)
            outer_mask.append(' ')
        elif depth == 0:
            outer_mask.append(ch)
        else:
            outer_mask.append(' ')
    outer = ''.join(outer_mask)
    lower_outer = outer.lower()
    clause_positions = [p for p in [lower_outer.find(x) for x in CLAUSE_TERMINATORS] if p != -1]
    cut = min(clause_positions) if clause_positions else len(sql)
    head = sql[:cut]
    tail = sql[cut:]
    outer_head = outer[:cut].lower()
    if re.search(r"\bwhere\b", outer_head, flags=re.I):
        return head + f" AND {tenant_cond}" + tail
    return head + f" WHERE {tenant_cond}" + tail


def scope_sql_to_tenant(sql: str, params=(), tenant_key: str | None = None, tenant_scope_tables=None):
    tenant_key = str(tenant_key or '').strip()
    q = str(sql or '')
    if not tenant_key or not q.strip():
        return q, tuple(params or ())
    if 'cliente_key' in q.lower():
        return q, tuple(params or ())
    table = tenant_scope_target_table(q, tenant_scope_tables or ())
    if not table:
        return q, tuple(params or ())

    params_t = tuple(params or ())
    m_ins = re.search(r"(\bINSERT\s+INTO\s+" + re.escape(table) + r"\s*\()([^)]*)(\)\s*VALUES\s*\()", q, flags=re.I | re.S)
    if m_ins:
        cols_txt = m_ins.group(2)
        cols = [c.strip() for c in cols_txt.split(',') if c.strip()]
        if not any(c.lower() == 'cliente_key' for c in cols):
            new_cols = 'cliente_key, ' + cols_txt.strip()
            start, end = m_ins.span(2)
            q2 = q[:start] + new_cols + q[end:]
            val_start = m_ins.end(3)
            q2 = q2[:val_start] + '?, ' + q2[val_start:]
            return q2, (tenant_key, *params_t)
        return q, params_t

    depth = 0
    outer_chars = []
    for ch in q:
        if ch == '(':
            depth += 1
            outer_chars.append(' ')
        elif ch == ')':
            depth = max(0, depth - 1)
            outer_chars.append(' ')
        elif depth == 0:
            outer_chars.append(ch)
        else:
            outer_chars.append(' ')
    outer_q = ''.join(outer_chars)
    m_root = re.search(r"\b(FROM|UPDATE|DELETE\s+FROM)\s+" + re.escape(table) + r"(?:\s+([A-Za-z_][A-Za-z0-9_]*))?", outer_q, flags=re.I)
    alias = table
    if m_root:
        alias_candidate = str(m_root.group(2) or '').strip()
        if alias_candidate and alias_candidate.upper() not in {'SET', 'WHERE', 'ORDER', 'GROUP', 'LIMIT', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'ON'}:
            alias = alias_candidate
    q2 = inject_tenant_condition_sql(q, alias)
    return q2, (*params_t, tenant_key)
