from segav_core.db_migrations import apply_runtime_migrations, migration_status


def test_apply_runtime_migrations_emits_schema_and_growth_sql():
    calls = []

    def fake_execute(sql, params=()):
        calls.append((sql, params))

    def fake_fetch_value(_sql, _params=(), default=0):
        return 0

    apply_runtime_migrations(fake_execute, fake_fetch_value, "sqlite")
    joined = "\n".join(sql for sql, _ in calls)
    assert "segav_schema_migrations" in joined
    assert "segav_integrations" in joined
    assert "segav_rule_engine_rules" in joined
    assert "password_must_change" in joined


def test_migration_status_marks_applied_rows():
    def fake_fetch_value(sql, params=(), default=0):
        return 1 if params and str(params[0]).endswith("001_user_security_columns") else 0

    rows = migration_status(fake_fetch_value)
    assert len(rows) >= 3
    assert rows[0]["applied"] is True
    assert any(row["applied"] is False for row in rows[1:])
