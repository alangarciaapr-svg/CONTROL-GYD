from segav_core.tenant_scope import inject_tenant_condition_sql, scope_sql_to_tenant, tenant_scope_target_table


TABLES = ("faenas", "trabajadores")


def test_target_table_detects_update():
    assert tenant_scope_target_table("UPDATE faenas SET nombre=? WHERE id=?", TABLES) == "faenas"


def test_injects_where_when_missing():
    scoped = inject_tenant_condition_sql("SELECT * FROM faenas ORDER BY id DESC", "faenas")
    assert "WHERE COALESCE(faenas.cliente_key,'')=?" in scoped
    assert scoped.endswith("ORDER BY id DESC")


def test_scope_insert_adds_cliente_key():
    sql, params = scope_sql_to_tenant("INSERT INTO faenas(nombre, estado) VALUES(?,?)", ("X", "ACTIVA"), tenant_key="cli_demo", tenant_scope_tables=TABLES)
    assert "cliente_key" in sql
    assert params[0] == "cli_demo"


def test_scope_select_appends_param():
    sql, params = scope_sql_to_tenant("SELECT * FROM faenas WHERE estado=?", ("ACTIVA",), tenant_key="cli_demo", tenant_scope_tables=TABLES)
    assert "cliente_key" in sql
    assert params[-1] == "cli_demo"


def test_scope_insert_many_columns_does_not_corrupt_estado():
    sql, params = scope_sql_to_tenant(
        "INSERT INTO faenas(mandante_id, contrato_faena_id, nombre, ubicacion, fecha_inicio, fecha_termino, estado) VALUES(?,?,?,?,?,?,?)",
        (1, 2, "Bellavista", "Osorno", "2026-05-04", None, "ACTIVA"),
        tenant_key="cli_demo",
        tenant_scope_tables=TABLES,
    )
    assert "fecha_termino, estado) VALUES(?," in sql
    assert "es?, tado" not in sql
    assert "cliente_key, mandante_id" in sql
    assert params[0] == "cli_demo"
    assert params[-1] == "ACTIVA"
