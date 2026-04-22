from __future__ import annotations

import io
import json
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from core_db import DB_BACKEND, execute, execute_rowcount, fetch_df, fetch_value
from segav_core.api_security import issue_token, verify_token
from segav_core.api_tenant import TENANT_HEADER, audit_api_action, resolve_tenant_for_user, visible_clientes_sql
from segav_core.auth import hash_password, perms_from_row, verify_password
from segav_core.db_migrations import apply_runtime_migrations, migration_status
from segav_core.prod_readiness import data_readiness, environment_readiness, summarize_readiness
from segav_core.security_hardening import is_account_locked, is_password_strong, next_failure_state, reset_security_state_payload

try:
    from streamlit_app import export_zip_for_faena
except Exception:
    export_zip_for_faena = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    apply_runtime_migrations(execute, fetch_value, DB_BACKEND)
    yield


app = FastAPI(title="SEGAV ERP API", version="1.3.0", lifespan=lifespan)


class LoginPayload(BaseModel):
    username: str
    password: str


class FaenaPayload(BaseModel):
    nombre: str
    ubicacion: str = ""
    fecha_inicio: str
    fecha_termino: Optional[str] = None
    estado: str = "ACTIVA"
    mandante_id: Optional[int] = None
    contrato_faena_id: Optional[int] = None


class TrabajadorPayload(BaseModel):
    rut: str
    nombres: str
    apellidos: str
    cargo: str = ""
    email: str = ""
    centro_costo: str = ""
    fecha_contrato: Optional[str] = None
    vigencia_examen: Optional[str] = None


class IntegrationEventPayload(BaseModel):
    integration_type: str = Field(default="MUTUALIDAD")
    event_type: str
    payload: dict = Field(default_factory=dict)


class RulePayload(BaseModel):
    rule_name: str
    target_scope: str
    severity: str = "MEDIA"
    rule_json: dict = Field(default_factory=dict)
    is_active: bool = True


@app.get("/health")
def health():
    return {"status": "ok", "service": "SEGAV ERP API", "db_backend": DB_BACKEND, "version": app.version}


@app.get("/api/v1/health/deep")
def deep_health():
    try:
        clients = int(fetch_value("SELECT COUNT(*) FROM segav_erp_clientes", default=0) or 0)
        users = int(fetch_value("SELECT COUNT(*) FROM users", default=0) or 0)
        return {"status": "ok", "db_backend": DB_BACKEND, "clientes": clients, "users": users}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB error: {exc}")


@app.post("/api/v1/auth/login")
def login(payload: LoginPayload, request: Request):
    df = fetch_df(
        "SELECT id, username, salt_b64, pass_hash_b64, role, perms_json, is_active, password_must_change, failed_login_attempts, locked_until "
        "FROM users WHERE username=? LIMIT 1",
        (str(payload.username or "").strip(),),
    )
    if df is None or df.empty:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    row = df.iloc[0]
    if int(row.get("is_active") or 0) != 1:
        raise HTTPException(status_code=403, detail="Usuario inactivo")
    locked, locked_until = is_account_locked(row.to_dict())
    if locked:
        raise HTTPException(status_code=423, detail=f"Cuenta bloqueada hasta {locked_until}")
    if not verify_password(payload.password, str(row.get("salt_b64") or ""), str(row.get("pass_hash_b64") or "")):
        failures, lock_until = next_failure_state(int(row.get("failed_login_attempts") or 0))
        execute("UPDATE users SET failed_login_attempts=?, locked_until=?, updated_at=datetime('now') WHERE id=?", (failures, lock_until, int(row.get("id") or 0)))
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    reset = reset_security_state_payload()
    client_ip = request.client.host if request.client else ""
    execute(
        "UPDATE users SET failed_login_attempts=?, locked_until=?, last_login_at=?, last_login_ip=?, updated_at=datetime('now') WHERE id=?",
        (reset["failed_login_attempts"], reset["locked_until"], reset["last_login_at"], client_ip, int(row.get("id") or 0)),
    )
    perms = perms_from_row(str(row.get("role") or "OPERADOR"), row.get("perms_json"))
    token = issue_token(
        {
            "sub": int(row.get("id") or 0),
            "username": str(row.get("username") or ""),
            "role": str(row.get("role") or "OPERADOR"),
            "perms": perms,
            "password_must_change": bool(int(row.get("password_must_change") or 0)),
        }
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": int(row.get("id") or 0),
            "username": str(row.get("username") or ""),
            "role": str(row.get("role") or "OPERADOR"),
            "password_must_change": bool(int(row.get("password_must_change") or 0)),
        },
    }


def require_user(authorization: Optional[str] = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Falta token bearer")
    token = authorization.split(" ", 1)[1].strip()
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")
    return payload


def require_tenant(user=Depends(require_user), x_segav_cliente_key: Optional[str] = Header(default=None, alias=TENANT_HEADER)):
    tenant = resolve_tenant_for_user(fetch_df, user, x_segav_cliente_key)
    if str(user.get("role") or "").upper() != "SUPERADMIN" and not tenant:
        raise HTTPException(status_code=403, detail="Usuario sin acceso a empresa o falta header x-segav-cliente-key")
    return {"user": user, "tenant": tenant}


def require_superadmin(user=Depends(require_user)):
    if str(user.get("role") or "").upper() != "SUPERADMIN":
        raise HTTPException(status_code=403, detail="Solo SUPERADMIN puede acceder a este endpoint")
    return user


def _fetch_one_with_tenant(table: str, item_id: int, tenant: str | None, columns: str):
    sql = f"SELECT {columns} FROM {table} WHERE id=?"
    params = [int(item_id)]
    if tenant:
        sql += " AND COALESCE(cliente_key,'')=?"
        params.append(tenant)
    df = fetch_df(sql, tuple(params))
    if df is None or df.empty:
        return None
    return df.iloc[0].fillna("").to_dict()


@app.get("/api/v1/me")
def me(ctx=Depends(require_tenant)):
    return {"user": ctx["user"], "tenant": ctx["tenant"]}


@app.post("/api/v1/auth/change-password")
def change_password(payload: LoginPayload, new_password: str = Query(..., min_length=10), user=Depends(require_user)):
    ok, msg = is_password_strong(new_password)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    df = fetch_df("SELECT id, salt_b64, pass_hash_b64 FROM users WHERE id=? LIMIT 1", (int(user.get("sub") or 0),))
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    row = df.iloc[0]
    if not verify_password(payload.password, str(row.get("salt_b64") or ""), str(row.get("pass_hash_b64") or "")):
        raise HTTPException(status_code=401, detail="Contraseña actual inválida")
    salt_b64, h_b64 = hash_password(new_password)
    execute(
        "UPDATE users SET salt_b64=?, pass_hash_b64=?, password_must_change=0, password_changed_at=datetime('now'), force_password_reset_reason=NULL, updated_at=datetime('now') WHERE id=?",
        (salt_b64, h_b64, int(user.get("sub") or 0)),
    )
    return {"status": "ok"}


@app.get("/api/v1/clientes")
def list_clientes(user=Depends(require_user)):
    role = str(user.get("role") or "OPERADOR")
    sql = visible_clientes_sql(role)
    params = () if role.upper() == "SUPERADMIN" else (int(user.get("sub") or 0),)
    df = fetch_df(sql, params)
    items = [] if df is None or df.empty else df.fillna("").to_dict(orient="records")
    return {"items": items, "count": len(items), "requested_by": user.get("username")}


@app.get("/api/v1/faenas")
def list_faenas(ctx=Depends(require_tenant)):
    tenant = ctx["tenant"]
    user = ctx["user"]
    if tenant:
        df = fetch_df("SELECT id, cliente_key, nombre, ubicacion, fecha_inicio, fecha_termino, estado FROM faenas WHERE COALESCE(cliente_key,'')=? ORDER BY id DESC", (tenant,))
    else:
        df = fetch_df("SELECT id, cliente_key, nombre, ubicacion, fecha_inicio, fecha_termino, estado FROM faenas ORDER BY id DESC")
    items = [] if df is None or df.empty else df.fillna("").to_dict(orient="records")
    return {"items": items, "count": len(items), "requested_by": user.get("username"), "tenant": tenant}


@app.get("/api/v1/faenas/{faena_id}")
def get_faena(faena_id: int, ctx=Depends(require_tenant)):
    row = _fetch_one_with_tenant("faenas", faena_id, ctx["tenant"], "id, cliente_key, nombre, ubicacion, fecha_inicio, fecha_termino, estado, mandante_id, contrato_faena_id")
    if not row:
        raise HTTPException(status_code=404, detail="Faena no encontrada")
    return {"item": row, "tenant": ctx["tenant"]}


@app.post("/api/v1/faenas")
def create_faena(payload: FaenaPayload, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    execute(
        "INSERT INTO faenas(cliente_key, mandante_id, contrato_faena_id, nombre, ubicacion, fecha_inicio, fecha_termino, estado) VALUES(?,?,?,?,?,?,?,?)",
        (tenant, payload.mandante_id, payload.contrato_faena_id, payload.nombre.strip(), payload.ubicacion.strip(), payload.fecha_inicio, payload.fecha_termino, payload.estado),
    )
    audit_api_action(execute, str(user.get("username") or "api"), "API_CREATE_FAENA", payload.nombre, tenant, user_id=int(user.get("sub") or 0), role_global=str(user.get("role") or "OPERADOR"), fetch_df=fetch_df)
    return {"status": "ok", "tenant": tenant}


@app.put("/api/v1/faenas/{faena_id}")
def update_faena(faena_id: int, payload: FaenaPayload, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    q = "UPDATE faenas SET nombre=?, ubicacion=?, fecha_inicio=?, fecha_termino=?, estado=?, mandante_id=?, contrato_faena_id=? WHERE id=?"
    params = [payload.nombre.strip(), payload.ubicacion.strip(), payload.fecha_inicio, payload.fecha_termino, payload.estado, payload.mandante_id, payload.contrato_faena_id, int(faena_id)]
    if tenant:
        q += " AND COALESCE(cliente_key,'')=?"
        params.append(tenant)
    affected = execute_rowcount(q, tuple(params))
    if affected <= 0:
        raise HTTPException(status_code=404, detail="Faena no encontrada para la empresa activa")
    audit_api_action(execute, str(user.get("username") or "api"), "API_UPDATE_FAENA", f"faena_id={faena_id}", tenant)
    return {"status": "ok", "affected": affected}


@app.delete("/api/v1/faenas/{faena_id}")
def delete_faena(faena_id: int, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    q = "DELETE FROM faenas WHERE id=?"
    params = [int(faena_id)]
    if tenant:
        q += " AND COALESCE(cliente_key,'')=?"
        params.append(tenant)
    affected = execute_rowcount(q, tuple(params))
    if affected <= 0:
        raise HTTPException(status_code=404, detail="Faena no encontrada para la empresa activa")
    audit_api_action(execute, str(user.get("username") or "api"), "API_DELETE_FAENA", f"faena_id={faena_id}", tenant)
    return {"status": "ok", "affected": affected}


@app.get("/api/v1/trabajadores")
def list_trabajadores(ctx=Depends(require_tenant)):
    tenant = ctx["tenant"]
    user = ctx["user"]
    if tenant:
        df = fetch_df("SELECT id, cliente_key, rut, nombres, apellidos, cargo, email FROM trabajadores WHERE COALESCE(cliente_key,'')=? ORDER BY id DESC", (tenant,))
    else:
        df = fetch_df("SELECT id, cliente_key, rut, nombres, apellidos, cargo, email FROM trabajadores ORDER BY id DESC")
    items = [] if df is None or df.empty else df.fillna("").to_dict(orient="records")
    return {"items": items, "count": len(items), "requested_by": user.get("username"), "tenant": tenant}


@app.get("/api/v1/trabajadores/{trabajador_id}")
def get_trabajador(trabajador_id: int, ctx=Depends(require_tenant)):
    row = _fetch_one_with_tenant("trabajadores", trabajador_id, ctx["tenant"], "id, cliente_key, rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen")
    if not row:
        raise HTTPException(status_code=404, detail="Trabajador no encontrado")
    return {"item": row, "tenant": ctx["tenant"]}


@app.post("/api/v1/trabajadores")
def create_trabajador(payload: TrabajadorPayload, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    execute(
        "INSERT INTO trabajadores(cliente_key, rut, nombres, apellidos, cargo, centro_costo, email, fecha_contrato, vigencia_examen) VALUES(?,?,?,?,?,?,?,?,?)",
        (tenant, payload.rut.strip(), payload.nombres.strip(), payload.apellidos.strip(), payload.cargo.strip(), payload.centro_costo.strip(), payload.email.strip(), payload.fecha_contrato, payload.vigencia_examen),
    )
    audit_api_action(execute, str(user.get("username") or "api"), "API_CREATE_TRABAJADOR", payload.rut, tenant, user_id=int(user.get("sub") or 0), role_global=str(user.get("role") or "OPERADOR"), fetch_df=fetch_df)
    return {"status": "ok", "tenant": tenant}


@app.put("/api/v1/trabajadores/{trabajador_id}")
def update_trabajador(trabajador_id: int, payload: TrabajadorPayload, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    q = "UPDATE trabajadores SET rut=?, nombres=?, apellidos=?, cargo=?, centro_costo=?, email=?, fecha_contrato=?, vigencia_examen=? WHERE id=?"
    params = [payload.rut.strip(), payload.nombres.strip(), payload.apellidos.strip(), payload.cargo.strip(), payload.centro_costo.strip(), payload.email.strip(), payload.fecha_contrato, payload.vigencia_examen, int(trabajador_id)]
    if tenant:
        q += " AND COALESCE(cliente_key,'')=?"
        params.append(tenant)
    affected = execute_rowcount(q, tuple(params))
    if affected <= 0:
        raise HTTPException(status_code=404, detail="Trabajador no encontrado para la empresa activa")
    audit_api_action(execute, str(user.get("username") or "api"), "API_UPDATE_TRABAJADOR", f"trabajador_id={trabajador_id}", tenant)
    return {"status": "ok", "affected": affected}


@app.delete("/api/v1/trabajadores/{trabajador_id}")
def delete_trabajador(trabajador_id: int, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    q = "DELETE FROM trabajadores WHERE id=?"
    params = [int(trabajador_id)]
    if tenant:
        q += " AND COALESCE(cliente_key,'')=?"
        params.append(tenant)
    affected = execute_rowcount(q, tuple(params))
    if affected <= 0:
        raise HTTPException(status_code=404, detail="Trabajador no encontrado para la empresa activa")
    audit_api_action(execute, str(user.get("username") or "api"), "API_DELETE_TRABAJADOR", f"trabajador_id={trabajador_id}", tenant)
    return {"status": "ok", "affected": affected}


@app.get("/api/v1/dashboard/summary")
def dashboard_summary(ctx=Depends(require_tenant)):
    tenant = ctx["tenant"]
    params = (tenant,) if tenant else ()
    where = " WHERE COALESCE(cliente_key,'')=?" if tenant else ""
    return {
        "tenant": tenant,
        "faenas": int(fetch_value(f"SELECT COUNT(*) FROM faenas{where}", params, default=0) or 0),
        "trabajadores": int(fetch_value(f"SELECT COUNT(*) FROM trabajadores{where}", params, default=0) or 0),
        "alertas_abiertas": int(fetch_value(f"SELECT COUNT(*) FROM sgsst_alertas{where + (' AND ' if tenant else ' WHERE ')}COALESCE(estado,'ABIERTA')='ABIERTA'", params, default=0) or 0),
        "inspecciones_moviles": int(fetch_value(f"SELECT COUNT(*) FROM segav_mobile_inspections{where}", params, default=0) or 0),
        "firmas_pendientes": int(fetch_value(f"SELECT COUNT(*) FROM segav_signature_requests{where + (' AND ' if tenant else ' WHERE ')}COALESCE(status,'PENDIENTE')='PENDIENTE'", params, default=0) or 0),
    }


@app.get("/api/v1/exports/faena/{faena_id}")
def export_faena(faena_id: int, ctx=Depends(require_tenant)):
    if export_zip_for_faena is None:
        raise HTTPException(status_code=503, detail="Exportador no disponible en este entorno")
    tenant = ctx["tenant"]
    if tenant:
        ok = int(fetch_value("SELECT COUNT(*) FROM faenas WHERE id=? AND COALESCE(cliente_key,'')=?", (int(faena_id), tenant), default=0) or 0)
        if ok <= 0:
            raise HTTPException(status_code=404, detail="Faena no encontrada para la empresa activa")
    zip_bytes, zip_name = export_zip_for_faena(int(faena_id))
    return StreamingResponse(io.BytesIO(zip_bytes), media_type="application/zip", headers={"Content-Disposition": f'attachment; filename="{zip_name}"'})


@app.get("/api/v1/rules")
def list_rules(ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    df = fetch_df("SELECT id, rule_name, target_scope, severity, is_active, updated_at FROM segav_rule_engine_rules WHERE COALESCE(cliente_key,'')=? ORDER BY id DESC", (tenant,))
    items = [] if df is None or df.empty else df.fillna("").to_dict(orient="records")
    return {"items": items, "count": len(items), "tenant": tenant}


@app.post("/api/v1/rules")
def create_rule(payload: RulePayload, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    execute(
        "INSERT INTO segav_rule_engine_rules(cliente_key, rule_name, target_scope, severity, rule_json, is_active, created_at, updated_at) VALUES(?,?,?,?,?,?,datetime('now'),datetime('now'))",
        (tenant, payload.rule_name, payload.target_scope, payload.severity, json.dumps(payload.rule_json, ensure_ascii=False), 1 if payload.is_active else 0),
    )
    audit_api_action(execute, str(user.get("username") or "api"), "API_CREATE_RULE", payload.rule_name, tenant)
    return {"status": "ok", "tenant": tenant}


@app.get("/api/v1/integrations/events")
def list_integration_events(ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    df = fetch_df("SELECT id, integration_type, event_type, status, created_at, processed_at FROM segav_integration_events WHERE COALESCE(cliente_key,'')=? ORDER BY id DESC LIMIT 100", (tenant,))
    items = [] if df is None or df.empty else df.fillna("").to_dict(orient="records")
    return {"items": items, "count": len(items), "tenant": tenant}


@app.post("/api/v1/integrations/events")
def create_integration_event(payload: IntegrationEventPayload, ctx=Depends(require_tenant)):
    tenant = ctx["tenant"] or ""
    user = ctx["user"]
    execute(
        "INSERT INTO segav_integration_events(cliente_key, integration_type, event_type, payload_json, status, created_at) VALUES(?,?,?,?,?,datetime('now'))",
        (tenant, payload.integration_type, payload.event_type, json.dumps(payload.payload, ensure_ascii=False), "PENDIENTE"),
    )
    audit_api_action(execute, str(user.get("username") or "api"), "API_CREATE_EVENT", payload.event_type, tenant)
    return {"status": "ok", "tenant": tenant}


@app.get("/api/v1/admin/migrations/status")
def api_migrations_status(user=Depends(require_superadmin)):
    rows = migration_status(fetch_value)
    return {"items": rows, "count": len(rows), "requested_by": user.get("username")}


@app.get("/api/v1/admin/production/readiness")
def api_production_readiness(user=Depends(require_superadmin), tenant: Optional[str] = Query(default=None)):
    env_checks = environment_readiness()
    data_checks = data_readiness(fetch_value, tenant=tenant)
    summary = summarize_readiness(env_checks, data_checks)
    summary["requested_by"] = user.get("username")
    summary["tenant"] = tenant
    return summary
