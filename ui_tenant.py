from __future__ import annotations


def pendientes_obligatorios_logic(fetch_df, worker_required_docs, faena_id: int) -> dict:
    try:
        trab = fetch_df(
            """
            SELECT t.id, t.rut, t.apellidos || ' ' || t.nombres AS nombre, t.cargo
            FROM asignaciones a
            JOIN trabajadores t ON t.id = a.trabajador_id
            WHERE a.faena_id=? AND COALESCE(NULLIF(TRIM(a.estado),''),'ACTIVA')='ACTIVA'
            ORDER BY t.apellidos, t.nombres
            """,
            (int(faena_id),),
        )
        if trab is None or trab.empty:
            return {}
        result = {}
        for _, row in trab.iterrows():
            tid = int(row["id"])
            cargo = str(row.get("cargo") or "")
            required = list(worker_required_docs(cargo) or [])
            if not required:
                result[str(row["nombre"])] = []
                continue
            docs = fetch_df(
                "SELECT DISTINCT doc_tipo FROM trabajador_documentos WHERE trabajador_id=?",
                (tid,),
            )
            present = set(docs["doc_tipo"].astype(str).tolist()) if docs is not None and not docs.empty else set()
            missing = [d for d in required if d not in present]
            result[str(row["nombre"])] = missing
        return result
    except Exception:
        return {}


def pendientes_empresa_faena_logic(fetch_df, get_empresa_monthly_doc_types, faena_id: int) -> list:
    try:
        required = list(get_empresa_monthly_doc_types() or [])
        if not required:
            return []
        docs = fetch_df(
            "SELECT DISTINCT doc_tipo FROM faena_empresa_documentos WHERE faena_id=?",
            (int(faena_id),),
        )
        present = set(docs["doc_tipo"].astype(str).tolist()) if docs is not None and not docs.empty else set()
        return [d for d in required if d not in present]
    except Exception:
        return []
