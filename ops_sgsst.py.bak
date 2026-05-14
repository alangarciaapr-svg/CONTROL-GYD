import json

ESTADOS_FAENA = ["ACTIVA", "TERMINADA"]
DOC_TIPO_LABELS = {
    "CONTRATO_TRABAJO": "CONTRATO",
    "REGISTRO_EPP": "REGISTRO DE EPP",
    "ENTREGA_RIOHS": "REGISTRO ENTREGA DE RIOHS",
    "IRL": "IRL",
    "LICENCIA_CONDUCIR": "LICENCIA DE CONDUCIR",
    "CEDULA_IDENTIDAD": "CÉDULA DE IDENTIDAD",
    "CERTIFICACION_CORMA": "CERTIFICACIÓN CORMA",
    "LIQUIDACIONES_SUELDO_MES": "LIQUIDACIONES DE SUELDO",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30": "CERTIFICADO DE ANTECEDENTES LABORALES F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1": "CERTIFICADO DE CUMPLIMIENTOS LABORALES Y PREVISIONALES F30-1",
    "CERTIFICADO_ACCIDENTABILIDAD": "CERTIFICADO DE ACCIDENTABILIDAD",
    "CERTIFICADO_CUMPLIMIENTO_LABORAL": "CERTIFICADO DE CUMPLIMIENTO LABORAL",
    "CERTIFICADO_ADHESION_A_MUTUALIDAD": "CERTIFICADO DE ADHESIÓN A MUTUALIDAD",
}
DOC_OBLIGATORIOS = [
    "CONTRATO_TRABAJO",
    "REGISTRO_EPP",
    "ENTREGA_RIOHS",
    "IRL",
]
DOCS_OPERARIO_MAQUINARIA_FORESTAL = [
    "LICENCIA_CONDUCIR",
    "CEDULA_IDENTIDAD",
]
DOCS_MOTOSIERRISTA = [
    "CERTIFICACION_CORMA",
    "CEDULA_IDENTIDAD",
]
CARGO_DOCS_RULES = {
    "OPERADOR DE MAQUINARIA FORESTAL": DOC_OBLIGATORIOS + DOCS_OPERARIO_MAQUINARIA_FORESTAL,
    "MOTOSIERRISTA": DOC_OBLIGATORIOS + DOCS_MOTOSIERRISTA,
    "ESTROBERO": list(DOC_OBLIGATORIOS),
    "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
    "MECANICO": list(DOC_OBLIGATORIOS),
    "ASERRADERO": list(DOC_OBLIGATORIOS),
    "PLANTA": list(DOC_OBLIGATORIOS),
}
CARGO_DOCS_ORDER = [
    "OPERADOR DE MAQUINARIA FORESTAL",
    "MOTOSIERRISTA",
    "ESTROBERO",
    "ADMINISTRATIVO",
    "MECANICO",
    "ASERRADERO",
    "PLANTA",
]
DOC_EMPRESA_SUGERIDOS = [
    "LIQUIDACIONES_SUELDO_MES",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1",
    "CERTIFICADO_ACCIDENTABILIDAD",
]
DOC_EMPRESA_REQUERIDOS = [
    "LIQUIDACIONES_SUELDO_MES",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1",
    "CERTIFICADO_ACCIDENTABILIDAD",
]
DOC_EMPRESA_MENSUALES = [
    "LIQUIDACIONES_SUELDO_MES",
    "CERTIFICADO_ANTECEDENTES_LABORALES_F30",
    "CERTIFICADO_CUMPLIMIENTOS_LABORALES_PREVISIONALES_F30_1",
    "CERTIFICADO_ACCIDENTABILIDAD",
]
ERP_CLIENT_PARAM_DEFAULTS = {
    "usa_multi_faena": "SI",
    "usa_docs_empresa_mensuales": "SI",
    "usa_miper": "SI",
    "usa_ds594": "SI",
    "usa_ley_16744": "SI",
    "usa_capacitaciones_odi": "SI",
    "usa_auditoria": "SI",
    "branding_cliente": "ESTANDAR",
}
ERP_TEMPLATE_PRESETS = {
    "GENERAL": {
        "label": "General",
        "vertical": "General",
        "description": "Base comercial multipropósito para servicios, administración y operación documental.",
        "cargos": ["OPERARIO", "SUPERVISOR", "ADMINISTRATIVO", "MECANICO", "BODEGUERO", "PLANTA"],
        "cargo_rules": {
            "OPERARIO": list(DOC_OBLIGATORIOS),
            "SUPERVISOR": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "MECANICO": list(DOC_OBLIGATORIOS),
            "BODEGUERO": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "FORESTAL": {
        "label": "Forestal",
        "vertical": "Forestal",
        "description": "Plantilla base para faenas forestales, con cargos y documentos obligatorios por rol.",
        "cargos": list(CARGO_DOCS_ORDER) + ["SUPERVISOR DE FAENA"],
        "cargo_rules": {
            **{k: list(dict.fromkeys(v)) for k, v in CARGO_DOCS_RULES.items()},
            "SUPERVISOR DE FAENA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "CONSTRUCCION": {
        "label": "Construcción",
        "vertical": "Construcción",
        "description": "Plantilla para contratistas y subcontratistas con control de cuadrillas, conducción y documentación mensual.",
        "cargos": ["OPERARIO", "CAPATAZ", "CONDUCTOR", "MECANICO", "ADMINISTRATIVO", "BODEGUERO", "PLANTA"],
        "cargo_rules": {
            "OPERARIO": list(DOC_OBLIGATORIOS),
            "CAPATAZ": list(DOC_OBLIGATORIOS),
            "CONDUCTOR": list(dict.fromkeys(DOC_OBLIGATORIOS + ["LICENCIA_CONDUCIR", "CEDULA_IDENTIDAD"])),
            "MECANICO": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "BODEGUERO": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "TRANSPORTE": {
        "label": "Transporte",
        "vertical": "Transporte",
        "description": "Plantilla para operación con conductores, mantención y trazabilidad documental por servicio.",
        "cargos": ["CONDUCTOR", "PEONETA", "MECANICO", "ADMINISTRATIVO", "PLANTA"],
        "cargo_rules": {
            "CONDUCTOR": list(dict.fromkeys(DOC_OBLIGATORIOS + ["LICENCIA_CONDUCIR", "CEDULA_IDENTIDAD"])),
            "PEONETA": list(DOC_OBLIGATORIOS),
            "MECANICO": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
    "SERVICIOS": {
        "label": "Servicios",
        "vertical": "Servicios",
        "description": "Plantilla para empresas de servicios generales con configuración ligera y adaptable.",
        "cargos": ["TECNICO", "SUPERVISOR", "ADMINISTRATIVO", "AUXILIAR", "PLANTA"],
        "cargo_rules": {
            "TECNICO": list(DOC_OBLIGATORIOS),
            "SUPERVISOR": list(DOC_OBLIGATORIOS),
            "ADMINISTRATIVO": list(DOC_OBLIGATORIOS),
            "AUXILIAR": list(DOC_OBLIGATORIOS),
            "PLANTA": list(DOC_OBLIGATORIOS),
        },
        "empresa_docs": list(DOC_EMPRESA_MENSUALES),
        "params": dict(ERP_CLIENT_PARAM_DEFAULTS),
    },
}
MESES_ES = {
    1: "ENERO",
    2: "FEBRERO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE",
}
REQ_DOCS_N = len(DOC_OBLIGATORIOS)
SGSST_NORMAS = ["DS 44", "Ley 16.744", "DS 594", "Ley Karin", "Interno"]
SGSST_ESTADOS = ["PENDIENTE", "EN CURSO", "CERRADO", "NO APLICA"]
SGSST_RESULTADOS = ["CUMPLE", "NO CUMPLE", "OBSERVACIÓN"]
SGSST_TIPOS_EVENTO = ["INCIDENTE", "ACCIDENTE DEL TRABAJO", "ACCIDENTE DE TRAYECTO", "ENFERMEDAD PROFESIONAL", "HALLAZGO"]
SGSST_GRAVEDADES = ["BAJA", "MEDIA", "ALTA", "GRAVE/FATAL"]
SGSST_TIPOS_CAP = ["ODI", "INDUCCIÓN", "CAPACITACIÓN", "CHARLA DE SEGURIDAD", "SIMULACRO"]
SGSST_MATRIZ_BASE = [
    {"norma": "DS 44", "articulo": "Sistema de gestión", "tema": "Implementación SGSST", "obligacion": "Mantener un sistema de gestión preventivo con instrumentos y seguimiento.", "aplica_a": "Empresa", "periodicidad": "Permanente", "responsable": "Gerencia / Prevención", "evidencia": "Manual SGSST, registros y seguimiento", "estado": "EN CURSO"},
    {"norma": "DS 44", "articulo": "MIPER", "tema": "Matriz de riesgos", "obligacion": "Mantener identificación de peligros y evaluación de riesgos por faena, tarea y cargo.", "aplica_a": "Faenas / Cargos", "periodicidad": "Anual o por cambio", "responsable": "Prevención", "evidencia": "MIPER vigente", "estado": "PENDIENTE"},
    {"norma": "DS 44", "articulo": "Programa preventivo", "tema": "Programa anual", "obligacion": "Planificar actividades preventivas con responsables, plazos y evidencias.", "aplica_a": "Empresa / Faenas", "periodicidad": "Anual", "responsable": "Gerencia / Prevención", "evidencia": "Programa anual y cierres", "estado": "PENDIENTE"},
    {"norma": "DS 44", "articulo": "Información y capacitación", "tema": "ODI y formación", "obligacion": "Entregar información de riesgos y capacitación preventiva a trabajadores.", "aplica_a": "Trabajadores", "periodicidad": "Ingreso y periódica", "responsable": "Jefaturas / Prevención", "evidencia": "Registros ODI y capacitaciones", "estado": "EN CURSO"},
    {"norma": "DS 44", "articulo": "Emergencias", "tema": "Plan de emergencia", "obligacion": "Disponer de plan de emergencias, simulacros y responsables.", "aplica_a": "Empresa / Faenas", "periodicidad": "Anual", "responsable": "Gerencia / Faenas", "evidencia": "Plan y registros de simulacro", "estado": "PENDIENTE"},
    {"norma": "Ley 16.744", "articulo": "Seguro", "tema": "Organismo administrador", "obligacion": "Mantener afiliación y coordinación preventiva con organismo administrador.", "aplica_a": "Empresa", "periodicidad": "Permanente", "responsable": "Gerencia", "evidencia": "Certificado de adhesión", "estado": "EN CURSO"},
    {"norma": "Ley 16.744", "articulo": "Accidentes y enfermedades", "tema": "Investigación", "obligacion": "Registrar, investigar y gestionar medidas correctivas de incidentes y accidentes.", "aplica_a": "Empresa / Faenas", "periodicidad": "Cada evento", "responsable": "Prevención / Jefatura", "evidencia": "Investigaciones y cierres", "estado": "PENDIENTE"},
    {"norma": "Ley 16.744", "articulo": "Participación", "tema": "CPHS / Monitoreo dotación", "obligacion": "Monitorear obligación de CPHS según dotación y mantener registros si aplica.", "aplica_a": "Empresa", "periodicidad": "Mensual", "responsable": "Gerencia", "evidencia": "Actas / control de dotación", "estado": "EN CURSO"},
    {"norma": "DS 594", "articulo": "Condiciones sanitarias", "tema": "Agua y servicios higiénicos", "obligacion": "Verificar agua potable, higiene, orden y aseo en lugares de trabajo.", "aplica_a": "Faenas / Planta", "periodicidad": "Mensual", "responsable": "Supervisor / Faena", "evidencia": "Checklist DS 594", "estado": "PENDIENTE"},
    {"norma": "DS 594", "articulo": "Condiciones ambientales", "tema": "Señalización, extintores y ambiente", "obligacion": "Controlar señalización, extintores, vías de circulación y condiciones ambientales.", "aplica_a": "Faenas / Planta", "periodicidad": "Mensual", "responsable": "Supervisor / Mantención", "evidencia": "Inspecciones y acciones", "estado": "PENDIENTE"},
]
ASSIGNACION_INSERT_SQL = """
INSERT INTO asignaciones(faena_id, trabajador_id, cargo_faena, fecha_ingreso, fecha_egreso, estado)
VALUES(?,?,?,?,?,?)
ON CONFLICT DO NOTHING
"""
