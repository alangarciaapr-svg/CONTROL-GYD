# Control Documental de Faenas (v6) – Import Excel de Trabajadores

✅ Nueva función: Importar trabajadores desde Excel (como tu plantilla).

Plantilla soportada (columnas):
- RUT (obligatoria)
- NOMBRE (obligatoria)
- CARGO (opcional)
- CENTRO_COSTO (opcional)
- EMAIL (opcional)
- FECHA DE CONTRATO (opcional)
- VIGENCIA_EXAMEN (opcional)

El sistema separa NOMBRE en:
- Nombres: tokens iniciales
- Apellidos: por defecto últimos 2 tokens (si hay 4+ palabras)

Main file: `streamlit_app.py`
