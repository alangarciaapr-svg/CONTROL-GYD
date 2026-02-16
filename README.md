# Gestión de Faenas – MVP (Streamlit)

Este repo está listo para subir a Streamlit Community Cloud.

## Ejecutar en tu PC
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Subir a GitHub
Sube estos archivos al repositorio:
- `streamlit_app.py`
- `requirements.txt`
- `.streamlit/config.toml`

## Desplegar en Streamlit Community Cloud
1. En Streamlit Community Cloud: **New app**
2. Repo: el tuyo
3. Branch: `main`
4. Main file: `streamlit_app.py`

## Notas importantes (MVP)
- Usa **SQLite** y guarda archivos en `uploads/`.
- En Streamlit Community Cloud, el filesystem no es un storage duradero para producción.
  Para producción: PostgreSQL remoto + storage S3/MinIO.
