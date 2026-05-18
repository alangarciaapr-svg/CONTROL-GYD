import os
from pathlib import Path

REQUIRED_FILES = [
    'streamlit_app.py',
    'core_db.py',
    'requirements.txt',
    '.streamlit/config.toml',
    'segav_core/app_config.py',
    'segav_core/bootstrap.py',
    'segav_core/auth.py',
]

def read_secrets_example():
    p = Path('.streamlit/secrets.toml.example')
    return p.exists()

def main():
    base = Path('.')
    print('=== SEGAV ERP · Verificación rápida ===')
    missing = [f for f in REQUIRED_FILES if not (base / f).exists()]
    if missing:
        print('Faltan archivos críticos:')
        for f in missing:
            print(' -', f)
    else:
        print('Estructura base: OK')

    has_dsn = bool(os.getenv('SUPABASE_DB_URL'))
    has_parts = all(os.getenv(k) for k in ['SUPABASE_DB_HOST', 'SUPABASE_DB_USER', 'SUPABASE_DB_PASSWORD'])
    has_storage = bool(os.getenv('SUPABASE_URL') and (os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')))

    if has_dsn or has_parts:
        print('Backend esperado: Supabase/Postgres')
    else:
        print('Backend esperado: SQLite local (app.db)')

    if has_storage:
        print('Storage Supabase: configurado por variables de entorno')
    else:
        print('Storage Supabase: no detectado en entorno; se usará fallback local si la app lo permite')

    print('Secrets example:', 'OK' if read_secrets_example() else 'FALTA .streamlit/secrets.toml.example')
    print('Usuario inicial por defecto si no existen usuarios: a.garcia / 225188')
    print('Recuerda cambiar la contraseña al primer ingreso.')

if __name__ == '__main__':
    main()
