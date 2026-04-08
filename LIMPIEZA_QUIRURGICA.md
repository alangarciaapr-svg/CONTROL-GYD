# Limpieza quirúrgica aplicada a esta entrega

Se preparó una entrega limpia de release sin tocar la lógica funcional visible del ERP.

## Qué se limpió
- Se excluyeron archivos compilados `*.pyc` y carpetas `__pycache__`.
- Se excluyeron imágenes de bosquejo, pruebas visuales y archivos temporales que no son necesarios para ejecutar la app.
- Se excluyeron zips históricos y archivos intermedios de depuración.
- Se mantuvieron solo los archivos de ejecución reales del ERP.

## Qué se conservó
- `streamlit_app.py`
- `core_db.py`
- carpeta `segav_core/`
- branding activo en `assets/branding/`
- archivos `.streamlit/`
- scripts de arranque y verificación

## Validación
- Compilación Python satisfactoria sobre la base entregada.
