# SEGAV ERP — Fase de saneamiento v8.4.70

## Qué se corrigió
- Se eliminaron definiciones duplicadas antiguas de páginas dentro de `streamlit_app.py` y se conservaron solo las últimas definiciones activas que delegan a `segav_core`.
- Se eliminó el sombreado del nombre `bootstrap_app` para que el arranque sea más claro de mantener.
- Se retiró un import no usado.
- Se agregó diagnóstico liviano para algunos fallos no críticos (`soft_errors` en `st.session_state`) en vez de silenciarlos por completo.

## Resultado esperado
- Menor fragilidad al modificar páginas, porque ya no conviven implementaciones antiguas y wrappers activos con el mismo nombre.
- Menor riesgo de corregir una función vieja sin afectar la usada realmente por la app.
- Mantenimiento más claro del arranque y del sidebar.

## Nota
No se eliminaron módulos visibles ni se modificó la navegación funcional de negocio. La limpieza fue estructural y conservadora.
