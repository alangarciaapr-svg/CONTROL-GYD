import sys
from pathlib import Path
import types

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import streamlit  # noqa: F401
except Exception:
    class _FakeCacheDecorator:
        def __call__(self, *args, **kwargs):
            def deco(fn):
                fn.clear = lambda: None
                return fn
            return deco

    def _noop(*args, **kwargs):
        return None

    fake_streamlit = types.SimpleNamespace(
        cache_data=_FakeCacheDecorator(),
        cache_resource=_FakeCacheDecorator(),
        session_state={},
        secrets={},
        set_page_config=_noop,
        markdown=_noop,
        caption=_noop,
        info=_noop,
        warning=_noop,
        success=_noop,
        error=_noop,
        metric=_noop,
        dataframe=_noop,
        selectbox=lambda *a, **k: None,
        text_input=lambda *a, **k: '',
        text_area=lambda *a, **k: '',
        checkbox=lambda *a, **k: False,
        button=lambda *a, **k: False,
        columns=lambda n, **k: [types.SimpleNamespace(metric=_noop, caption=_noop) for _ in range(n if isinstance(n, int) else len(n))],
        expander=lambda *a, **k: types.SimpleNamespace(__enter__=lambda self: self, __exit__=lambda self, exc_type, exc, tb: False),
        tabs=lambda labels: [types.SimpleNamespace(__enter__=lambda self: self, __exit__=lambda self, exc_type, exc, tb: False) for _ in labels],
        rerun=_noop,
    )
    sys.modules['streamlit'] = fake_streamlit
