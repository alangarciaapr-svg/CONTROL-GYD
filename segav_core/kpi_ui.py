from __future__ import annotations

from typing import Any, Iterable

import pandas as pd
import streamlit as st

_TONE_ICONS = {
    "neutral": "●",
    "info": "●",
    "success": "●",
    "warning": "▲",
    "danger": "●",
    "purple": "●",
}


def inject_kpi_css() -> None:
    """Estilo global liviano para KPIs nativos y gráficos ejecutivos.

    Se mantiene sin HTML estructural dentro de las tarjetas KPI para evitar que
    aparezcan fragmentos visibles en Streamlit. Solo se inyecta CSS seguro.
    """
    if st.session_state.get("_segav_kpi_css_loaded_v2"):
        return
    st.session_state["_segav_kpi_css_loaded_v2"] = True
    st.markdown(
        """
<style>
[data-testid="stMetric"] {
    background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(248,250,252,.95));
    border: 1px solid rgba(15, 23, 42, .08) !important;
    border-radius: 20px !important;
    padding: 14px 16px !important;
    box-shadow: 0 12px 26px rgba(15, 23, 42, .07);
}
[data-testid="stMetricValue"] {font-weight: 900 !important; letter-spacing:-.03em;}
[data-testid="stMetricLabel"] {font-weight: 800 !important; color:#475569 !important;}
.segav-kpi-native-caption {
    font-size:.82rem;
    color:#64748b;
    margin-top:-.25rem;
    margin-bottom:.35rem;
}
.segav-kpi-status-line {
    font-size:.78rem;
    font-weight:800;
    letter-spacing:.025em;
    text-transform:uppercase;
    margin:.25rem 0 .15rem 0;
}
.segav-kpi-section {
    border-radius: 18px;
    padding: 14px 16px;
    border: 1px solid rgba(15, 23, 42, .08);
    background: linear-gradient(135deg, rgba(248,250,252,.96), rgba(239,246,255,.72));
    box-shadow: 0 10px 26px rgba(15, 23, 42, .055);
    margin: 10px 0 14px 0;
}
.segav-kpi-section-title {font-size:1.02rem; font-weight:900; color:#0f172a;}
.segav-kpi-section-subtitle {font-size:.86rem; color:#64748b; margin-top:4px;}
.segav-chart-card {
    border: 1px solid rgba(15, 23, 42, .08);
    border-radius: 18px;
    padding: 12px 14px;
    background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(248,250,252,.92));
    box-shadow: 0 10px 24px rgba(15, 23, 42, .06);
    margin: 8px 0 14px 0;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def tone_for_percentage(value: float, *, danger_below: float = 60, warning_below: float = 80) -> str:
    try:
        v = float(value)
    except Exception:
        return "neutral"
    if v < danger_below:
        return "danger"
    if v < warning_below:
        return "warning"
    return "success"


def tone_for_count(value: int | float, *, zero_good: bool = True, warning_at: int = 1, danger_at: int = 3) -> str:
    try:
        v = float(value)
    except Exception:
        return "neutral"
    if zero_good and v <= 0:
        return "success"
    if v >= danger_at:
        return "danger"
    if v >= warning_at:
        return "warning"
    return "info"


def _status_text(status: str = "", tone: str = "neutral") -> str:
    status = str(status or "").strip()
    if not status:
        return ""
    return f"{_TONE_ICONS.get(str(tone).lower(), '●')} {status}"


def kpi_card(
    label: str,
    value: Any,
    *,
    subtitle: str = "",
    icon: str = "📊",
    tone: str = "neutral",
    status: str = "",
    delta: str = "",
    progress: float | None = None,
    help_text: str | None = None,
) -> None:
    """Tarjeta KPI profesional usando componentes nativos de Streamlit.

    Se evita construir la tarjeta completa con HTML para impedir errores visuales
    como ``<div class=...>`` apareciendo como texto dentro del dashboard.
    """
    inject_kpi_css()
    label_txt = f"{icon} {label}" if icon else str(label)
    try:
        st.metric(label=label_txt, value=str(value), delta=str(delta) if delta else None, help=help_text)
    except TypeError:
        st.metric(label=label_txt, value=str(value), delta=str(delta) if delta else None)
    if subtitle:
        st.caption(str(subtitle))
    st_line = _status_text(status, tone)
    if st_line:
        st.caption(st_line)
    if progress is not None:
        try:
            p = max(0.0, min(100.0, float(progress))) / 100.0
            st.progress(p)
        except Exception:
            pass


def kpi_grid(cards: Iterable[dict[str, Any]], *, columns: int = 4) -> None:
    cards_list = list(cards)
    if not cards_list:
        return
    cols_n = max(1, min(int(columns or 4), 6))
    for start in range(0, len(cards_list), cols_n):
        row_cards = cards_list[start:start + cols_n]
        cols = st.columns(len(row_cards))
        for col, card in zip(cols, row_cards):
            with col:
                with st.container(border=True):
                    kpi_card(**card)


def kpi_section(title: str, subtitle: str = "") -> None:
    inject_kpi_css()
    # HTML acotado solo para encabezado, sin fragmentos dinámicos complejos.
    st.markdown(
        f"""
<div class="segav-kpi-section">
  <div class="segav-kpi-section-title">{str(title)}</div>
  <div class="segav-kpi-section-subtitle">{str(subtitle or '')}</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def professional_bar_chart(
    data: pd.DataFrame | pd.Series,
    *,
    x: str | None = None,
    y: str | None = None,
    title: str = "",
    horizontal: bool = False,
    height: int = 280,
    limit: int | None = None,
) -> None:
    """Gráfico de barras con presentación ejecutiva y fallback seguro.

    Usa Altair si está disponible, que normalmente viene con Streamlit. Si no,
    cae a una tabla ordenada para no romper la app.
    """
    inject_kpi_css()
    try:
        if data is None:
            st.info("Sin datos para graficar.")
            return
        if isinstance(data, pd.Series):
            df = data.reset_index()
            x = x or str(df.columns[0])
            y = y or str(df.columns[1])
        else:
            df = data.copy()
            if df.index.name and (x is None or y is None):
                df = df.reset_index()
            if x is None:
                x = str(df.columns[0])
            if y is None:
                y = str(df.columns[1]) if len(df.columns) > 1 else str(df.columns[0])
        if df.empty:
            st.info("Sin datos para graficar.")
            return
        if limit:
            df = df.head(int(limit))
        # Evita errores de tipos raros en Altair.
        df[x] = df[x].astype(str)
        df[y] = pd.to_numeric(df[y], errors="coerce").fillna(0)
        if title:
            st.markdown(f"#### {title}")
        try:
            import altair as alt
            if horizontal:
                chart = (
                    alt.Chart(df)
                    .mark_bar(cornerRadiusEnd=6)
                    .encode(
                        y=alt.Y(f"{x}:N", sort="-x", title=None),
                        x=alt.X(f"{y}:Q", title=None),
                        tooltip=[alt.Tooltip(f"{x}:N", title=x), alt.Tooltip(f"{y}:Q", title=y)],
                    )
                    .properties(height=height)
                )
            else:
                chart = (
                    alt.Chart(df)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X(f"{x}:N", sort="-y", title=None),
                        y=alt.Y(f"{y}:Q", title=None),
                        tooltip=[alt.Tooltip(f"{x}:N", title=x), alt.Tooltip(f"{y}:Q", title=y)],
                    )
                    .properties(height=height)
                )
            st.altair_chart(chart, use_container_width=True)
        except Exception:
            st.dataframe(df[[x, y]], use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"No se pudo construir el gráfico: {exc}")
