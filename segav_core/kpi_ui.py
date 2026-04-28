from __future__ import annotations

import html
from typing import Any, Iterable

import streamlit as st


_TONES = {
    "neutral": {"accent": "#64748b", "bg": "rgba(100,116,139,.10)", "text": "#0f172a"},
    "info": {"accent": "#2563eb", "bg": "rgba(37,99,235,.10)", "text": "#0f172a"},
    "success": {"accent": "#16a34a", "bg": "rgba(22,163,74,.10)", "text": "#0f172a"},
    "warning": {"accent": "#f59e0b", "bg": "rgba(245,158,11,.13)", "text": "#0f172a"},
    "danger": {"accent": "#dc2626", "bg": "rgba(220,38,38,.12)", "text": "#0f172a"},
    "purple": {"accent": "#7c3aed", "bg": "rgba(124,58,237,.10)", "text": "#0f172a"},
}


def _render_html(markup: str) -> None:
    """Renderiza HTML sin dejar fragmentos visibles como texto.

    Preferimos ``st.html`` cuando está disponible; en versiones antiguas de
    Streamlit se mantiene el fallback compatible con ``unsafe_allow_html``.
    """
    if hasattr(st, "html"):
        st.html(markup)
    else:
        st.markdown(markup, unsafe_allow_html=True)


def inject_kpi_css() -> None:
    """Carga una sola vez el estilo visual de indicadores ejecutivos."""
    if st.session_state.get("_segav_kpi_css_loaded"):
        return
    st.session_state["_segav_kpi_css_loaded"] = True
    _render_html(
        """
<style>
.segav-kpi-card {
    position: relative;
    min-height: 132px;
    border: 1px solid rgba(15, 23, 42, .08);
    border-radius: 20px;
    padding: 16px 17px 14px 17px;
    background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(248,250,252,.95));
    box-shadow: 0 12px 28px rgba(15, 23, 42, .075);
    overflow: hidden;
    margin-bottom: 12px;
}
.segav-kpi-card::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 5px;
    background: var(--segav-kpi-accent, #64748b);
}
.segav-kpi-head {
    display:flex;
    justify-content:space-between;
    align-items:flex-start;
    gap:10px;
}
.segav-kpi-label {
    font-size: .78rem;
    letter-spacing: .045em;
    text-transform: uppercase;
    color: #64748b;
    font-weight: 800;
    line-height: 1.2;
}
.segav-kpi-icon {
    display:flex;
    align-items:center;
    justify-content:center;
    min-width: 34px;
    height: 34px;
    border-radius: 13px;
    background: var(--segav-kpi-soft, rgba(100,116,139,.10));
    font-size: 1.05rem;
}
.segav-kpi-value {
    font-size: clamp(1.55rem, 2.5vw, 2.2rem);
    line-height: 1.0;
    margin-top: 14px;
    font-weight: 900;
    color: #0f172a;
    letter-spacing: -.035em;
}
.segav-kpi-subtitle {
    margin-top: 8px;
    color: #64748b;
    font-size: .86rem;
    line-height: 1.25;
    min-height: 18px;
}
.segav-kpi-footer {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
    margin-top: 12px;
}
.segav-kpi-badge {
    display:inline-flex;
    align-items:center;
    gap:6px;
    border-radius: 999px;
    padding: 4px 9px;
    color: var(--segav-kpi-accent, #64748b);
    background: var(--segav-kpi-soft, rgba(100,116,139,.10));
    font-size: .72rem;
    font-weight: 850;
    letter-spacing: .025em;
    text-transform: uppercase;
}
.segav-kpi-delta {
    color: var(--segav-kpi-accent, #64748b);
    font-size: .76rem;
    font-weight: 850;
    white-space: nowrap;
}
.segav-kpi-progress {
    height: 7px;
    border-radius: 999px;
    background: rgba(148,163,184,.24);
    margin-top: 12px;
    overflow:hidden;
}
.segav-kpi-progress > span {
    display:block;
    height:100%;
    width: var(--segav-kpi-progress, 0%);
    border-radius:999px;
    background: var(--segav-kpi-accent, #64748b);
}
.segav-kpi-strip {
    border-radius: 18px;
    padding: 14px 16px;
    border: 1px solid rgba(15, 23, 42, .08);
    background: rgba(248,250,252,.82);
    margin: 10px 0 14px 0;
}
.segav-kpi-strip-title {
    font-size: .92rem;
    font-weight: 900;
    color: #0f172a;
    letter-spacing: -.01em;
}
.segav-kpi-strip-subtitle {
    font-size: .82rem;
    color: #64748b;
    margin-top: 3px;
}
</style>
        """
    )


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _tone_cfg(tone: str | None) -> dict[str, str]:
    return _TONES.get(str(tone or "neutral").lower(), _TONES["neutral"])


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
    inject_kpi_css()
    cfg = _tone_cfg(tone)
    progress_html = ""
    if progress is not None:
        try:
            p = max(0, min(100, float(progress)))
        except Exception:
            p = 0
        progress_html = f'<div class="segav-kpi-progress"><span style="--segav-kpi-progress:{p:.1f}%;"></span></div>'
    status_html = f'<span class="segav-kpi-badge">{_esc(status)}</span>' if status else ""
    delta_html = f'<span class="segav-kpi-delta">{_esc(delta)}</span>' if delta else ""
    footer_html = f'<div class="segav-kpi-footer">{status_html}{delta_html}</div>' if (status_html or delta_html) else ""
    help_attr = f' title="{_esc(help_text)}"' if help_text else ""
    _render_html(
        f"""<div class="segav-kpi-card"{help_attr} style="--segav-kpi-accent:{cfg['accent']}; --segav-kpi-soft:{cfg['bg']};">
  <div class="segav-kpi-head">
    <div class="segav-kpi-label">{_esc(label)}</div>
    <div class="segav-kpi-icon">{_esc(icon)}</div>
  </div>
  <div class="segav-kpi-value">{_esc(value)}</div>
  <div class="segav-kpi-subtitle">{_esc(subtitle)}</div>
  {progress_html}
  {footer_html}
</div>"""
    )


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
                kpi_card(**card)


def kpi_section(title: str, subtitle: str = "") -> None:
    inject_kpi_css()
    _render_html(
        f"""
<div class="segav-kpi-strip">
  <div class="segav-kpi-strip-title">{_esc(title)}</div>
  <div class="segav-kpi-strip-subtitle">{_esc(subtitle)}</div>
</div>
        """
    )
