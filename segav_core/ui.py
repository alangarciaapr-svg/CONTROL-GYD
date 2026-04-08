import streamlit as st


def inject_css():
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1rem; padding-bottom: 2rem;}
        /* Metric cards */
        div[data-testid="stMetric"]{
            padding: 14px 14px 10px 14px;
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 16px;
        }
        /* Dataframes */
        div[data-testid="stDataFrame"]{
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 14px;
            overflow: hidden;
        }
        /* Expander */
        details[data-testid="stExpander"]{
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 14px;
            padding: 6px 10px;
        }

/* Buttons */
div.stButton > button {
    border-radius: 14px !important;
    padding-top: 0.55rem !important;
    padding-bottom: 0.55rem !important;
}
/* Sidebar spacing */
section[data-testid="stSidebar"] .block-container {padding-top: 1rem;}


/* iOS-like look & feel */
html, body, [class*="css"]  {
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
section[data-testid="stSidebar"] { border-right: 1px solid rgba(49,51,63,0.12); }
section[data-testid="stSidebar"] .block-container { padding-top: 1rem; }

/* Cards */
.gyd-card {
    background: rgba(255,255,255,0.72);
    border: 1px solid rgba(49,51,63,0.10);
    border-radius: 18px;
    padding: 14px 16px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.06);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    margin-bottom: 12px;
}
.gyd-muted { opacity: 0.75; }

/* Buttons */
div.stButton > button, div.stDownloadButton > button {
    border-radius: 16px !important;
    padding: 0.62rem 0.9rem !important;
}

/* Tabs */
button[data-baseweb="tab"] {
    border-radius: 14px;
    margin-right: 6px;
    padding-left: 14px;
    padding-right: 14px;
}

/* Dataframe container */
[data-testid="stDataFrame"] {
    border-radius: 16px;
    border: 1px solid rgba(49,51,63,0.10);
    overflow: hidden;
}

/* Metric cards */
[data-testid="stMetric"] {
    border: 1px solid rgba(49,51,63,0.10);
    border-radius: 16px;
    padding: 10px 12px;
}

        </style>
        """,
        unsafe_allow_html=True,
    )


def ui_header(title: str, desc: str = ""):
    st.markdown(
        f"""
        <div class="gyd-card">
            <div style="font-size:1.35rem; font-weight:700; line-height:1.25;">{title}</div>
            {f'<div class="gyd-muted" style="margin-top:6px;">{desc}</div>' if desc else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


def ui_tip(text: str):
    st.info(text, icon="ℹ️")
