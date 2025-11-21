import streamlit as st

def load_global_ui():
    css = """
    <style>

    html, body, [class*="css"]  {
        font-family: 'Cambria', serif !important;
    }

    /* App background */
    .stApp {
        background: #0a0a0a;
    }

    /* Table (Glossy Neon) */
    .stDataFrame, .stDataEditor {
        background: rgba(255,255,255,0.05) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(255,255,255,0.12) !important;
        box-shadow: 0px 0px 15px rgba(0,255,255,0.15);
        backdrop-filter: blur(6px);
    }

    .stDataFrame th, .stDataEditor th {
        background: rgba(255,255,255,0.12) !important;
        color: #e5e5e5 !important;
        font-weight: 600 !important;
    }

    .stDataFrame td, .stDataEditor td {
        color: #dcdcdc !important;
        border-bottom: 1px solid rgba(255,255,255,0.08) !important;
    }

    .stDataFrame tr:hover td,
    .stDataEditor tr:hover td {
        background: rgba(0,255,255,0.10) !important;
        transition: 0.15s ease-in-out;
    }

    /* Titles */
    h1, h2, h3, h4, h5 {
        color: #e8e8e8 !important;
        font-weight: 600;
    }

    /* Divider */
    hr {
        border: 0;
        border-top: 1px solid rgba(255,255,255,0.2);
    }

    /* Scrollbar */
    ::-webkit-scrollbar {
        height: 8px;
        width: 8px;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(0,255,255,0.25);
        border-radius: 10px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(0,255,255,0.45);
    }

    </style>
    """
    st.markdown(css, unsafe_allow_html=True)
st.markdown("""
<style>
/* allow Pandas Styler cell background colors to show */
[data-testid="stDataFrame"] table td {
    background-color: inherit !important;
}
[data-testid="stDataFrame"] table th {
    background-color: inherit !important;
}
</style>
""", unsafe_allow_html=True)
neon_css = """
<style>

    /* ==== GLOSSY NEON CARD BACKGROUND ==== */
    .stDataFrame, .stDataEditor {
        background: rgba(255, 255, 255, 0.05) !important;
        backdrop-filter: blur(12px) saturate(180%) !important;
        -webkit-backdrop-filter: blur(12px) saturate(180%) !important;
        border-radius: 16px !important;
        border: 1px solid rgba(255, 255, 255, 0.20) !important;
        box-shadow: 0 0 25px rgba(0, 255, 255, 0.20),
                    0 0 40px rgba(0, 255, 255, 0.10) !important;
        overflow: hidden !important;
    }

    /* ==== HEADER NEON ==== */
    .stDataFrame th, .stDataEditor th {
        background: rgba(0, 0, 0, 0.40) !important;
        backdrop-filter: blur(10px) !important;
        color: #00faff !important;
        font-weight: 700 !important;
        text-shadow: 0 0 6px #00faff !important;
        border-bottom: 1px solid rgba(0, 255, 255, 0.25) !important;
        padding: 10px !important;
    }

    /* ==== CELL ==== */
    .stDataFrame td, .stDataEditor td {
        color: #e8e8e8 !important;
        padding: 7px 10px !important;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05) !important;
        font-weight: 500 !important;
    }

    /* ==== HOVER EFFECT ==== */
    .stDataFrame tr:hover td, .stDataEditor tr:hover td {
        background: rgba(0, 255, 255, 0.15) !important;
        box-shadow: inset 0 0 12px rgba(0, 255, 255, 0.30) !important;
        transition: all 0.25s ease-in-out !important;
    }

    /* ==== SCROLLBAR ==== */
    ::-webkit-scrollbar {
        height: 8px;
        width: 8px;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(0, 255, 255, 0.35);
        border-radius: 10px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(0, 255, 255, 0.60);
    }

</style>
"""

st.markdown(neon_css, unsafe_allow_html=True)
st.markdown("""
<style>

/* REMOVE INTERNAL SCROLLBARS */
[data-testid="stDataFrame"] div[style*="overflow"] {
    overflow-x: hidden !important;
}

/* EXPAND DATAFRAME FULL-WIDTH */
[data-testid="stDataFrame"] table {
    width: 100% !important;
    table-layout: fixed !important;
}

/* FIX COLUMN TEXT WRAPPING */
[data-testid="stDataFrame"] td, 
[data-testid="stDataFrame"] th {
    white-space: nowrap !important;
}

/* REMOVE EDITOR SCROLL */
[data-testid="stDataEditor"] div[style*="overflow"] {
    overflow-x: hidden !important;
}

</style>
""", unsafe_allow_html=True)
