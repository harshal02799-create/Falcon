import streamlit as st
# your other imports...

# import pandas as pd
# import streamlit as st
# import requests
# from datetime import datetime
# import streamlit.components.v1 as components
# import html

# # ============================================================
# # FETCH TRADINGVIEW DATA (same as your main board)
# # ============================================================
# from tradingview_screener import Query

# def fetch_tradingview_data():
#     n_rows, df = (
#         Query()
#         .select(
#             'name', 'exchange', 'close', 'high', 'close|1',
#             'change', 'price_52_week_high', 'High.All',
#             'volume', 'Value.Traded',
#             'market_cap_basic'
#         )
#         .set_markets('india')
#         .limit(9000)
#         .get_scanner_data()
#     )

#     df = df.rename(columns={
#         'name': 'Symbol',
#         'close': 'LTP',
#         'change': 'PcntChg'
#     }).fillna(0)

#     df["Symbol"] = df["Symbol"].str.upper()
#     return df


# # ============================================================
# # FETCH NSE PRICE BAND (same as main, but later filtered to SME)
# # ============================================================
# def fetch_price_band():
#     file_name = "sec_list.csv"
#     url = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"

#     session = requests.Session()
#     headers = {"User-Agent": "Mozilla/5.0"}
#     session.get("https://www.nseindia.com", headers=headers)
#     resp = session.get(url, headers=headers)

#     # FIX: use Python's built-in StringIO
#     from io import StringIO
#     df = pd.read_csv(StringIO(resp.text))

#     series_col = [c for c in df.columns if c.lower().strip() == "series"][0]
#     df["Symbol"] = df["Symbol"].str.upper()
#     return df

# # ============================================================
# # MERGE FUNCTION
# # ============================================================
# def merge_data():
#     nse_df = fetch_price_band()
#     tv_df = fetch_tradingview_data()

#     merged = nse_df.merge(tv_df, on="Symbol", how="left")

#     # ---------------------------------------------------------
#     # Detect Price Band Column (SME friendly)
#     # ---------------------------------------------------------
#     possible_cols = [
#         "Band", "Band (%)", "Band%", "Price band %", "Price Band",
#         "PriceBand", "Price band", "Band(%)"
#     ]

#     price_band_col = None
#     for col in merged.columns:
#         normalized = col.replace(" ", "").replace("%", "").lower()
#         for p in possible_cols:
#             if normalized == p.replace(" ", "").replace("%", "").lower():
#                 price_band_col = col
#                 break
#         if price_band_col:
#             break

#     # If STILL not found → show debug and stop
#     if not price_band_col:
#         st.error("❌ Price Band column not found in SME NSE file!")
#         st.write("Columns found:", list(merged.columns))
#         st.stop()

#     # ---------------------------------------------------------
#     # CLEAN PRICE BAND COLUMN
#     # ---------------------------------------------------------
#     merged[price_band_col] = (
#         merged[price_band_col]
#         .astype(str)
#         .str.replace("%", "")
#         .str.replace("No Band", "0", case=False)
#         .str.replace("-", "0")
#         .str.strip()
#     )

#     merged[price_band_col] = pd.to_numeric(merged[price_band_col], errors="coerce").fillna(0)

#     # ---------------------------------------------------------
#     # VALUE TRADE IN CR
#     # ---------------------------------------------------------
#     merged["ValueTrade(Cr)"] = merged["Value.Traded"] / 1_00_00_000

#     return merged, price_band_col

# # ============================================================
# # SME IPO LIST (API with security_type = SME)
# # ============================================================
# def fetch_sme_ipo_symbols():
#     url = "https://www.nseindia.com/api/public-past-issues?from_date=01-01-2025&to_date=" \
#           + datetime.now().strftime("%d-%m-%Y") + "&security_type=SME"

#     headers = {"User-Agent": "Mozilla/5.0"}
#     session = requests.Session()
#     session.get("https://www.nseindia.com", headers=headers)
#     resp = session.get(url, headers=headers)

#     try:
#         data = resp.json()
#     except:
#         return []

#     rows = data["data"] if "data" in data else data

#     ipo_syms = []
#     for row in rows:
#         s = str(row.get("symbol", "")).strip().upper()
#         if s:
#             ipo_syms.append(s)

#     return list(set(ipo_syms))


# # ============================================================
# # STYLE BAND
# # ============================================================
# def get_color_for_band(p, band):
#     p = float(p)

#     if band == 5:
#         if p >= 4.5: return "blue"
#         if p >= 3: return "green"
#     if band == 10:
#         if p >= 9: return "blue"
#         if p >= 6: return "green"
#     if band == 20:
#         if p >= 19: return "blue"
#         if p >= 16: return "green"
#         if p >= 12: return "yellow"
#     return None

# def style_band(df, band):
#     styles = pd.DataFrame("", index=df.index, columns=df.columns)

#     for idx, row in df.iterrows():
#         val = row["PcntChg"]
#         color = get_color_for_band(val, band)

#         if color == "blue":
#             bg = "background-color: #0046FF; color:white; font-weight:600;"
#         elif color == "green":
#             bg = "background-color: #00CC44; color:white; font-weight:600;"
#         elif color == "yellow":
#             bg = "background-color: #055F6A; color:white; font-weight:600;"
#         elif color == "red":
#             bg = "background-color: #FF3B30; color:white; font-weight:600;"
#         elif color == "orange":
#             bg = "background-color: #055F6A; color:white; font-weight:600;"
#         else:
#             bg = ""

#         if bg:
#             styles.loc[idx] = [bg] * len(df.columns)

#     # SAFE FORMATTER
#     return (
#         df.style
#         .apply(lambda _: styles, axis=None)
#         .format({
#             "LTP": lambda x: f"{float(x):.2f}",
#             "PcntChg": lambda x: f"{float(x):.2f}",
#             "ValueTrade(Cr)": lambda x: f"{float(x):.2f}",
#         })
#     )

# # ============================================================
# # SME PAGE UI
# # ============================================================
# st.set_page_config(layout="wide")
# st.title("📘 SME Dashboard")

# with st.spinner("Loading SME data..."):
#     merged, price_band_col = merge_data()

# # FILTER SME
# series_col = [c for c in merged.columns if c.lower().strip() == "series"][0]
# sme = merged[merged[series_col].str.upper() == "SM"].copy()

# # SPLIT BANDS
# band5 = sme[sme[price_band_col] == 5]
# band10 = sme[sme[price_band_col] == 10]
# band20 = sme[sme[price_band_col] == 20]

# final_cols = ["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]


# # ============================================================
# # ROW 1 — BAND TABLES
# # ============================================================
# c1, c2, c3 = st.columns(3)

# with c1:
#     st.subheader("🟦 SME — 5% BAND")
#     st.dataframe(style_band(band5[final_cols], 5), use_container_width=True, hide_index=True)

# with c2:
#     st.subheader("🟩 SME — 10% BAND")
#     st.dataframe(style_band(band10[final_cols], 10), use_container_width=True, hide_index=True)

# with c3:
#     st.subheader("🟧 SME — 20% BAND")
#     st.dataframe(style_band(band20[final_cols], 20), use_container_width=True, hide_index=True)


# st.divider()

# # ============================================================
# # ROW 2 — VALUE TRADE TABLE (NEW COLOR LOGIC)
# # ============================================================
# st.subheader("💰 SME — ValueTrade Levels")

# vt = sme[["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]].copy()
# vt = vt[vt["ValueTrade(Cr)"] > 0.50]     # minimum 50 lakhs

# rows = []
# for _, r in vt.iterrows():
#     v = r["ValueTrade(Cr)"]

#     # COLOR CONDITIONS
#     if v > 5:
#         row_color = "#0046FF"        # dark blue
#     elif v > 2.5:
#         row_color = "#0C8F8A"        # teal
#     elif v > 1:
#         row_color = "#00CC44"        # green
#     else:
#         row_color = "#01363D"        # 50 lakhs color (your screenshot)

#     rows.append(f"""
#         <tr style='background:{row_color};color:white;font-weight:600;'>
#             <td style='padding:8px;'>{r['Symbol']}</td>
#             <td style='padding:8px;text-align:center'>{r['LTP']:.2f}</td>
#             <td style='padding:8px;text-align:center'>{r['PcntChg']:.2f}</td>
#             <td style='padding:8px;text-align:center'>{r['ValueTrade(Cr)']:.2f}</td>
#         </tr>
#     """)

# table_html = f"""
# <table style='width:100%;border-collapse:separate;border-spacing:0;background:rgba(255,255,255,0.03);
# border-radius:12px;overflow:hidden;border:1px solid rgba(255,255,255,0.15);font-family:Cambria;'>
# <thead>
# <tr>
# <th style='padding:10px;text-align:left'>Symbol</th>
# <th style='padding:10px;text-align:center'>LTP</th>
# <th style='padding:10px;text-align:center'>PcntChg</th>
# <th style='padding:10px;text-align:center'>ValueTrade(Cr)</th>
# </tr>
# </thead>
# <tbody>
# {''.join(rows)}
# </tbody>
# </table>
# """

# components.html(table_html, height=520, scrolling=True)


# st.divider()

# # ============================================================
# # ROW 3 — SME IPO TABLE
# # ============================================================
# st.subheader("🟣 SME IPO — Last 1 Year")

# ipo_syms = fetch_sme_ipo_symbols()
# ipo_df = sme[sme["Symbol"].isin(ipo_syms)]

# if ipo_df.empty:
#     st.info("No SME IPO data found.")
# else:
#     st.dataframe(
#         ipo_df[["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]],
#         hide_index=True,
#         use_container_width=True
#     )












import streamlit as st
from nav import nav_menu

nav_menu()

import pandas as pd
import requests
from datetime import datetime
import os
import io
from tradingview_screener import Query
import streamlit as st
import html
import streamlit.components.v1 as components
# REMOVE TOP SPACE / PADDING
st.markdown("""
    <style>
        .block-container {
            padding-top: 1.2rem;
        }
    </style>
""", unsafe_allow_html=True)

# -----------------------------
# CONFIG
# -----------------------------
AUTO_REFRESH_MS = 30_000  # refresh interval

# -----------------------------
# UTIL / STYLING
# -----------------------------
def get_color_for_band(pcnt, band):
    """Band-based color logic (same as main board)."""
    try:
        v = float(pcnt)
    except:
        return None

    if band == 5:
        if v >= 4.5: return "blue"
        if v >= 3:   return "green"
        if v <= -4.5: return "red"
        if v <= -3:   return "yellow"

    if band == 10:
        if v >= 9: return "blue"
        if v >= 6: return "green"
        if v <= -9: return "red"
        if v <= -6: return "yellow"

    if band == 20:
        if v >= 19: return "blue"
        if v >= 16: return "green"
        if v >= 12: return "yellow"
        if v <= -19: return "red"
        if v <= -16: return "yellow"
        if v <= -12: return "orange"

    return None

# ValueTrade color thresholds (SME)
def get_valuetrade_color(v_cr):
    """Return row style CSS based on ValueTrade(Cr) thresholds for SME."""
    try:
        v = float(v_cr)
    except:
        return ""

    # thresholds per your request (Cr)
    # >0.50 Cr (50 lacs) -> Light Blue (use image color)
    # >1 Cr -> Green 00CC44
    # >2.5 Cr -> Yellow/Teal 055F6a
    # >5 Cr -> Dark Blue 004677
    if v >= 5:
        return "background-color:#0046FF; color:white; font-weight:600;"
    if v >= 2.5:
        return "background-color:#055F6A; color:white; font-weight:600;"
    if v >= 1:
        return "background-color:#00CC44; color:white; font-weight:600;"
    if v >= 0.50:
        # light blue (use your image color)
        return "background-color:#055F6A; color:white; font-weight:600;"
    return ""

# Force numeric safe formatting for display (used before styling)
def safe_round_df(df, cols, ndigits=2):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(ndigits)
    return df

# Full-row Styler builder for band tables
def style_band_fullrow(df, band):
    # ensure numeric columns are numeric
    safe_round_df(df, ["LTP", "PcntChg", "ValueTrade(Cr)"], 2)

    # create empty styles DataFrame
    styles = pd.DataFrame("", index=df.index, columns=df.columns)

    for idx, row in df.iterrows():
        color_key = get_color_for_band(row.get("PcntChg", 0), band)

        if color_key == "blue":
            bg = "background-color: #0046FF; color:white; font-weight:600;"
        elif color_key == "green":
            bg = "background-color: #00CC44; color:white; font-weight:600;"
        elif color_key == "yellow":
            bg = "background-color: #055F6A; color:white; font-weight:600;"
        elif color_key == "red":
            bg = "background-color: #FF3B30; color:white; font-weight:600;"
        elif color_key == "orange":
            bg = "background-color: #FF9500; color:white; font-weight:600;"
        else:
            bg = ""

        if bg:
            styles.loc[idx] = [bg] * len(df.columns)

    # apply styling
    styler = df.style.apply(lambda _: styles, axis=None)

    # numeric formatting
    styler = styler.format({
        "LTP": "{:.2f}",
        "PcntChg": "{:.2f}",
        "ValueTrade(Cr)": "{:.2f}",
    })

    # -------------------------------
    # ⭐ CENTER ALIGN ALL COLUMNS
    # -------------------------------
    styler = styler.set_properties(**{
        'text-align': 'center'
    })

    # center-align header too
    styler = styler.set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'center')]}
    ])

    return styler
# REMOVE TOP SPACE / PADDING
st.markdown("""
    <style>
        .block-container {
            padding-top: 1.2rem;
        }
    </style>
""", unsafe_allow_html=True)
# Row styling for valuetrade table (based on ValueTrade thresholds)
def html_table_from_df_with_valuecolor(df, cols, value_col="ValueTrade(Cr)", height=520):
    """Build a glossy HTML table and color rows based on value_col thresholds."""
    rows_html = []
    for _, r in df.iterrows():
        row_style = get_valuetrade_color(r.get(value_col, 0))
        cells = []
        for c in cols:
            val = r.get(c, "")
            if pd.isna(val):
                display = "-"
            else:
                if isinstance(val, (int, float)):
                    if c in ["LTP", "PcntChg", value_col]:
                        display = f"{val:.2f}"
                    else:
                        display = str(val)
                else:
                    display = html.escape(str(val))
            # align numeric to center
            if c in ["LTP", "PcntChg", value_col]:
                cells.append(f"<td style='padding:8px; text-align:center'>{display}</td>")
            else:
                cells.append(f"<td style='padding:8px; white-space:nowrap'>{display}</td>")
        rows_html.append(f"<tr style='{row_style}'>"+ "".join(cells) + "</tr>")

    header_cells = []
    for c in cols:
        if c in ["LTP", "PcntChg", value_col]:
            header_cells.append(f"<th style='padding:10px; text-align:center'>{html.escape(c)}</th>")
        else:
            header_cells.append(f"<th style='padding:10px; text-align:left'>{html.escape(c)}</th>")

    header_html = "<thead><tr>" + "".join(header_cells) + "</tr></thead>"

    glossy_css = """
    <style>
      .glossy-table {
        width:100%;
        border-collapse:separate;
        border-spacing:0;
        background: rgba(255,255,255,0.02);
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.06);
        font-family: Cambria, serif;
      }
      .glossy-table th {
        background: rgba(255,255,255,0.03);
        color: #ddd;
        font-weight:600;
      }
      .glossy-table td {
        color:#ddd;
        border-bottom: 1px solid rgba(255,255,255,0.02);
      }
      .glossy-table tbody tr:hover td {
        background: rgba(255,255,255,0.03);
      }
    </style>
    """

    html_table = f"""
    {glossy_css}
    <table class='glossy-table'>
      {header_html}
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    """
    components.html(html_table, height=height, scrolling=True)

# -----------------------------
# DATA FETCHERS
# -----------------------------
def fetch_tradingview_data():
    """Fetch LTP data from tradingview_screener (as before)."""
    n_rows, tradingview = (
        Query()
        .select(
            'name', 'exchange', 'close', 'high', 'close|1',
            'change', 'price_52_week_high', 'High.All',
            'volume', 'Value.Traded',
            'market_cap_basic'
        )
        .set_markets('india')
        .limit(9000)
        .get_scanner_data()
    )

    tradingview = (
        tradingview.rename(columns={'name': 'Symbol', 'close': 'LTP', 'change': 'PcntChg'})
        .round(2)
        .fillna(0)
    )

    tradingview = tradingview.sort_values(
        by='exchange',
        key=lambda x: x.eq('NSE'),
        ascending=False
    ).drop_duplicates(subset=['Symbol'], keep='first')

    tradingview["Symbol"] = tradingview["Symbol"].str.upper()
    return tradingview

def fetch_price_band():
    """
    Download NSE sec_list.csv and return a DataFrame.
    Falls back to saved file if available.
    """
    file_name = "sec_list.csv"
    url = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com",
    }

    os.makedirs("price_band_files", exist_ok=True)
    save_path = f"price_band_files/{file_name}"

    # Try network first
    try:
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        resp = session.get(url, headers=headers, timeout=30)
        if resp.status_code == 200 and resp.content:
            with open(save_path, "wb") as f:
                f.write(resp.content)
            # read from bytes buffer (avoid pandas.compat)
            try:
                df = pd.read_csv(io.BytesIO(resp.content))
            except Exception:
                # try text variant
                df = pd.read_csv(io.StringIO(resp.text))
            # ensure Symbol column uppercase
            if "Symbol" in df.columns:
                df["Symbol"] = df["Symbol"].astype(str).str.upper()
            return df
        else:
            # fallback to saved file if exists
            if os.path.exists(save_path):
                return pd.read_csv(save_path)
            raise Exception(f"NSE fetch failed: HTTP {resp.status_code}")
    except Exception as e:
        # fallback to saved file if exists
        if os.path.exists(save_path):
            return pd.read_csv(save_path)
        raise

def fetch_ipo_symbols_last_1_year_sme():
    """
    Fetch IPO list from NSE but for SME (security_type=SME).
    Returns set of symbols.
    """
    url = "https://www.nseindia.com/api/public-past-issues"
    from_date = "01-01-2025"  # fixed as prior
    today = datetime.now().strftime("%d-%m-%Y")
    final_url = f"{url}?from_date={from_date}&to_date={today}&security_type=SME"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com/"
    }

    session = requests.Session()
    try:
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        resp = session.get(final_url, headers=headers, timeout=30)
        data = resp.json()
    except Exception:
        return []

    rows = []
    if isinstance(data, dict) and "data" in data:
        rows = data["data"]
    elif isinstance(data, list):
        rows = data
    else:
        return []

    symbols = []
    for row in rows:
        sym = str(row.get("symbol", "")).strip().upper()
        if sym:
            symbols.append(sym)
    return list(set(symbols))

# -----------------------------
# MERGE + PREP
# -----------------------------
def merge_data():
    nse_df = fetch_price_band()
    tv_df = fetch_tradingview_data()

    # Ensure Symbol exists in nse_df
    if "Symbol" not in nse_df.columns:
        # try detect alternative column and rename
        possible_symbol_cols = [c for c in nse_df.columns if "symbol" in c.lower()]
        if possible_symbol_cols:
            nse_df = nse_df.rename(columns={possible_symbol_cols[0]: "Symbol"})
        else:
            raise Exception("Symbol column not found in NSE file. Columns: " + ", ".join(nse_df.columns))

    nse_df["Symbol"] = nse_df["Symbol"].astype(str).str.upper()

    merged = nse_df.merge(tv_df, on="Symbol", how="left")

    # Detect price band column robustly
    possible_cols = ["Price Band", "Price band", "Price Band %", "Price band %", "Band", "Band %", "PriceBand", "Band(%)"]
    price_band_col = None
    for col in merged.columns:
        normalized = col.strip().lower().replace(" ", "")
        for p in possible_cols:
            if normalized == p.lower().replace(" ", ""):
                price_band_col = col
                break
        if price_band_col:
            break

    if price_band_col is None:
        # try fuzzy detection by keywords
        for col in merged.columns:
            if "band" in col.lower() and "%" in col:
                price_band_col = col
                break

    if price_band_col is None:
        # last resort: find any column containing 'band'
        for col in merged.columns:
            if "band" in col.lower():
                price_band_col = col
                break

    if price_band_col is None:
        raise Exception("❌ Price Band column not found! Columns: " + ", ".join(merged.columns))

    # Clean price band values
    merged[price_band_col] = (
        merged[price_band_col].astype(str)
        .str.replace('%', '', regex=False)
        .str.replace('No Band', '0', case=False)
        .str.replace('NOBAND', '0', case=False)
        .str.replace('-', '0')
        .str.strip()
    )
    merged[price_band_col] = pd.to_numeric(merged[price_band_col], errors='coerce').fillna(0)

    # create ValueTrade(Cr)
    if "Value.Traded" in merged.columns:
        merged["ValueTrade(Cr)"] = pd.to_numeric(merged["Value.Traded"], errors="coerce") / 1_00_00_000
    else:
        # try alternative column names
        possible_vt = [c for c in merged.columns if "value" in c.lower() and "traded" in c.lower()]
        if possible_vt:
            merged["ValueTrade(Cr)"] = pd.to_numeric(merged[possible_vt[0]], errors="coerce") / 1_00_00_000
        else:
            merged["ValueTrade(Cr)"] = 0.0

    return merged, price_band_col
# REMOVE TOP SPACE / PADDING
st.markdown("""
    <style>
        .block-container {
            padding-top: 1.2rem;
        }
    </style>
""", unsafe_allow_html=True)
# -----------------------------
# STREAMLIT UI
# -----------------------------
st.set_page_config(page_title="SME Dashboard", layout="wide")
st.markdown("""
<style>
    * { font-family: Cambria, serif !important; font-size: 16px !important; }
</style>
""", unsafe_allow_html=True)

# glossy CSS (reused)
glossy_table_css = """
<style>
    .stDataFrame, .stDataEditor {
        background: rgba(255,255,255,0.03) !important;
        border-radius: 12px !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        padding: 6px !important;
    }
</style>
"""
st.markdown(glossy_table_css, unsafe_allow_html=True)

# Auto refresh
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=AUTO_REFRESH_MS, key='sme_refresh')

with st.spinner("Loading SME data..."):
    merged, price_band_col = merge_data()

# Sidebar filters
st.sidebar.title("SME Filters")
search = st.sidebar.text_input("🔍 Search Symbol / Name (substring)")
min_value_filter = st.sidebar.number_input("Min ValueTrade (Cr) filter for some tables", min_value=0.0, value=0.5, step=0.1)

# filter by search if provided
if search:
    s = search.upper()
    # check there is a Name column to search as second column, otherwise search all string columns
    name_col = None
    possible_name_cols = [c for c in merged.columns if "security" in c.lower() or "name" in c.lower()]
    if possible_name_cols:
        name_col = possible_name_cols[0]
    if name_col:
        merged = merged[merged["Symbol"].str.contains(s, na=False) | merged[name_col].str.upper().str.contains(s, na=False)]
    else:
        merged = merged[merged["Symbol"].str.contains(s, na=False)]

# Narrow to SME segment
series_col_candidates = [c for c in merged.columns if c.strip().lower() == "series"]
if not series_col_candidates:
    # try fuzzy find
    series_col_candidates = [c for c in merged.columns if "series" in c.lower()]
if not series_col_candidates:
    st.error("Series column not found in NSE file. Columns: " + ", ".join(merged.columns))
    st.stop()
series_col = series_col_candidates[0]
sme_df = merged[merged[series_col].astype(str).str.upper() == "SM"].copy()
st.success(f"SME stocks loaded: {len(sme_df)}")

# Prepare band splits for SME
band5 = sme_df[sme_df[price_band_col] == 5].copy().reset_index(drop=True)
band10 = sme_df[sme_df[price_band_col] == 10].copy().reset_index(drop=True)
band20 = sme_df[sme_df[price_band_col] == 20].copy().reset_index(drop=True)

# Ensure required numeric cols
for df in [band5, band10, band20, sme_df]:
    for col in ["LTP", "PcntChg", "ValueTrade(Cr)", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

# -----------------------------
# Layout Row 1: three band tables (no No-Band)
# -----------------------------
st.header("SME — Price Band Dashboard")
row1 = st.columns(3)

final_cols = ["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]

with row1[0]:
    st.subheader("🟦 5% BAND")
    if band5.empty:
        st.info("No SME stocks in 5% band.")
    else:
        df_show = band5[[c for c in final_cols if c in band5.columns]].copy()
        df_show = df_show.sort_values("PcntChg", ascending=False)
        st.dataframe(style_band_fullrow(df_show, 5), use_container_width=True, hide_index=True)

with row1[1]:
    st.subheader("🟩 10% BAND")
    if band10.empty:
        st.info("No SME stocks in 10% band.")
    else:
        df_show = band10[[c for c in final_cols if c in band10.columns]].copy()
        df_show = df_show.sort_values("PcntChg", ascending=False)
        st.dataframe(style_band_fullrow(df_show, 10), use_container_width=True, hide_index=True)

with row1[2]:
    st.subheader("🟧 20% BAND")
    if band20.empty:
        st.info("No SME stocks in 20% band.")
    else:
        df_show = band20[[c for c in final_cols if c in band20.columns]].copy()
        df_show = df_show.sort_values("PcntChg", ascending=False)
        st.dataframe(style_band_fullrow(df_show, 20), use_container_width=True, hide_index=True)


st.divider()

# -----------------------------
# Layout Row 2: 52W High & ValueTrade > 0.50 Cr
# -----------------------------
row2 = st.columns([1, 1])
with row2[0]:
    st.subheader("📌 52W High & ATH (SME)")

    # detect 52W column
    col_52 = None
    for c in sme_df.columns:
        if "52" in c.lower() and "week" in c.lower():
            col_52 = c
            break
    if col_52 is None:
        for c in sme_df.columns:
            if "price_52" in c.lower() or "52week" in c.lower():
                col_52 = c
                break

    if not col_52:
        st.warning("52W high column not detected.")
    else:
        tmp = sme_df.copy()
        tmp = tmp[["Symbol", "LTP", "PcntChg", col_52, "ValueTrade(Cr)"]].rename(
            columns={col_52: "52W_High"}
        )

        # convert to numeric safely
        tmp["LTP"] = pd.to_numeric(tmp["LTP"], errors="coerce").round(2)
        tmp["PcntChg"] = pd.to_numeric(tmp["PcntChg"], errors="coerce").round(2)
        tmp["52W_High"] = pd.to_numeric(tmp["52W_High"], errors="coerce").round(2)
        tmp["ValueTrade(Cr)"] = pd.to_numeric(tmp["ValueTrade(Cr)"], errors="coerce").round(2)

        # Calculate distance
        tmp["Dist_52W(%)"] = (((tmp["LTP"] - tmp["52W_High"]) / tmp["52W_High"]) * 100).round(2)

        # Sort
        tmp = tmp.sort_values("Dist_52W(%)", ascending=False).head(50).reset_index(drop=True)

        # Build rows
        rows_html = []
        for _, r in tmp.iterrows():
            row_style = ""
            try:
                if pd.notna(r["Dist_52W(%)"]) and abs(float(r["Dist_52W(%)"])) < 1:
                    row_style = "background-color:#00CC44; color:white; font-weight:600;"
            except:
                pass

            def safe_fmt(v):
                try:
                    return f"{float(v):.2f}"
                except:
                    return "-"

            cells = [
                f"<td style='padding:8px; white-space:nowrap'>{html.escape(str(r['Symbol']))}</td>",
                f"<td style='padding:8px; text-align:center'>{safe_fmt(r['LTP'])}</td>",
                f"<td style='padding:8px; text-align:center'>{safe_fmt(r['PcntChg'])}</td>",
                f"<td style='padding:8px; text-align:center'>{safe_fmt(r['52W_High'])}</td>",
                f"<td style='padding:8px; text-align:center'>{safe_fmt(r['ValueTrade(Cr)'])}</td>",
                f"<td style='padding:8px; text-align:center'>{safe_fmt(r['Dist_52W(%)'])}</td>"
            ]

            rows_html.append(f"<tr style='{row_style}'>" + "".join(cells) + "</tr>")

        # Header with ValueTrade column included
        header_html = """
        <thead>
          <tr>
            <th style='text-align:left; padding:10px'>Symbol</th>
            <th style='text-align:center; padding:10px'>LTP</th>
            <th style='text-align:center; padding:10px'>PcntChg</th>
            <th style='text-align:center; padding:10px'>52W_High</th>
            <th style='text-align:center; padding:10px'>ValueTrade(Cr)</th>
            <th style='text-align:center; padding:10px'>Dist_52W(%)</th>
          </tr>
        </thead>
        """

        table_style = """
        <style>
          .glossy-table {
            width:100%;
            border-collapse:separate;
            border-spacing:0;
            background: rgba(255,255,255,0.02);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.06);
            font-family: Cambria, serif;
            color: #ddd;
          }
          .glossy-table th {
            background: rgba(255,255,255,0.03);
            color: #ddd;
            font-weight:600;
          }
          .glossy-table td {
            color:#ddd;
            border-bottom: 1px solid rgba(255,255,255,0.02);
          }
        </style>
        """

        html_table = f"""
        {table_style}
        <table class='glossy-table'>
          {header_html}
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
        """

        components.html(html_table, height=520, scrolling=True)


with row2[1]:
    st.subheader("💰 ValueTrade > 0.50 Cr (SME)")
    # use the sme_df ValueTrade(Cr) column and filter > 0.50 (50 lacs)
    vt_df = sme_df.copy()
    if "ValueTrade(Cr)" not in vt_df.columns:
        vt_df["ValueTrade(Cr)"] = 0.0
    vt_filtered = vt_df[vt_df["ValueTrade(Cr)"] >= 0.50].copy()
    if vt_filtered.empty:
        st.info("No SME stocks with ValueTrade >= 0.50 Cr.")
    else:
        # pick columns to show
        cols_show = [c for c in ["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"] if c in vt_filtered.columns]
        vt_filtered = vt_filtered.sort_values("ValueTrade(Cr)", ascending=False).reset_index(drop=True)
        html_table_from_df_with_valuecolor(vt_filtered[cols_show], cols_show, value_col="ValueTrade(Cr)", height=520)

st.divider()

# -----------------------------
# Layout Row 3: Breakout & IPO (last 1Y SME, ValueTrade > 1 Cr)
# -----------------------------
row3 = st.columns([1, 1])

with row3[0]:
    st.subheader("🔥 Breakout Candidates (SME)")

    # breakout detection
    bo5 = band5[band5.get("PcntChg", 0) > 4.5]
    bo10 = band10[band10.get("PcntChg", 0) > 9.5]
    bo20 = band20[band20.get("PcntChg", 0) > 19.5]

    # assign Band column
    if not bo5.empty:
        bo5["Band"] = "5%"
    if not bo10.empty:
        bo10["Band"] = "10%"
    if not bo20.empty:
        bo20["Band"] = "20%"

    # combine all
    bo_all = pd.concat([bo5, bo10, bo20], ignore_index=True)

    if bo_all.empty:
        st.info("No breakout candidates at the moment.")
    else:
        bo_all = bo_all.sort_values("PcntChg", ascending=False).reset_index(drop=True)

        # -------------------------
        # Row Color logic per band
        # -------------------------
        def row_color(band):
            if band == "20%":
                return "background-color:#0046FF; color:white; font-weight:600;"
            if band == "10%":
                return "background-color:#00CC44; color:white; font-weight:600;"
            if band == "5%":
                return "background-color:#055F6A; color:white; font-weight:600;"
            return ""

        rows_html = []
        for _, r in bo_all.iterrows():
            style = row_color(r["Band"])
            rows_html.append(f"""
                <tr style="{style}">
                    <td style="padding:8px; white-space:nowrap">{r['Symbol']}</td>
                    <td style="padding:8px; text-align:center">{r['LTP']:.2f}</td>
                    <td style="padding:8px; text-align:center">{r['PcntChg']:.2f}</td>
                    <td style="padding:8px; text-align:center">{r['ValueTrade(Cr)']:.2f}</td>
                    <td style="padding:8px; text-align:center">{r['Band']}</td>
                </tr>
            """)

        header_html = """
        <thead>
            <tr>
                <th style="padding:10px;text-align:left">Symbol</th>
                <th style="padding:10px;text-align:center">LTP</th>
                <th style="padding:10px;text-align:center">PcntChg</th>
                <th style="padding:10px;text-align:center">ValueTrade(Cr)</th>
                <th style="padding:10px;text-align:center">Band</th>
            </tr>
        </thead>
        """

        table_css = """
        <style>
            .breakout-table {
                width:100%;
                border-collapse:separate;
                border-spacing:0;
                background: rgba(255,255,255,0.02);
                border-radius: 12px;
                overflow:hidden;
                border: 1px solid rgba(255,255,255,0.06);
                font-family: Cambria, serif;
            }
            .breakout-table th {
                background: rgba(255,255,255,0.03);
                color:#ddd;
                font-weight:600;
            }
            .breakout-table td {
                color:#ddd;
                border-bottom:1px solid rgba(255,255,255,0.03);
            }
        </style>
        """

        html_table = f"""
        {table_css}
        <table class="breakout-table">
            {header_html}
            <tbody>
                {''.join(rows_html)}
            </tbody>
        </table>
        """

        components.html(html_table, height=520, scrolling=True)

with row3[1]:
    st.subheader("🟣 IPO (Last 1Y SME) — ValueTrade > 1 Cr")
    ipo_syms = fetch_ipo_symbols_last_1_year_sme()
    if not ipo_syms:
        st.info("No SME IPO info fetched or none in the last 1 year.")
    else:
        ipo_df = sme_df[sme_df["Symbol"].isin(ipo_syms)].copy()
        if ipo_df.empty:
            st.info("No SME IPO symbols present in current market data.")
        else:
            # filter by ValueTrade > 1 Cr
            ipo_df["ValueTrade(Cr)"] = pd.to_numeric(ipo_df["ValueTrade(Cr)"], errors="coerce")
            ipo_filtered = ipo_df[ipo_df["ValueTrade(Cr)"] >= 1.0].copy()
            if ipo_filtered.empty:
                st.info("No SME IPOs with ValueTrade >= 1 Cr.")
            else:
                cols = [c for c in ["Symbol", "LTP", "PcntChg", "MarketCap(Cr)", "ValueTrade(Cr)"] if c in ipo_filtered.columns]
                # if MarketCap not present, try to map from tradingview column
                if "MarketCap(Cr)" not in ipo_filtered.columns:
                    # detect market cap column
                    mc = None
                    for c in ipo_filtered.columns:
                        if "market_cap" in c.lower():
                            mc = c
                            break
                    if mc:
                        ipo_filtered[mc] = pd.to_numeric(ipo_filtered[mc], errors="coerce") / 1_00_00_000
                        ipo_filtered = ipo_filtered.rename(columns={mc: "MarketCap(Cr)"})
                        if "MarketCap(Cr)" not in cols:
                            cols.append("MarketCap(Cr)")

                ipo_filtered = safe_round_df(ipo_filtered, ["LTP", "PcntChg", "ValueTrade(Cr)", "MarketCap(Cr)"], 2)
                # color rows per value thresholds
                html_table_from_df_with_valuecolor(ipo_filtered[cols].sort_values("ValueTrade(Cr)", ascending=False), cols, value_col="ValueTrade(Cr)", height=520)

st.divider()

# Footer summary
st.markdown(f"""
**Summary (SME)**  
Total SME rows in NSE file: **{len(sme_df)}**  
5% band: **{len(band5)}** | 10% band: **{len(band10)}** | 20% band: **{len(band20)}**
""")
