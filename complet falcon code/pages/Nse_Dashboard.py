
import streamlit as st
from nav import nav_menu

nav_menu()   # ← add menu here

# st.title("📊 NSE Dashboard")






# ============================================================
# 📥 ONE-TIME PER DAY DOWNLOAD — NSE sec_list.csv
# ============================================================
import os
import requests
from datetime import datetime
import streamlit as st

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
NSE_FOLDER = os.path.join(PROJECT_ROOT, "nse_files")
PRICE_DATA = os.path.join(NSE_FOLDER, "PRICE_BAND_DATA")

os.makedirs(PRICE_DATA, exist_ok=True)

STATUS_FILE = os.path.join(PRICE_DATA, "last_update.txt")
CSV_PATH = os.path.join(PRICE_DATA, "sec_list.csv")


def get_last_update():
    if os.path.exists(STATUS_FILE):
        return open(STATUS_FILE, "r", encoding="utf-8").read().strip()
    return None


def set_last_update(date_str):
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        f.write(date_str)


def download_price_band_file():
    url = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com"
    }

    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers)
        r = session.get(url, headers=headers, timeout=30)
    except Exception as e:
        return False, f"❌ Download error: {e}"

    if r.status_code != 200:
        return False, f"❌ HTTP {r.status_code} while downloading sec_list.csv"

    try:
        with open(CSV_PATH, "wb") as f:
            f.write(r.content)
    except Exception as e:
        return False, f"❌ Failed saving file: {e}"

    today = datetime.now().strftime("%Y-%m-%d")
    set_last_update(today)
    return True, f"✔ Price Band File Updated Today ({today})"


def nse_download_button_ui():
    # st.header("📥 NSE Price Band File (Daily Download)")

    today = datetime.now().strftime("%Y-%m-%d")
    last = get_last_update()

    if last == today:
        st.success(f"✔ Already updated today ({today})")
        return True

    st.info("NSE price band file not downloaded today.")

    if st.button("⬇ Download Today's NSE File"):
        with st.spinner("Downloading NSE Price Band File..."):
            ok, msg = download_price_band_file()
        if ok:
            st.success(msg)
            return True
        else:
            st.error(msg)
            return False

    return False


# ---------------------------
# CALL UI at top — must be FIRST before dashboard loads
# ---------------------------
updated_today = nse_download_button_ui()

if not updated_today:
    st.warning("Please download today's NSE Price Band file to continue.")
    st.stop()

# ✔ Now dashboard code continues below this line...
PRICE_BAND_FILE_PATH = CSV_PATH
print("Using NSE price band file:", PRICE_BAND_FILE_PATH)
























import pandas as pd
import requests
from datetime import datetime, timedelta
import os
from tradingview_screener import Query
import streamlit as st
import time

# ============================================================
# COLOR LOGIC FOR ALL BAND TABLES
# ============================================================
def get_color_for_band(pcnt, band):
    try:
        v = float(pcnt)
    except:
        return None

    # ============= POSITIVE SIDE =============
    if band == 5:
        if v >= 4.5: return "blue"
        if v >= 3:   return "green"

    if band == 10:
        if v >= 9: return "blue"
        if v >= 6: return "green"

    if band == 20:
        if v >= 19: return "blue"
        if v >= 16: return "green"
        if v >= 12: return "yellow"

    # ============= NEGATIVE SIDE =============
    if band == 5:
        if v <= -4.5: return "red"
        if v <= -3:   return "yellow"

    if band == 10:
        if v <= -9: return "red"
        if v <= -6: return "yellow"

    if band == 20:
        if v <= -19: return "red"
        if v <= -16: return "yellow"
        if v <= -12: return "orange"

    return None
def format_2(x):
    try:
        return f"{float(x):.2f}"
    except:
        return x


# ============================================================
# 🔵 FETCH TRADINGVIEW LTP DATA
# ============================================================
def fetch_tradingview_data():
    n_rows, tradingview = (
        Query()
        .select(
            'name', 'exchange', 'close', 'high', 'close|1',
            'change', 'price_52_week_high', 'High.All',
            'volume', 'Value.Traded',
            'market_cap_basic'  # 🔥 REQUIRED FOR IPO TABLE
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


# ============================================================
# 🔽 DOWNLOAD NSE PRICE BAND LIST (YESTERDAY)
# ============================================================
# ============================================================
# READ LOCAL DOWNLOADED NSE PRICE BAND FILE
# ============================================================
def fetch_price_band():
    # Read file downloaded earlier by the button
    df = pd.read_csv(PRICE_BAND_FILE_PATH)

    # ---- FILTER EQ ----
    series_col = [c for c in df.columns if c.strip().lower() == "series"][0]
    df = df[df[series_col].str.upper() == "EQ"]

    # ---- Remove Remarks ----
    if "Remarks" in df.columns:
        df = df.drop(columns=["Remarks"])

    # ---- SECURITY NAME COLUMN ----
    sec_name_col = [c for c in df.columns if "security" in c.lower()][0]

    # ---- Remove ETFs / BEES / FUNDS etc ----
    remove_keywords = [
        "ETF", "FUND", "BEES", "LIQUID",
        "NIFTY", "MUTUAL", "MUTUL", "LIQIUID"
    ]
    pattern = "|".join(remove_keywords)
    df = df[~df[sec_name_col].str.upper().str.contains(pattern)]

    df["Symbol"] = df["Symbol"].str.upper()
    return df

# ============================================================
# TABLE 8 → IPO (Last 1 Year) Filter Function
# ============================================================
def fetch_ipo_symbols_last_1_year():

    url = "https://www.nseindia.com/api/public-past-issues"

    from_date = "01-01-2025"
    today = datetime.now().strftime("%d-%m-%Y")

    final_url = f"{url}?from_date={from_date}&to_date={today}&security_type=Equity"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers)

    resp = session.get(final_url, headers=headers)

    try:
        data = resp.json()
    except:
        return []

    if isinstance(data, dict) and "data" in data:
        rows = data["data"]
    elif isinstance(data, list):
        rows = data
    else:
        return []

    ipo_symbols = []
    for row in rows:
        sym = str(row.get("symbol", "")).strip().upper()
        if sym:
            ipo_symbols.append(sym)

    return list(set(ipo_symbols))


# ============================================================
# MAIN FUNCTION FOR TABLE 8
# ============================================================
def table8_last1year_ipo(merged):

    ipo_symbols = fetch_ipo_symbols_last_1_year()

    df = merged.copy()
    df = df[df["Symbol"].isin(ipo_symbols)]

    marketcap_col = None
    for col in df.columns:
        if "market_cap_basic" in col.lower():
            marketcap_col = col
            break

    if marketcap_col is None:
        return pd.DataFrame()

    df[marketcap_col] = pd.to_numeric(df[marketcap_col], errors="coerce") / 1_00_00_000
    df = df.rename(columns={marketcap_col: "MarketCap(Cr)"})

    df["VolumeValue(Cr)"] = (df["volume"] * df["LTP"]) / 1_00_00_000

    df = df[
        (df["MarketCap(Cr)"] > 500) &
        (df["VolumeValue(Cr)"] > 10)
    ]

    final = df[["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)", "MarketCap(Cr)"]].copy()

    return final.sort_values("ValueTrade(Cr)", ascending=False)



# ============================================================
# 🔄 MERGE NSE + TRADINGVIEW
# ============================================================
def merge_data():
    nse_df = fetch_price_band()
    tv_df = fetch_tradingview_data()

    merged = nse_df.merge(tv_df, on="Symbol", how="left")

    # Detect Price Band Column
    possible_cols = [
        "Price Band", "Price band", "Price Band %", "Price band %",
        "Band", "Band %", "PriceBand", "Band(%)"
    ]

    price_band_col = None
    for col in merged.columns:
        if col.strip().lower() in [p.lower() for p in possible_cols]:
            price_band_col = col
            break

    if price_band_col is None:
        raise Exception("❌ Price Band column not found!")

    # Clean Price Band column safely
    merged[price_band_col] = (
        merged[price_band_col]
        .astype(str)
        .str.replace('%', '')
        .str.replace('No Band', '0', case=False)
        .str.replace('NOBAND', '0', case=False)
        .str.replace('-', '0')
        .str.strip()
    )
    # Anything non-numeric becomes 0
    merged[price_band_col] = pd.to_numeric(merged[price_band_col], errors='coerce').fillna(0)

    # Create ValueTrade(Cr) column BEFORE splitting
    merged["ValueTrade(Cr)"] = merged["Value.Traded"] / 1_00_00_000
    # Split bands
    band_5 = merged[merged[price_band_col] == 5].copy()
    band_10 = merged[merged[price_band_col] == 10].copy()
    band_20 = merged[merged[price_band_col] == 20].copy()
    band_none = merged[
        (merged[price_band_col] == 0) |
        (merged[price_band_col].isna())
        ].copy()

    # Sort each by PcntChg (High → Low)
    band_5 = band_5.sort_values("PcntChg", ascending=False)
    band_10 = band_10.sort_values("PcntChg", ascending=False)
    band_20 = band_20.sort_values("PcntChg", ascending=False)
    band_none = band_none.sort_values("PcntChg", ascending=False)

    # Return EVERYTHING including the band column name
    return merged.copy(), band_5.copy(), band_10.copy(), band_20.copy(), band_none.copy(), price_band_col


# ============================================================
# STREAMLIT UI
# ============================================================
st.set_page_config(
    page_title="NSE Price Band Dashboard",
    layout="wide"
)

# ============================
# 🌈 UNIVERSAL GLOSSY TABLE STYLE (APPLY TO ALL TABLES)
# ============================
glossy_table_css = """
<style>

    /* ---- Universal Table Wrapper ---- */
    .stDataFrame, .stDataEditor {
        background: rgba(255,255,255,0.03) !important;
        border-radius: 15px !important;
        border: 1px solid rgba(255,255,255,0.12) !important;
        overflow: hidden !important;
        padding: 5px !important;
        box-shadow: 0 0 12px rgba(255,255,255,0.06);
    }

    /* ---- Table header ---- */
    .stDataFrame th, .stDataEditor th {
        background: rgba(255,255,255,0.10) !important;
        backdrop-filter: blur(8px) !important;
        color: #fff !important;
        font-weight: 600 !important;
        padding: 8px !important;
        border-bottom: 1px solid rgba(255,255,255,0.25) !important;
    }

    /* ---- Table cells ---- */
    .stDataFrame td, .stDataEditor td {
        color: #e5e5e5 !important;
        padding: 6px 8px !important;
        border-bottom: 1px solid rgba(255,255,255,0.05) !important;
    }

    /* ---- Row Hover Effect ---- */
    .stDataFrame tr:hover td, .stDataEditor tr:hover td {
        background: rgba(255,255,255,0.08) !important;
        transition: background 0.25s ease-in-out;
    }

    /* ---- Optional: Scrollbar Upgrade ---- */
    ::-webkit-scrollbar {
        height: 8px;
        width: 8px;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(255,255,255,0.25);
        border-radius: 10px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(255,255,255,0.40);
    }

</style>
"""

# Apply CSS
st.markdown(glossy_table_css, unsafe_allow_html=True)
# ============================
# 🌐 UNIVERSAL FONT OVERRIDE (CAMBRIA 18)
# ============================
cambria_css = """
<style>

    /* Apply Cambria to EVERYTHING */
    * {
        font-family: Cambria, serif !important;
        font-size: 18px !important;
    }

    /* Improve table readability */
    .stDataFrame, .stDataEditor {
        font-size: 18px !important;
    }

    .stDataFrame td, .stDataFrame th,
    .stDataEditor td, .stDataEditor th {
        font-size: 18px !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] * {
        font-family: Cambria, serif !important;
        font-size: 18px !important;
    }

    /* Headers + Subheaders */
    h1, h2, h3, h4, h5 {
        font-family: Cambria, serif !important;
        font-weight: 600 !important;
    }

</style>
"""

st.markdown(cambria_css, unsafe_allow_html=True)

# REMOVE TOP SPACE / PADDING
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

from streamlit_autorefresh import st_autorefresh

# Auto refresh every 30 seconds
st_autorefresh(interval=30_000, key="ltp_refresh")

with st.spinner("Fetching latest data..."):
    merged, band_5, band_10, band_20, band_none, price_band_col = merge_data()



# Sidebar
st.sidebar.title("⚙️ Filters")

search = st.sidebar.text_input("🔍 Search Symbol / Name")

columns = merged.columns.tolist()

selected_cols = st.sidebar.multiselect(
    "📋 Select Columns to Display",
    options=columns,
    default=columns
)

# Apply search
if search:
    search = search.upper()
    merged = merged[
        merged["Symbol"].str.contains(search, na=False) |
        merged.iloc[:, 1].str.upper().str.contains(search, na=False)
    ]
    band_5 = band_5[band_5["Symbol"].str.contains(search, na=False)]
    band_10 = band_10[band_10["Symbol"].str.contains(search, na=False)]
    band_20 = band_20[band_20["Symbol"].str.contains(search, na=False)]
    band_none = band_none[band_none["Symbol"].str.contains(search, na=False)]

# ============================================================
# SHOW FOUR TABLES SIDE BY SIDE
# ============================================================

# ---- Required final columns ----
final_cols = ["Symbol", "LTP", "PcntChg", "Value.Traded"]

# Rename Value.Traded → ValueTrade(Cr)
merged["ValueTrade(Cr)"] = merged["Value.Traded"] / 1_00_00_000  # convert to Cr



# ---- Required final columns ----
final_cols = ["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]

# Add Band column to each table
band_5 = band_5.assign(Band="5%").reset_index(drop=True)
band_10 = band_10.assign(Band="10%").reset_index(drop=True)
band_20 = band_20.assign(Band="20%").reset_index(drop=True)
band_none = band_none.assign(Band="No Band").reset_index(drop=True)
# ---- Round to 2 decimals ----
round_cols = ["LTP", "PcntChg", "ValueTrade(Cr)"]

for df in [band_5, band_10, band_20, band_none]:
    for col in round_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

band_none_clean = band_none.dropna(subset=["LTP", "PcntChg", "ValueTrade(Cr)"])
band_none_display = pd.concat([
    band_none_clean.head(5),
    band_none_clean.tail(5)
]).reset_index(drop=True)

# ============================================================
#  FULL ROW COLOR LOGIC FOR 5%,10%,20%,NO BAND TABLES
# ============================================================
def style_band(df, band):
    # Create full style map (row × col)
    styles = pd.DataFrame("", index=df.index, columns=df.columns)

    for idx, row in df.iterrows():
        val = row["PcntChg"]
        color = get_color_for_band(val, band)

        # Decide row background
        if color == "blue":
            bg = "background-color: #0046FF; color:white; font-weight:600;"
        elif color == "green":
            bg = "background-color: #00CC44; color:white; font-weight:600;"
        elif color == "yellow":
            bg = "background-color: #055F6A; color:white; font-weight:600;"
        elif color == "red":
            bg = "background-color: #FF3B30; color:white; font-weight:600;"
        elif color == "orange":
            bg = "background-color: #055F6A; color:white; font-weight:600;"
        else:
            bg = ""

        # Apply SAME COLOUR to all columns in the row
        if bg:
            for col in df.columns:
                styles.loc[idx, col] = bg

    # Build Styler
    styler = df.style.apply(lambda _: styles, axis=None)

    # Force 2-digit formatting
    format_rules = {
        "LTP": "{:.2f}",
        "PcntChg": "{:.2f}",
        "ValueTrade(Cr)": "{:.2f}"
    }

    return styler.format(format_rules)
# Round values to 2 decimals
row1 = st.columns(4)

with row1[0]:
    st.subheader("🟦 5% BAND")
    st.dataframe(style_band(band_5[final_cols], 5),
                 use_container_width=True, hide_index=True)

with row1[1]:
    st.subheader("🟩 10% BAND")
    st.dataframe(style_band(band_10[final_cols], 10),
                 use_container_width=True, hide_index=True)

with row1[2]:
    st.subheader("🟧 20% BAND")
    st.dataframe(style_band(band_20[final_cols], 20),
                 use_container_width=True, hide_index=True)

with row1[3]:
    st.subheader("⬜ NO BAND")
    st.dataframe(style_band(band_none_display[final_cols], 5),
                 use_container_width=True, hide_index=True)
for df in [band_5, band_10, band_20, band_none]:
    for col in round_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)


# with c1:
#     st.subheader("🟦 5% BAND")
#     st.dataframe(
#         style_band(band_5[final_cols], 5),
#         use_container_width=True,
#         hide_index=True
#     )


# with c2:
#     st.subheader("🟩 10% BAND")
#     st.dataframe(
#         style_band(band_10[final_cols], 10),
#         use_container_width=True,
#         hide_index=True
#     )

# with c3:
#     st.subheader("🟧 20% BAND")
#     st.dataframe(
#         style_band(band_20[final_cols], 20),
#         use_container_width=True,
#         hide_index=True
#     )


# with c4:
#     st.subheader("⬜ NO BAND")
#     st.dataframe(
#         style_band(band_none_display[final_cols], 5),
#         use_container_width=True,
#         hide_index=True
#     )

# ------------------------------------------------------------
# SECOND ROW (5,6,7,8)
# ------------------------------------------------------------
# col5, col6, col7, col8 = st.columns(4)
# ------------------------------------------------------------
# SECOND ROW (Tables 6 & 7)
# ------------------------------------------------------------
row2 = st.columns([1, 1])

import html
import streamlit.components.v1 as components

# ───────────────────────────────────────────────
# 📌 TABLE 6 — 52W High & ATH (LEFT SIDE)
# ───────────────────────────────────────────────
with row2[0]:
    st.subheader("📌 52W High & ATH")

    # Detect columns
    col_52 = None
    col_ath = None
    for c in merged.columns:
        if "52" in c.lower() and "week" in c.lower():
            col_52 = c
        if "high.all" in c.lower().replace(" ", "") or "ath" in c.lower():
            col_ath = c

    if col_52 is None:
        for c in merged.columns:
            if "price_52" in c.lower() or "52week" in c.lower():
                col_52 = c
                break

    # Remove NO-BAND stocks
    tmp_base = merged[~merged["Symbol"].isin(band_none["Symbol"])]

    # Build table
    cols_needed = ["Symbol", "LTP", "PcntChg", col_52]
    tmp = tmp_base[cols_needed].copy()
    tmp = tmp.rename(columns={col_52: "52W_High"})

    tmp["LTP"] = pd.to_numeric(tmp["LTP"], errors="coerce").round(2)
    tmp["52W_High"] = pd.to_numeric(tmp["52W_High"], errors="coerce").round(2)
    tmp["PcntChg"] = pd.to_numeric(tmp["PcntChg"], errors="coerce").round(2)

    tmp["Dist_52W(%)"] = (((tmp["LTP"] - tmp["52W_High"]) / tmp["52W_High"]) * 100).round(2)

    if col_ath:
        tmp[col_ath] = pd.to_numeric(merged[col_ath], errors="coerce").round(2)
        tmp = tmp.rename(columns={col_ath: "ATH"})
        tmp["Dist_ATH(%)"] = (((tmp["LTP"] - tmp["ATH"]) / tmp["ATH"]) * 100).round(2)
    else:
        tmp["Dist_ATH(%)"] = pd.NA

    tmp = tmp.sort_values("Dist_52W(%)", ascending=False).head(50).reset_index(drop=True)

    def fmt(x):
        if pd.isna(x):
            return "-"
        try:
            return f"{float(x):.2f}"
        except:
            return html.escape(str(x))

    # HTML table build
    rows_html = []
    for _, r in tmp.iterrows():
        row_style = ""

        try:
            if pd.notna(r.get("Dist_ATH(%)")) and abs(float(r["Dist_ATH(%)"])) < 1:
                row_style = "background-color:#0066FF; color:white; font-weight:600;"
            elif pd.notna(r.get("Dist_52W(%)")) and abs(float(r["Dist_52W(%)"])) < 1:
                row_style = "background-color:#00CC44; color:white; font-weight:600;"
        except:
            row_style = ""

        cells = [
            f"<td style='padding:8px; white-space:nowrap'>{html.escape(str(r['Symbol']))}</td>",
            f"<td style='padding:8px; text-align:right'>{fmt(r['LTP'])}</td>",
            f"<td style='padding:8px; text-align:right'>{fmt(r['PcntChg'])}</td>",
            f"<td style='padding:8px; text-align:right'>{fmt(r['52W_High'])}</td>",
            f"<td style='padding:8px; text-align:right'>{fmt(r['Dist_52W(%)'])}</td>"
        ]
        rows_html.append(f"<tr style='{row_style}'>" + "".join(cells) + "</tr>")

    header_html = """
    <thead>
      <tr>
        <th style='text-align:left; padding:10px'>Symbol</th>
        <th style='text-align:right; padding:10px'>LTP</th>
        <th style='text-align:right; padding:10px'>PcntChg</th>
        <th style='text-align:right; padding:10px'>52W_High</th>
        <th style='text-align:right; padding:10px'>Dist_52W(%)</th>
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
      }
      .glossy-table th {
        background: rgba(255,255,255,0.03);
        color: #ddd;
        font-weight:600;
      }
      .glossy-table td {
        color: #ddd;
        border-bottom: 1px solid rgba(255,255,255,0.02);
      }
      .glossy-table tbody tr:hover td {
        background: rgba(255,255,255,0.03);
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

# ───────────────────────────────────────────────
# 💰 TABLE 7 — ValueTrade > 100 Cr (COLORED)
# ───────────────────────────────────────────────
with row2[1]:
    st.subheader("💰 ValueTrade > 100 Cr")

    vt10 = band_10[band_10["ValueTrade(Cr)"] > 100][["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]].copy()
    vt10["Band"] = "10%"

    vt20 = band_20[band_20["ValueTrade(Cr)"] > 100][["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]].copy()
    vt20["Band"] = "20%"

    vt_all = pd.concat([vt10, vt20], ignore_index=True)

    if vt_all.empty:
        st.info("No stocks with ValueTrade > 100 Cr in 10% or 20% bands.")
    else:
        vt_all = vt_all.round(2)
        vt_all = vt_all.sort_values("ValueTrade(Cr)", ascending=False).reset_index(drop=True)

        # ------------------------------
        # Glossy table (same as 52W High)
        # ------------------------------
        import html

        rows_html = []
        for _, r in vt_all.iterrows():
            color_style = ""

            # 🔵 Blue for > 500 Cr
            if r["ValueTrade(Cr)"] > 500:
                color_style = "background-color:#0066FF; color:white; font-weight:600;"

            # 🟩 Green for > 100 Cr
            elif r["ValueTrade(Cr)"] > 100:
                color_style = "background-color:#00CC44; color:white; font-weight:600;"

            cells = [
                f"<td style='padding:8px; white-space:nowrap'>{html.escape(str(r['Symbol']))}</td>",
                f"<td style='padding:8px; text-align:right'>{r['LTP']:.2f}</td>",
                f"<td style='padding:8px; text-align:right'>{r['PcntChg']:.2f}</td>",
                f"<td style='padding:8px; text-align:right'>{r['ValueTrade(Cr)']:.2f}</td>",
                f"<td style='padding:8px; text-align:center'>{r['Band']}</td>"
            ]

            rows_html.append(f"<tr style='{color_style}'>" + "".join(cells) + "</tr>")

        header_html = """
        <thead>
          <tr>
            <th style='padding:10px; text-align:left'>Symbol</th>
            <th style='padding:10px; text-align:right'>LTP</th>
            <th style='padding:10px; text-align:right'>PcntChg</th>
            <th style='padding:10px; text-align:right'>ValueTrade(Cr)</th>
            <th style='padding:10px; text-align:center'>Band</th>
          </tr>
        </thead>
        """

        # glossy table style (same as 52W High)
        glossy_css = """
        <style>
          .value-table {
             width:100%;
             border-collapse:separate;
             border-spacing:0;
             background: rgba(255,255,255,0.02);
             border-radius: 12px;
             overflow: hidden;
             border: 1px solid rgba(255,255,255,0.06);
             font-family: Cambria, serif;
          }
          .value-table th {
             background: rgba(255,255,255,0.04);
             color:#ddd;
             font-weight:600;
          }
          .value-table td {
             color:#ddd;
             border-bottom:1px solid rgba(255,255,255,0.03);
          }
          .value-table tr:hover td {
             background: rgba(255,255,255,0.04);
          }
        </style>
        """

        html_table = f"""
        {glossy_css}
        <table class='value-table'>
            {header_html}
            <tbody>
                {''.join(rows_html)}
            </tbody>
        </table>
        """

        components.html(html_table, height=520, scrolling=True)

# ------------------------------------------------------------
# THIRD ROW (Tables 5 & 8)
# ------------------------------------------------------------
row3 = st.columns([1, 1])

# ───────────────────────────────────────────────
# 🔥 TABLE 5 — Breakout Candidates (LEFT SIDE)
# ───────────────────────────────────────────────
with row3[0]:
    st.subheader("🔥 Breakout Candidates")

    breakout_10 = band_10[band_10["PcntChg"] > 9.5][["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]].copy()
    breakout_10["Band"] = "10%"

    breakout_20 = band_20[band_20["PcntChg"] > 19.50][["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]].copy()
    breakout_20["Band"] = "20%"

    breakout_all = pd.concat([breakout_10, breakout_20], ignore_index=True)

    if breakout_all.empty:
        st.info("No breakout candidates at the moment.")
    else:
        breakout_all = breakout_all.round(2)
        st.data_editor(breakout_all, hide_index=True, width="stretch")

    # Summary box
    st.subheader("📊 Summary")
    st.markdown(f"""
    **Total Stocks:** {len(merged)}  
    **5% Band:** {len(band_5)}  
    **10% Band:** {len(band_10)}  
    **20% Band:** {len(band_20)}  
    **No Band:** {len(band_none)}  
    """)

# ───────────────────────────────────────────────
# 🟣 TABLE 8 — IPO Last 1 Year (RIGHT SIDE)
# ───────────────────────────────────────────────
with row3[1]:
    st.subheader("🟣 IPO (Last 1Y) – MarketCap > 500 Cr")

    table8 = table8_last1year_ipo(merged)

    if table8.empty:
        st.info("No IPO matches filters.")

    else:
        table8 = table8.round(2)

        # Build header
        header_html = """
        <thead>
            <tr>
                <th style='padding:10px;text-align:left;'>Symbol</th>
                <th style='padding:10px;text-align:right;'>LTP</th>
                <th style='padding:10px;text-align:right;'>PcntChg</th>
                <th style='padding:10px;text-align:right;'>MarketCap(Cr)</th>
                <th style='padding:10px;text-align:right;'>ValueTrade(Cr)</th>
            </tr>
        </thead>
        """

        import html
        rows_html = []

        for _, r in table8.iterrows():
            v = float(r["ValueTrade(Cr)"])

            # ---- Color Logic ----
            if 50 <= v < 100:
                row_style = "background-color:#055F6A; color:white; font-weight:600;"
            elif 100 <= v < 250:
                row_style = "background-color:#00CC44; color:white; font-weight:600;"
            elif v >= 250:
                row_style = "background-color:#0046FF; color:white; font-weight:600;"
            else:
                row_style = ""

            # Build row cells
            cells = [
                f"<td style='padding:8px;white-space:nowrap'>{html.escape(str(r['Symbol']))}</td>",
                f"<td style='padding:8px;text-align:right'>{r['LTP']:.2f}</td>",
                f"<td style='padding:8px;text-align:right'>{r['PcntChg']:.2f}</td>",
                f"<td style='padding:8px;text-align:right'>{r['MarketCap(Cr)']:.2f}</td>",
                f"<td style='padding:8px;text-align:right'>{r['ValueTrade(Cr)']:.2f}</td>"
            ]

            rows_html.append(
                f"<tr style='{row_style}'>" + "".join(cells) + "</tr>"
            )

        # ---- Glossy Style (same as your 52W table) ----
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
          }
          .glossy-table th {
            background: rgba(255,255,255,0.03);
            color: #ddd;
            font-weight:600;
          }
          .glossy-table td {
            color: #ddd;
            border-bottom: 1px solid rgba(255,255,255,0.02);
          }
          .glossy-table tbody tr:hover td {
            background: rgba(255,255,255,0.03);
          }
        </style>
        """

        # ---- Build Final HTML ----
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

































