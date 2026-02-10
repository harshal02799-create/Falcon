import streamlit as st
from nav import nav_menu
import pandas as pd
import os
import streamlit.components.v1 as components
import numpy as np
from tradingview_screener import Query, Column
import plotly.express as px

table_css = """
<style>
/* ===== TABLE BASE ===== */
.vol-table {
    width: 100%;
    border-collapse: collapse;
    font-family: Cambria, Georgia, serif;
    font-size: 14px;
    font-weight: 700;
    background: #000;
    color: #e5e7eb;
}

/* ===== HEADER ===== */
.vol-table thead th {
    position: sticky;
    top: 0;
    background: linear-gradient(180deg, #1f2933, #111827);
    color: #f9fafb;
    padding: 10px 12px;
    font-size: 16px;
    font-weight: 700;
    letter-spacing: 0.3px;
    border-bottom: 1px solid rgba(255,255,255,0.15);
    z-index: 2;
    text-align: right;
}

/* Left align first column */
.vol-table thead th:first-child,
.vol-table td:first-child {
    text-align: left;
}

/* ===== BODY CELLS ===== */
.vol-table td {
    padding: 8px 12px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
    text-align: right;
}

/* ===== ROW HOVER ===== */
.vol-table tbody tr {
    transition: background 0.15s ease;
}

.vol-table tbody tr:hover {
    background: #0f172a;
}

/* ===== TREND COLORS ===== */
.bullish {
    color: #22c55e;
    font-weight: 700;
}

.bearish {
    color: #ef4444;
    font-weight: 700;
}

.neutral {
    color: #facc15;
    font-weight: 700;
}

/* ===== SCROLLBAR ===== */
::-webkit-scrollbar {
    height: 6px;
    width: 6px;
}

::-webkit-scrollbar-thumb {
    background: #374151;
    border-radius: 6px;
}

::-webkit-scrollbar-track {
    background: #020617;
}
</style>
"""

# ===============================
# NAV
# ===============================
nav_menu()
def render_html_table(header_html, rows_html, min_rows=1):
    ROW_HEIGHT = 26
    HEADER_HEIGHT = 36
    MAX_HEIGHT = 450

    height = min(
        HEADER_HEIGHT + max(len(rows_html), min_rows) * ROW_HEIGHT,
        MAX_HEIGHT
    )

    components.html(
        f"""
        {table_css}
        <table class="vol-table">
            {header_html}
            <tbody>{''.join(rows_html)}</tbody>
        </table>
        """,
        height=height,
        scrolling=True
    )



# ===============================
# LOAD COMPANY MASTER (JSON)
# ===============================
@st.cache_data
def load_company_master():
    BASE_DIR = os.path.dirname(__file__)  # pages/
    json_path = os.path.join(BASE_DIR, "Company_master.json")

    if not os.path.exists(json_path):
        st.error(f"Company_master.json not found at: {json_path}")
        st.stop()

    master = pd.read_json(json_path)

    master = master.rename(
        columns={
            "nsesymbol": "Symbol",
            "sectorname": "Sector",
            "industryname": "Industry",
        }
    )

    master["Symbol"] = master["Symbol"].str.upper()

    return master[["Symbol", "Sector", "Industry"]]


# ===============================
# FETCH TRADINGVIEW DATA (LIVE)
# ===============================
@st.cache_data(ttl=300)
def fetch_tradingview_data():
    _, df = (
        Query()
        .select(
            "name",
            "exchange",
            "close",
            "change",
            "volume",
            "Value.Traded",
            "Value.Traded|1M",   # 🔥 weekly value
            "market_cap_basic",
        )
        .set_markets("india")
        .where(Column("exchange") == "NSE")
        .limit(9000)
        .get_scanner_data()
    )

    df = df.rename(
        columns={
            "name": "Symbol",
            "close": "LTP",
            "change": "PcntChg",
            "Value.Traded": "Today_Value",
            "Value.Traded|1M": "30_Day_Avg_Cr",
        }
    )

    df["Symbol"] = df["Symbol"].str.upper()
    df = df.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)

    return df


# ===============================
# LOAD & MERGE DATA
# ===============================
tv_df = fetch_tradingview_data()
master_df = load_company_master()

if tv_df.empty:
    st.error("No data received from TradingView")
    st.stop()

# df = tv_df.merge(master_df, on="Symbol", how="left")
df = master_df.merge(tv_df, on="Symbol", how="left")

df["Today_Value"] = df["Today_Value"].fillna(0)

# df["30_Day_Avg_Cr_Cr"] = df["30_Day_Avg_Cr_Cr"].fillna(0)

df["PcntChg"] = pd.to_numeric(df["PcntChg"], errors="coerce")

df["market_cap_basic"] = df["market_cap_basic"].fillna(0)

df["Sector"] = df["Sector"].fillna("Unknown")
df["Industry"] = df["Industry"].fillna("Unknown")

# ===============================
# NUMERIC SAFETY (FIXED)
# ===============================

for col in [
    "PcntChg",
    "Today_Value",
    "30_Day_Avg_Cr",
    "market_cap_basic",
]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# -------------------------------
# Convert to Crores AFTER fillna
# -------------------------------
df["30_Day_Avg_Cr_Cr"] = df["30_Day_Avg_Cr"] / 100000000
df["Today_Value"] = df["Today_Value"] / 1e7

# ===============================
# TABLE 1 — SECTOR MONEY ANALYSIS
# ===============================

col1, col2, col3 = st.columns([1.5, 1., .5], gap="small")

with col1:
    st.subheader("📊 Sector  Analysis ")

    df_all = df.copy()

    df_all["Sector"] = df_all["Sector"].fillna("Unknown")
    df_all["Today_Value"] = df_all["Today_Value"].fillna(0)
    df_all["30_Day_Avg_Cr_Cr"] = df_all["30_Day_Avg_Cr_Cr"].fillna(0)
    df_all["PcntChg"] = df_all["PcntChg"].fillna(0)
    sector_df = (
        df_all
        .groupby("Sector", dropna=False)
        .agg(
            Total_Stocks=("Symbol", "count"),
            Advancers=("PcntChg", lambda x: (x > 0).sum()),
            Decliners=("PcntChg", lambda x: (x < 0).sum()),
            Avg_Change=("PcntChg", "mean"),
            Today_Value=("Today_Value", "sum"),
            Avg_Value_1M_Cr=("30_Day_Avg_Cr_Cr", "mean")
            ,
        )
        .reset_index()
    )

    sector_df["Value_X"] = (
            ( sector_df["Today_Value"])
            /sector_df["Avg_Value_1M_Cr"].replace(0, np.nan)
    )

    sector_df["Value_X"] = sector_df["Value_X"].round(2).fillna(0)

    with col1:

        header_html = """
        <thead>
        <tr>
            <th>Sector</th>
            <th>Total</th>
            <th>Adv</th>
            <th>Dec</th>
            <th>Avg %</th>
            <th>Today (Cr)</th>
            <th>Value | X</th>
            <th>20 Day AVG (Cr)</th>
        </tr>
        </thead>
        """

        rows_html = []
        for _, r in sector_df.sort_values("Avg_Change", ascending=False).head(10).iterrows():
            rows_html.append(f"""
            <tr>
                <td style="text-align:left">{r['Sector']}</td>
                <td>{r['Total_Stocks']}</td>
                <td>{r['Advancers']}</td>
                <td>{r['Decliners']}</td>
                <td>{r['Avg_Change']:.2f}</td>
                <td>{r['Today_Value']:,.0f}</td>
                <td>{r['Value_X']:.2f}x</td>
                <td>{r['Avg_Value_1M_Cr']:,.0f}</td>
            </tr>
            """)

        render_html_table(header_html, rows_html, min_rows=10)

with col2:
    # ===============================
    # TABLE 2 — MARKET CAP ANALYSIS
    # ===============================
    st.subheader("📊 Market Cap Performance")

    df = df.dropna(subset=["market_cap_basic", "PcntChg"])

    df["MktCap(Cr)"] = (df["market_cap_basic"] / 1e7).round(0)
    df = df.sort_values("market_cap_basic", ascending=False).reset_index(drop=True)

    bins = [0, 100, 250, 500, len(df)]
    labels = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]

    df["CapBucket"] = pd.cut(
        df.index,
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    cap_summary = (
        df.groupby("CapBucket")
        .agg(
            Stocks=("Symbol", "count"),
            Avg_Return=("PcntChg", "mean"),
            Advancers=("PcntChg", lambda x: (x > 0).sum()),
            Decliners=("PcntChg", lambda x: (x < 0).sum()),
        )
        .reset_index()
    )

    def cap_trend(v):
        if v >= 0.5:
            return "🟢 Bullish"
        elif v <= -0.5:
            return "🔴 Bearish"
        return "🟡 Neutral"

    cap_summary["Avg_Return"] = cap_summary["Avg_Return"].round(2)
    cap_summary["Trend"] = cap_summary["Avg_Return"].apply(cap_trend)
    # -----------------------------
    # Build HTML header
    # -----------------------------
    header_html = """
    <thead>
    <tr>
        <th style="text-align:left">Cap Bucket</th>
        <th>Stocks</th>
        <th>Avg Return %</th>
        <th>Adv</th>
        <th>Dec</th>
        <th>Trend</th>
    </tr>
    </thead>
    """

    # -----------------------------
    # Build rows
    # -----------------------------
    rows_html = []
    for _, r in cap_summary.iterrows():
        trend_class = (
            "bullish" if r["Trend"].startswith("🟢")
            else "bearish" if r["Trend"].startswith("🔴")
            else "neutral"
        )

        rows_html.append(f"""
        <tr>
            <td style="text-align:left">{r['CapBucket']}</td>
            <td>{r['Stocks']}</td>
            <td>{r['Avg_Return']:.2f}</td>
            <td>{r['Advancers']}</td>
            <td>{r['Decliners']}</td>
            <td class="{trend_class}">{r['Trend']}</td>
        </tr>
        """)

    # -----------------------------
    # Render styled table
    # -----------------------------
    render_html_table(header_html, rows_html, min_rows=7)

    breadth_col = st.container()

with col3:

    # ------------------ LIQUID UNIVERSE (ValueTrade >= 10 Cr) ------------------
    liq_df = df[pd.to_numeric(df["Today_Value"], errors="coerce") >= 10]

    total = len(df)


    # ---------- USER INPUT FOR NET_R_% ----------
    user_pct = st.number_input(
        "NET_R_% [Value Trade ≥ 10 Cr]",
        min_value=0.25,
        max_value=5.0,
        value=1.0,
        step=0.25,
        key="net_r_threshold"
    )

    pos_3 = (liq_df["PcntChg"] > 3).sum()
    neg_3 = (liq_df["PcntChg"] < -3).sum()

    total_liq = len(liq_df)

    pos_pct = round(pos_3 / total_liq * 100, 1) if total_liq > 0 else 0
    neg_pct = round(neg_3 / total_liq * 100, 1) if total_liq > 0 else 0

    total_liq = len(liq_df)

    pos_pct = round(pos_3 / total_liq * 100, 1) if total_liq > 0 else 0
    neg_pct = round(neg_3 / total_liq * 100, 1) if total_liq > 0 else 0

    adv = (liq_df["PcntChg"] > 0).sum()
    dec = (liq_df["PcntChg"] < 0).sum()

    adv_pct = round(adv / (adv + dec) * 100, 1) if (adv + dec) > 0 else 0
    adv_pct = round(adv / (adv + dec) * 100, 1)


    # ---------- WEIGHTED NET BREADTH ----------
    pos_df = liq_df[liq_df["PcntChg"] > 3]
    neg_df = liq_df[liq_df["PcntChg"] < -3]

    pos_count = len(pos_df)
    neg_count = len(neg_df)

    avg_pos = pos_df["PcntChg"].mean() if pos_count > 0 else 0
    avg_neg = abs(neg_df["PcntChg"].mean()) if neg_count > 0 else 0

    total_active = pos_count + neg_count

    net_breadth = round(
        ((pos_count * avg_pos) - (neg_count * avg_neg)) / total_active,
        1
    ) if total_active > 0 else 0

    # ---------- NET_R_% (USER DEFINED WEIGHTED BREADTH) ----------
    pos_df_r = liq_df[liq_df["PcntChg"] > user_pct]
    neg_df_r = liq_df[liq_df["PcntChg"] < -user_pct]

    pos_count_r = len(pos_df_r)
    neg_count_r = len(neg_df_r)

    avg_pos_r = pos_df_r["PcntChg"].mean() if pos_count_r > 0 else 0
    avg_neg_r = abs(neg_df_r["PcntChg"].mean()) if neg_count_r > 0 else 0

    total_active_r = pos_count_r + neg_count_r

    NET_R_pct = round(
        ((pos_count_r * avg_pos_r) - (neg_count_r * avg_neg_r)) / total_active_r,
        2
    ) if total_active_r > 0 else 0





    # -------- COLOR LOGIC --------
    green = "#00cc44"
    red = "#ff4d4d"
    grey = "#9ca3af"

    pos_color = green if pos_pct >= 2 else grey
    neg_color = red if neg_pct >= 2 else grey
    ad_color = green if adv_pct >= 50 else red
    net_color = green if net_breadth > 0 else red if net_breadth < 0 else grey
    net_r_color = green if NET_R_pct > 0 else red if NET_R_pct < 0 else grey
    table_html = f"""
    <style>
        .breadth-table {{
            width: 100%;
            border-collapse: collapse;
            font-family: Cambria, Georgia, serif;
            font-size: 14px;
            font-weight: 700;
            color: #e5e7eb;
            background: #000;
        }}

        .breadth-table td {{
            padding: 8px 12px;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            white-space: nowrap;
        }}

        .breadth-label {{
            text-align: left;
            color: #f9fafb;
            font-weight: 700;
            font-size: 14px;
        }}

        .breadth-value {{
            text-align: center;
            font-weight: 700;
            font-size: 14px;
        }}
    </style>

    <table class="breadth-table">
        <tr>
            <td class="breadth-label">&gt; +3%</td>
            <td class="breadth-value" style="color:{pos_color};">
                {pos_3} / {pos_pct}%
            </td>
        </tr>
        <tr>
            <td class="breadth-label">&lt; −3%</td>
            <td class="breadth-value" style="color:{neg_color};">
                {neg_3} / {neg_pct}%
            </td>
        </tr>
        <tr>
            <td class="breadth-label">A / D ( Value > 10 cr)</td>
            <td class="breadth-value" style="color:{ad_color};">
                {adv} / {dec} ({adv_pct}%)
            </td>
        </tr>
        <tr>
            <td class="breadth-label">Net_Return</td>
            <td class="breadth-value" style="color:{net_color}; font-size:16px;">
                {net_breadth}%
            </td>
        </tr>
        <tr>
            <td class="breadth-label">NET_R_% (±{user_pct}%)</td>
            <td class="breadth-value" style="color:{net_r_color}; font-size:16px;">
                {NET_R_pct}%
            </td>
        </tr>
    </table>
    """

    st.markdown(table_html, unsafe_allow_html=True)

# ===============================
# TABLE 3 — SECTOR × MARKET CAP
# ===============================

# st.subheader("🏭 Sector-wise Market Cap Action")

sector_list = sorted(df["Sector"].dropna().unique())

query_params = st.query_params
selected_sector = query_params.get("sector")

# Streamlit returns list → fix it
if isinstance(selected_sector, list):
    selected_sector = selected_sector[0]

# fallback
if not selected_sector or selected_sector not in sector_list:
    selected_sector = sector_list[0]

selected_sector = st.selectbox(
    "",
    sector_list,
    index=sector_list.index(selected_sector),
    key="sector_select"
)


# hcol, scol = st.columns([.5, 1.5])
#
# with hcol:
#     st.subheader("🏭 Sector-wise Market Cap Action")
#
# with scol:
#     sector_list = sorted(df["Sector"].dropna().unique())
#
#     query_params = st.query_params
#     selected_sector = query_params.get("sector")
#
#     # Streamlit returns list → fix it
#     if isinstance(selected_sector, list):
#         selected_sector = selected_sector[0]
#
#     # fallback
#     if not selected_sector or selected_sector not in sector_list:
#         selected_sector = sector_list[0]
#
#     selected_sector = st.selectbox(
#         "",
#         sector_list,
#         index=sector_list.index(selected_sector),
#         key="sector_select_cap"   # ✅ UNIQUE KEY
#     )

ind_df = df[df["Sector"] == selected_sector].copy()

if ind_df.empty:
    st.warning("No data available for selected sector.")
    st.stop()

cap_ind_summary = (
    ind_df.groupby("CapBucket")
    .agg(
        Stocks=("Symbol", "count"),
        Avg_Return=("PcntChg", "mean"),
        Advancers=("PcntChg", lambda x: (x > 0).sum()),
        Decliners=("PcntChg", lambda x: (x < 0).sum()),
    )
    .reset_index()
)

cap_ind_summary["Avg_Return"] = cap_ind_summary["Avg_Return"].round(2)
cap_ind_summary["Trend"] = cap_ind_summary["Avg_Return"].apply(cap_trend)


cap_ind_summary["Avg_Return"] = (
    cap_ind_summary["Avg_Return"]
    .fillna(0)
    .round(2)
)


# -----------------------------
# HTML HEADER — Sector × Cap
# -----------------------------
header_html = """
<thead>
<tr>
    <th style="text-align:left">Cap Bucket</th>
    <th>Stocks</th>
    <th>Avg Return %</th>
    <th>Adv</th>
    <th>Dec</th>
    <th>Trend</th>
</tr>
</thead>
"""

# -----------------------------
# HTML ROWS
# -----------------------------
rows_html = []
for _, r in cap_ind_summary.iterrows():
    trend_class = (
        "bullish" if r["Trend"].startswith("🟢")
        else "bearish" if r["Trend"].startswith("🔴")
        else "neutral"
    )

    rows_html.append(f"""
    <tr>
        <td style="text-align:left">{r['CapBucket']}</td>
        <td>{r['Stocks']}</td>
        <td>{r['Avg_Return']:.2f}</td>
        <td>{r['Advancers']}</td>
        <td>{r['Decliners']}</td>
        <td class="{trend_class}">{r['Trend']}</td>
    </tr>
    """)

# -----------------------------
# TOTAL (ALL CAPS COMBINED)
# -----------------------------
total_stocks = int(cap_ind_summary["Stocks"].sum())
total_adv = int(cap_ind_summary["Advancers"].sum())
total_dec = int(cap_ind_summary["Decliners"].sum())

# Weighted Avg Return (correct)
total_avg_return = round(
    (cap_ind_summary["Avg_Return"] * cap_ind_summary["Stocks"]).sum()
    / total_stocks,
    2
) if total_stocks > 0 else 0

# Trend for TOTAL
if total_avg_return >= 0.5:
    total_trend = "🟢 Bullish"
elif total_avg_return <= -0.5:
    total_trend = "🔴 Bearish"
else:
    total_trend = "🟡 Neutral"

total_trend_class = (
    "bullish" if total_trend.startswith("🟢")
    else "bearish" if total_trend.startswith("🔴")
    else "neutral"
)

rows_html.append(f"""
<tr style="
    background:#020617;
    font-weight:900;
    border-top:3px solid rgba(255,255,255,0.35);
">
    <td style="text-align:left">TOTAL</td>
    <td>{total_stocks}</td>
    <td class="{total_trend_class}">{total_avg_return:.2f}</td>
    <td>{total_adv}</td>
    <td>{total_dec}</td>
    <td class="{total_trend_class}">{total_trend}</td>
</tr>
""")
# render_html_table(header_html, rows_html, min_rows=6)


# ===============================
# FIXED CAP ORDER & COLORS
# ===============================
CAP_ORDER = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]

ADV_COLORS = {
    "Large Cap": "#0000FF",
    "Mid Cap": "#FFFF00",
    "Small Cap": "#00FF00",
    "Micro Cap": "#808080",
}

DEC_COLORS = {
    "Large Cap": "#0000FF",
    "Mid Cap": "#FFFF00",
    "Small Cap": "#00FF00",
    "Micro Cap": "#808080",
}
total_adv = int(cap_ind_summary["Advancers"].sum())
total_dec = int(cap_ind_summary["Decliners"].sum())

if total_adv + total_dec == 0:
    st.warning("No Advance/Decline data available.")
    st.stop()

adv_data = (
    cap_ind_summary
    .set_index("CapBucket")["Advancers"]
    .reindex(CAP_ORDER)
)
dec_data = (
    cap_ind_summary
    .set_index("CapBucket")["Decliners"]
    .reindex(CAP_ORDER)
)

# Remove zeros & NaNs
adv_data = adv_data[adv_data > 0]
dec_data = dec_data[dec_data > 0]




pie_col1, table_col, pie_col2 = st.columns([1.2, 6, 1.2], gap="small")

with table_col:
    render_html_table(
        header_html,
        rows_html,
        min_rows=8   # shows 5–6 rows, rest scroll
    )

# =========================
# 🟢 ADVANCERS — MINI PIE
# =========================
with pie_col1:
    adv_df = adv_data.reset_index()
    adv_df.columns = ["CapBucket", "Count"]

    fig_adv = px.pie(
        adv_df,
        names="CapBucket",
        values="Count",
        hole=0.35,
        title="",
        color="CapBucket",
        color_discrete_map=ADV_COLORS
    )

    fig_adv.update_traces(
        hovertemplate="<b>%{label}</b><br>Stocks: %{value}<br>%{percent}",
        textinfo="percent",
        textfont_size=14
    )

    fig_adv.update_layout(
        showlegend=False,
        height=200,
        margin=dict(t=20, b=20, l=20, r=20),
        paper_bgcolor="#000000",
        font=dict(color="#e5e7eb"),

        # ✅ CENTER TEXT
        annotations=[
            dict(
                text="<b>ADV</b>",
                x=0.5,
                y=0.5,
                font=dict(size=14, color="#6698FF"),
                showarrow=False
            )
        ]
    )

    st.plotly_chart(fig_adv, use_container_width=True)


# =========================
# 🔴 DECLINERS — MINI PIE
# =========================
with pie_col2:
    dec_df = dec_data.reset_index()
    dec_df.columns = ["CapBucket", "Count"]

    fig_dec = px.pie(
        dec_df,
        names="CapBucket",
        values="Count",
        hole=0.35,
        title="",
        color="CapBucket",
        color_discrete_map=DEC_COLORS
    )

    fig_dec.update_traces(
        hovertemplate="<b>%{label}</b><br>Stocks: %{value}<br>%{percent}",
        textinfo="percent",
        textfont_size=14
    )

    fig_dec.update_layout(
        showlegend=False,
        height=200,
        margin=dict(t=20, b=20, l=20, r=20),
        paper_bgcolor="#000000",
        font=dict(color="#e5e7eb"),

        # ✅ CENTER TEXT
        annotations=[
            dict(
                text="<b>DEC</b>",
                x=0.5,
                y=0.5,
                font=dict(size=14, color="#ef4444"),
                showarrow=False
            )
        ]
    )

    st.plotly_chart(fig_dec, use_container_width=True)


legend_html = """
<div style="
    display:flex;
    justify-content:left;
    gap:14px;
    margin-top:-8px;
    font-family:Cambria;
    font-size:14px;
    color:#e5e7eb;
">
  <span><span style="color:#0000FF;">■</span> Large</span>
  <span><span style="color:#FFFF00;">■</span> Mid</span>
  <span><span style="color:#00FF00;">■</span> Small</span>
  <span><span style="color:#808080;">■</span> Micro</span>
</div>
"""
st.markdown(legend_html, unsafe_allow_html=True)

pie_legend_html = """
<div style="
    display:flex;
    justify-content:right;
    gap:16px;
    margin-top:-6px;
    font-family:Cambria;
    font-size:14px;
    color:#e5e7eb;
">
  <span><span style="color:#0000FF;">■</span> Large</span>
  <span><span style="color:#FFFF00;">■</span> Mid</span>
  <span><span style="color:#00FF00;">■</span> Small</span>
  <span><span style="color:#808080;">■</span> Micro</span>
</div>
"""
st.markdown(pie_legend_html, unsafe_allow_html=True)










# -----------------------------
# RENDER SUMMARY TABLE
# -----------------------------
# with table_col:
#     render_html_table(header_html, rows_html, min_rows=6)

ind_df["CapBucket"] = pd.Categorical(
    ind_df["CapBucket"],
    categories=CAP_ORDER,
    ordered=True
)
ind_df = ind_df.sort_values(
    ["CapBucket", "PcntChg"],
    ascending=[True, False]
)

# -----------------------------
# VALUE | X (STOCK LEVEL)
# -----------------------------
ind_df["Value_X"] = (
    ind_df["Today_Value"]
    / ind_df["30_Day_Avg_Cr_Cr"].replace(0, np.nan)
).round(2).fillna(0)

ind_df["Value_X"] = ind_df["Value_X"].round(2)


# -----------------------------
# HTML HEADER
# -----------------------------
header_html = """
<thead>
<tr>
    <th style="text-align:left">Cap</th>
    <th style="text-align:left">Symbol</th>
    <th>LTP</th>
    <th>%Chg</th>
    <th>Mkt Cap (Cr)</th>
    <th>Today (Cr)</th>
    <th>Value | X</th>
    <th>20 Day AVG (Cr)</th>
    <th style="text-align:left">Industry</th>
</tr>
</thead>
"""

# -----------------------------
# HTML ROWS
# -----------------------------
rows_html = []
for _, r in ind_df.iterrows():
    chg_class = (
        "bullish" if r["PcntChg"] > 0
        else "bearish" if r["PcntChg"] < 0
        else "neutral"
    )

    rows_html.append(f"""
    <tr>
        <td style="text-align:left">{r['CapBucket']}</td>
        <td style="text-align:left">{r['Symbol']}</td>
        <td>{r['LTP']:.2f}</td>
        <td class="{chg_class}">{r['PcntChg']:.2f}</td>
        <td>{r['MktCap(Cr)']:,.0f}</td>
        <td>{r['Today_Value']:,.0f}</td>
        <td>{r['Value_X']:.2f}x</td>
        <td>{r['30_Day_Avg_Cr_Cr']:,.0f}</td>
        <td style="text-align:left">{r['Industry']}</td>
    </tr>
    """)

# -----------------------------
# RENDER FINAL TABLE
# -----------------------------
render_html_table(header_html, rows_html, min_rows=15)


st.subheader("📈 Momentum & Trend Scanners")
# ===============================
# TECHNICAL SCANNER DATA
# ===============================
@st.cache_data(ttl=300)
def fetch_tv_scanner():
    _, sdf = (
        Query()
        .set_markets("india")
        .select(
            "name",
            "close",
            "change",
            "SMA10",
            "SMA20",
            "SMA50",
            "SMA100",
            "SMA200",
            "RSI",
            "Value.Traded",
            "market_cap_basic",
            "price_52_week_low",
            "price_52_week_high",
            "High.All"
        )
        .where(Column("exchange") == "NSE")
        .limit(9000)
        .get_scanner_data()
    )

    sdf = sdf.rename(columns={
        "name": "Symbol",
        "Value.Traded": "Today_Value",
        "market_cap_basic": "MarketCap",
        "change": "PcntChg"
    })

    sdf["Symbol"] = sdf["Symbol"].str.upper()

    numeric_cols = [
        "close", "SMA10", "SMA20", "SMA50",
        "SMA100", "SMA200", "RSI",
        "Today_Value", "MarketCap",
        "price_52_week_low", "price_52_week_high", "High.All",
        "PcntChg"
    ]

    sdf[numeric_cols] = sdf[numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    )

    sdf["Today_Value"] = sdf["Today_Value"].fillna(0) / 1e7
    sdf["MarketCap"] = sdf["MarketCap"].fillna(0) / 1e7
    sdf["PcntChg"] = sdf["PcntChg"].fillna(0)

    return sdf

# ===============================
# MERGE INDUSTRY (FROM JSON)
# ===============================
scanner_df = fetch_tv_scanner()

company_master = load_company_master()

scanner_df = scanner_df.merge(
    company_master[["Symbol", "Industry"]],
    on="Symbol",
    how="left"
)

scanner_df["Industry"] = scanner_df["Industry"].fillna("Unknown")


# COMMON FILTER
base_filter = (
    scanner_df[["close", "SMA10", "SMA20", "SMA50"]].notna().all(axis=1) &
    (scanner_df["close"] > scanner_df["SMA10"]) &
    (scanner_df["SMA10"] > scanner_df["SMA20"]) &
    (scanner_df["SMA20"] > scanner_df["SMA50"]) &
    (scanner_df["MarketCap"] > 500)
)

# ===============================
# CAP BUCKET ASSIGNMENT (PURE MCAP)
# ===============================
# scanner_df = scanner_df.sort_values("MarketCap", ascending=False).reset_index(drop=True)
#
# scanner_df["CapBucket"] = "Micro Cap"
# scanner_df.loc[:99, "CapBucket"] = "Large Cap"
# scanner_df.loc[100:249, "CapBucket"] = "Mid Cap"
# scanner_df.loc[250:499, "CapBucket"] = "Small Cap"
#
# CAP_ORDER = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]
# scanner_df["CapBucket"] = pd.Categorical(
#     scanner_df["CapBucket"],
#     categories=CAP_ORDER,
#     ordered=True
# )
# 🚀 52W LOW TABLE (EXCLUDE ATH = 52W HIGH)
table_a = scanner_df[
    base_filter &
    scanner_df["price_52_week_low"].notna() &
    (scanner_df["close"] >= scanner_df["price_52_week_low"] * 1.30)
].copy()

# 🔥 ATH TABLE
table_b = scanner_df[
    base_filter &
    scanner_df["High.All"].notna() &
    (scanner_df["close"] >= scanner_df["High.All"] * 0.80)
].copy()

def assign_cap_bucket(df):
    df = df.sort_values("MarketCap", ascending=False).reset_index(drop=True)

    df["CapBucket"] = "Micro Cap"
    df.loc[:99, "CapBucket"] = "Large Cap"
    df.loc[100:249, "CapBucket"] = "Mid Cap"
    df.loc[250:499, "CapBucket"] = "Small Cap"

    return df

table_a = assign_cap_bucket(table_a)
table_b = assign_cap_bucket(table_b)


min_value = st.number_input(
    "Minimum Today Value (Cr)",
    min_value=1.0,
    value=1.0,
    step=1.0
)

show_all = st.checkbox("Show all stocks", value=False)

if not show_all:
    table_a = table_a[table_a["Today_Value"] >= min_value]
    table_b = table_b[table_b["Today_Value"] >= min_value]


colA, spacer, colB = st.columns([1, 0.001, 1])

with colA:
    st.markdown(
        f"""
        <div style="font-family:Cambria; font-size:20px; font-weight:800; color:#f9fafb;">
            🚀 30% &gt; From 52W Low 
            <span style="color:#22c55e;">({len(table_a)})</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    header_html = """
    <thead>
    <tr>
        <th style="text-align:left">Symbol</th>
        <th>LTP</th>
        <th>PcntChg</th>
        <th>RSI</th>
        <th>52W High</th>
        <th>Today (Cr)</th>
        <th>MCap (Cr)</th>
        <th style="text-align:left">Industry</th>
    </tr>
    </thead>
    """

    rows_html = []
    for _, r in table_a.sort_values("Today_Value", ascending=False).iterrows():
        rows_html.append(f"""
        <tr>
            <td style="text-align:left">{r['Symbol']}</td>
            <td>{r['close']:.2f}</td>
            <td class="{'bullish' if r['PcntChg'] > 0 else 'bearish' if r['PcntChg'] < 0 else 'neutral'}">{r['PcntChg']:.2f}</td>
            <td class="bullish">{r['RSI']:.1f}</td>
            <td>{r['price_52_week_high']:.2f}</td>
            <td>{r['Today_Value']:,.1f}</td>
            <td>{r['MarketCap']:,.0f}</td>
            <td style="text-align:left">{r['Industry']}</td>
        </tr>
        """)

    render_html_table(header_html, rows_html, min_rows=16)

with colB:
    st.markdown(
        f"""
        <div style="font-family:Cambria; font-size:20px; font-weight:800; color:#f9fafb;">
            🔥 Near ATH 
            <span style="color:#facc15;">({len(table_b)})</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    header_html = """
    <thead>
    <tr>
        <th style="text-align:left">Symbol</th>
        <th>LTP</th>        
        <th>PcntChg</th>
        <th>RSI</th>
        <th>ATH</th>
        <th>Today (Cr)</th>
        <th>MCap (Cr)</th>
        <th style="text-align:left">Industry</th>
    </tr>
    </thead>
    """

    rows_html = []
    for _, r in table_a.sort_values("Today_Value", ascending=False).iterrows():
        rows_html.append(f"""
        <tr>
            <td style="text-align:left">{r['Symbol']}</td>
            <td>{r['close']:.2f}</td>
            <td class="{'bullish' if r['PcntChg'] > 0 else 'bearish' if r['PcntChg'] < 0 else 'neutral'}">
    {r['PcntChg']:.2f}
</td>
       
            <td class="bullish">{r['RSI']:.1f}</td>
            <td>{r['High.All']:.2f}</td>
            <td>{r['Today_Value']:,.1f}</td>
            <td>{r['MarketCap']:,.0f}</td>
            <td style="text-align:left">{r['Industry']}</td>
        </tr>
        """)

    render_html_table(header_html, rows_html, min_rows=16)



row5 = st.columns([1])

with row5[0]:

    if df.empty or "Sector" not in df.columns:
        st.info("Sector data not available.")
    else:
        tmp = df.copy()

        # -----------------------------
        # Numeric safety
        # -----------------------------
        for c in ["PcntChg", "Today_Value"]:
            tmp[c] = pd.to_numeric(tmp[c], errors="coerce").fillna(0)

        tmp["Sector"] = tmp["Sector"].fillna("Unknown")
        tmp["Industry"] = tmp["Industry"].fillna("Unknown")

        # -----------------------------
        # Sector + Industry aggregation
        # -----------------------------
        sec_ind_df = (
            tmp
            .groupby(["Sector", "Industry"], dropna=False)
            .agg(
                Total_Stocks=("Symbol", "count"),
                Advancers=("PcntChg", lambda x: (x > 0).sum()),
                Decliners=("PcntChg", lambda x: (x < 0).sum()),
                Avg_Change=("PcntChg", "mean"),
                Value_Cr=("Today_Value", "sum"),
            )
            .reset_index()
        )

        # -----------------------------
        # Derived metrics
        # -----------------------------
        sec_ind_df["Breadth"] = (
            sec_ind_df["Advancers"] - sec_ind_df["Decliners"]
        )

        def trend(row):
            if row["Avg_Change"] >= 1 and row["Breadth"] > 0:
                return "🟢 Bullish"
            elif row["Avg_Change"] <= -1 and row["Breadth"] < 0:
                return "🔴 Bearish"
            else:
                return "🟡 Neutral"

        sec_ind_df["Trend"] = sec_ind_df.apply(trend, axis=1)

        # -----------------------------
        # Formatting & sorting
        # -----------------------------
        sec_ind_df["Avg_Change"] = sec_ind_df["Avg_Change"].round(2)
        sec_ind_df["Value_Cr"] = sec_ind_df["Value_Cr"].round(2)

        sec_ind_df = sec_ind_df.sort_values(
            ["Avg_Change", "Value_Cr"],
            ascending=[False, False]
        )
        title_col, summary_col = st.columns([4, 3])
        with title_col:
            st.markdown(
                "<div style='font-family:Cambria; font-size:22px; font-weight:800; color:#f9fafb;'>"
                "📊 Sector & Industry Analysis"
                "</div>",
                unsafe_allow_html=True
            )

        # -----------------------------
        # TOTAL SUMMARY
        # -----------------------------
        total_stocks = sec_ind_df["Total_Stocks"].sum()
        total_adv = sec_ind_df["Advancers"].sum()
        total_dec = sec_ind_df["Decliners"].sum()
        with summary_col:
            st.markdown(f"""
            <div style="
                display:flex;
                gap:28px;
                padding:8px 14px;
                background:#020617;
                border:1px solid rgba(255,255,255,0.1);
                border-radius:6px;
                font-family:Cambria;
                font-size:15px;
                font-weight:700;
                color:#e5e7eb;
                justify-content:flex-end;
            ">
                <span>📦 <b>Total:</b> {total_stocks}</span>
                <span style="color:#22c55e;">▲ <b>Adv:</b> {total_adv}</span>
                <span style="color:#ef4444;">▼ <b>Dec:</b> {total_dec}</span>
            </div>
            """, unsafe_allow_html=True)

        # -----------------------------
        # HTML HEADER
        # -----------------------------
        header_html = """
        <thead>
        <tr>
            <th style="text-align:left">Sector</th>
            <th style="text-align:left">Industry</th>
            <th>Total</th>
            <th>Adv</th>
            <th>Dec</th>
            <th>Breadth</th>
            <th>Avg %</th>
            <th>Value (Cr)</th>
            <th>Trend</th>
        </tr>
        </thead>
        """

        # -----------------------------
        # HTML ROWS
        # -----------------------------
        rows_html = []

        for _, r in sec_ind_df.iterrows():
            # Trend color class
            trend_class = (
                "bullish" if r["Trend"].startswith("🟢")
                else "bearish" if r["Trend"].startswith("🔴")
                else "neutral"
            )

            rows_html.append(f"""
            <tr>
                <td style="text-align:left">{r['Sector']}</td>
                <td style="text-align:left">{r['Industry']}</td>
                <td>{r['Total_Stocks']}</td>
                <td>{r['Advancers']}</td>
                <td>{r['Decliners']}</td>
                <td>{r['Breadth']}</td>
                <td class="{trend_class}">{r['Avg_Change']:.2f}</td>
                <td>{r['Value_Cr']:,.0f}</td>
                <td class="{trend_class}">{r['Trend']}</td>
            </tr>
            """)

        # -----------------------------
        # RENDER HTML TABLE
        # -----------------------------
        render_html_table(header_html, rows_html, min_rows=15)


def render_cap_html_table(title, cap_df, summary_row):
    header_html = """
    <thead>
    <tr>
        <th style="text-align:left">Symbol</th>
        <th>LTP</th>
        <th>%Chg</th>
        <th>Mkt Cap (Cr)</th>
        <th>Today (Cr)</th>
        <th style="text-align:left">Industry</th>
    </tr>
    </thead>
    """

    rows_html = []

    for _, r in cap_df.iterrows():
        chg_class = (
            "bullish" if r["PcntChg"] > 0
            else "bearish" if r["PcntChg"] < 0
            else "neutral"
        )

        rows_html.append(f"""
        <tr>
            <td style="text-align:left">{r['Symbol']}</td>
            <td>{r['LTP']:.2f}</td>
            <td class="{chg_class}">{r['PcntChg']:.2f}</td>
            <td>{r['MktCap(Cr)']:,.0f}</td>
            <td>{r['Today_Value']:,.0f}</td>
            <td style="text-align:left">{r['Industry']}</td>
        </tr>
        """)

    st.markdown(f"""
    ### {title}
    **Stocks:** {summary_row['Stocks']} |
    **Avg Return:** <span style="font-weight:700;">{summary_row['Avg_Return']}%</span> |
    **A/D:** {summary_row['Advancers']} / {summary_row['Decliners']} |
    **Trend:** {summary_row['Trend']}
    """, unsafe_allow_html=True)

    render_html_table(header_html, rows_html, min_rows=10)

st.subheader("📊 Broader Market Cap table")

# -----------------------------
# Safety check
# -----------------------------
if df.empty or "market_cap_basic" not in df.columns:
    st.info("Market cap data not available.")
else:
    mdf = df.copy()

    # -----------------------------
    # Numeric safety
    # -----------------------------
    for c in ["market_cap_basic", "PcntChg", "Today_Value"]:
        mdf[c] = pd.to_numeric(mdf[c], errors="coerce").fillna(0)

    # -----------------------------
    # Market Cap in Crores
    # -----------------------------
    mdf["MktCap(Cr)"] = (mdf["market_cap_basic"] / 1e7).round(0)

    # -----------------------------
    # Sort by Market Cap (DESC)
    # -----------------------------
    mdf = mdf.sort_values("market_cap_basic", ascending=False).reset_index(drop=True)

    # -----------------------------
    # Assign Cap Buckets (PURE MCAP)
    # -----------------------------
    mdf["CapBucket"] = "Micro Cap"
    mdf.loc[:99, "CapBucket"] = "Large Cap"
    mdf.loc[100:249, "CapBucket"] = "Mid Cap"
    mdf.loc[250:499, "CapBucket"] = "Small Cap"

    CAP_ORDER = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]
    mdf["CapBucket"] = pd.Categorical(mdf["CapBucket"], categories=CAP_ORDER, ordered=True)

    # -----------------------------
    # Cap Summary
    # -----------------------------
    cap_summary = (
        mdf
        .groupby("CapBucket", observed=True)
        .agg(
            Stocks=("Symbol", "count"),
            Avg_Return=("PcntChg", "mean"),
            Advancers=("PcntChg", lambda x: (x > 0).sum()),
            Decliners=("PcntChg", lambda x: (x < 0).sum()),
        )
        .reset_index()
    )

    cap_summary["Avg_Return"] = cap_summary["Avg_Return"].round(2)

    def trend(v):
        if v >= 0.5:
            return "🟢 Bullish"
        elif v <= -0.5:
            return "🔴 Bearish"
        return "🟡 Neutral"

    cap_summary["Trend"] = cap_summary["Avg_Return"].apply(trend)
    # -----------------------------
    # DISPLAY — HTML (2 CAPS PER ROW)
    # -----------------------------
    cap_pairs = [("Large Cap", "Mid Cap"), ("Small Cap", "Micro Cap")]

    for left_cap, right_cap in cap_pairs:
        c1, c2 = st.columns(2, gap="medium")

        for col, cap in zip([c1, c2], [left_cap, right_cap]):
            with col:
                cap_df = (
                    mdf[mdf["CapBucket"] == cap]
                    .sort_values("PcntChg", ascending=False)
                )

                if cap_df.empty:
                    st.info(f"No {cap} stocks")
                    continue

                summary_row = cap_summary[
                    cap_summary["CapBucket"] == cap
                    ].iloc[0]

                render_cap_html_table(cap, cap_df, summary_row)















