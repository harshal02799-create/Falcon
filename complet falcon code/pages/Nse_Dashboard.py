
import streamlit as st
from nav import nav_menu
import os
import requests
from datetime import datetime
import streamlit as st
import requests
import pandas as pd
import re
import csv
import io
from datetime import datetime, timedelta
import os
from tradingview_screener import Query, Column







# =========================================================
# 🔵 TRADINGVIEW DATA (NSE ONLY)
# =========================================================
def fetch_tradingview_data():

    _, tv = (
        Query()
        .select(
            'name',
            'exchange',
            'close',
            'change',
            'volume',
            'market_cap_basic'
        )
        .set_markets('india')
        .where(Column('exchange') == 'NSE')
        .limit(9000)
        .get_scanner_data()
    )

    tv = tv.rename(columns={
        'name': 'Symbol',
        'close': 'LTP',
        'change': 'PcntChg'
    })

    tv["Symbol"] = tv["Symbol"].astype(str).str.upper()
    tv = tv.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)

    return tv


# =========================================================
# 0️⃣ READ sec_list.csv (OPTIONAL)
# =========================================================
def read_sec_list_csv(path):

    if not os.path.exists(path):
        print(f"⚠️ sec_list.csv NOT FOUND: {path}")
        return pd.DataFrame(columns=["SYMBOL", "SECURITY_NAME"])

    df = pd.read_csv(path)
    df.columns = [c.strip().upper() for c in df.columns]
    df = df.rename(columns={"SECURITY NAME": "SECURITY_NAME"})

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
    df["SECURITY_NAME"] = df["SECURITY_NAME"].astype(str).str.strip()

    return df


# =========================================================
# 1️⃣ INVESTORGAIN — LISTED IPO DATA
# =========================================================
def fetch_listed_only_ipo_df():

    urls = [
        "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2025/2025-26/0/ipo?search=&v=16-59",
        "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2026/2025-26/0/ipo?search=&v=17-19"
    ]

    headers = {"User-Agent": "Mozilla/5.0"}

    dfs = []
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=10)
            rows = r.json().get("reportTableData", [])
            if rows:
                dfs.append(pd.DataFrame(rows))
        except:
            continue

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    df = df.apply(lambda col: col.map(
        lambda x: re.sub(r"<.*?>", "", str(x)).strip()
    ))

    status = df["Status"].str.extract(r"L@([\d.]+).*?\((.*?)\)")
    df["Listing Price"] = status[0]
    df["Listing Gain (%)"] = status[1]

    df["IPO Price"] = df["IPO Price"].str.replace("₹", "").str.strip()
    df = df.dropna(subset=["Listing Price"])

    return df[["IPO", "Listing Price", "Listing Gain (%)"]].copy()


# =========================================================
# 2️⃣ NSE — COMPANY NAME ↔ SYMBOL
# =========================================================
def fetch_nse_company_symbol_df():

    today = datetime.now()
    from_date = (today - timedelta(days=365)).strftime("%d-%m-%Y")
    to_date = today.strftime("%d-%m-%Y")

    url = (
        "https://www.nseindia.com/api/public-past-issues"
        f"?from_date={from_date}&to_date={to_date}"
        f"&security_type=Equity&csv=true"
    )

    headers = {"User-Agent": "Mozilla/5.0"}
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers)

    r = session.get(url, headers=headers)
    csv_text = r.content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []

    for row in reader:
        if row.get("COMPANY NAME"):
            rows.append({
                "COMPANY NAME": row["COMPANY NAME"].strip(),
                "SYMBOL": row["Symbol"].strip().upper()
            })

    return pd.DataFrame(rows)


# =========================================================
# 3️⃣ SMART IPO → SYMBOL MAPPING
# =========================================================
def smart_bucket_merge(df_ipo, df_nse, df_sec):

    def fw(x): return x.split()[0].upper()

    out = []

    for _, r in df_ipo.iterrows():
        ipo = r["IPO"].upper()
        symbol = None

        for _, n in df_nse.iterrows():
            if fw(n["COMPANY NAME"]) == fw(ipo):
                symbol = n["SYMBOL"]
                break

        if symbol is None:
            for _, s in df_sec.iterrows():
                if fw(s["SECURITY_NAME"]) == fw(ipo):
                    symbol = s["SYMBOL"]
                    break

        if symbol is None:
            symbol = ipo.replace(" ", "")[:15]

        out.append({
            "Symbol": symbol,
            "Listing Price": float(r["Listing Price"]),
            "Listing Gain (%)": r["Listing Gain (%)"]
        })

    return pd.DataFrame(out)


# =========================================================
# 4️⃣ MERGE WITH TRADINGVIEW
# =========================================================
def merge_with_tradingview(ipo_df):

    tv = fetch_tradingview_data()

    df = ipo_df.merge(
        tv,
        on="Symbol",
        how="left"
    )

    return df


# =========================================================
# 5️⃣ CALCULATIONS + FINAL TABLE
# =========================================================
def build_table8(df):

    df["LTP"] = pd.to_numeric(df["LTP"], errors="coerce")
    df["Listing Price"] = pd.to_numeric(df["Listing Price"], errors="coerce")

    df["ValueTrade(Cr)"] = (
        pd.to_numeric(df["volume"], errors="coerce") * df["LTP"]
    ) / 1_00_00_000

    df["After Listing Gain (%)"] = (
        (df["LTP"] - df["Listing Price"]) / df["Listing Price"] * 100
    ).round(2).astype(str) + "%"

    return df[
        [
            "Symbol",
            "LTP",
            "PcntChg",
            "ValueTrade(Cr)",
            "Listing Gain (%)",
            "After Listing Gain (%)"
        ]
    ].sort_values("ValueTrade(Cr)", ascending=False)


# =========================================================
# 🚀 RUN EVERYTHING
# =========================================================


SEC_LIST_PATH = r"C:\Users\freedom\Desktop\complet final codes\falcon code\nse_files\PRICE_BAND_DATA\sec_list.csv"

df_ipo = fetch_listed_only_ipo_df()
df_nse = fetch_nse_company_symbol_df()
df_sec = read_sec_list_csv(SEC_LIST_PATH)

ipo_master = smart_bucket_merge(df_ipo, df_nse, df_sec)
ipo_tv = merge_with_tradingview(ipo_master)
ipo_table8 = build_table8(ipo_tv)














nav_menu()   # ← add menu here
st.markdown("<div style='margin-top:6px'></div>", unsafe_allow_html=True)

# ============================================================
# 📥 ONE-TIME PER DAY DOWNLOAD — NSE sec_list.csv
# ============================================================

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
        st.markdown(
            f"""
            <div style="
                background:#14532d;
                color:#86efac;
                padding:6px 14px;
                border-radius:8px;
                font-size:15px;
                font-weight:600;
                margin:4px 0;
            ">
                ✓ Already updated today ({today})
            </div>
            """,
            unsafe_allow_html=True
        )

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


from datetime import datetime, timedelta
import os

# ============================================================
# COLOR LOGIC FOR ALL BAND TABLES
# ============================================================
def get_color(v, band=None):
    try:
        v = float(v)
    except:
        return None

    # ================= NO BAND =================
    if band is None:
        if v >= 9.25:
            return "BLUE"
        if v >= 6:
            return "GREEN"
        if v <= -9.25:
            return "RED"
        if v <= -6:
            return "ORANGE"
        return None

    # ================= 5 % BAND =================
    if band == 5:
        if v >= 4.5:
            return "BLUE"
        if v >= 3:
            return "GREEN"
        if v <= -4.5:
            return "RED"
        if v <= -3:
            return "ORANGE"
        return None

    # ================= 10 % BAND =================
    if band == 10:
        if v >= 9.25:
            return "BLUE"
        if v >= 6:
            return "GREEN"
        if v <= -9.25:
            return "RED"
        if v <= -6:
            return "ORANGE"
        return None

    # ================= 20 % BAND =================
    if band == 20:
        if v >= 19.25:
            return "BLUE"
        if v >= 15:
            return "GREEN"
        if v <= -19.25:
            return "RED"
        if v <= -15:
            return "ORANGE"
        return None

    return None
def format_2(x):
    try:
        return f"{float(x):.2f}"
    except:
        return x



# ============================================================
# 🔵 FETCH TRADINGVIEW LTP DATA (NSE ONLY)
# ============================================================
from tradingview_screener import Query, Column

def fetch_tradingview_data():
    n_rows, tradingview = (
        Query()
        .select(
            'name',
            'exchange',
            'close',
            'change',
            'volume',
            'volume|15',  # 👈 ADD THIS
            'volume|30',
            'Value.Traded',
            'high',
            'average_volume_30d_calc',
            'price_52_week_high',
            'High.All',
            'market_cap_basic',
            'sector',
            'industry'
        )
        .set_markets('india')
        .where(Column('exchange') == 'NSE')   # ✅ NSE ONLY
        .limit(9000)
        .get_scanner_data()
    )

    tradingview = tradingview.rename(columns={
        'name': 'Symbol',
        'sector': 'Sector',
        'industry': 'Industry',
        'close': 'LTP',
        'change': 'PcntChg',
        'sector': 'Sector',
        'average_volume_30d_calc': 'AvgVol30',
        'volume|15': 'Vol15High'  # 👈 CLEAR NAME
    })

    tradingview["Symbol"] = tradingview["Symbol"].str.upper()
    tradingview = tradingview.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)

    return tradingview

import requests
import pandas as pd
import streamlit as st

@st.cache_data(ttl=30)  # 👈 CRITICAL: prevents repeated hits
def fetch_all_nse_indices():
    url = "https://www.nseindia.com/api/allIndices"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive"
    }

    session = requests.Session()
    session.headers.update(headers)

    # Step 1: Hit homepage to set cookies
    session.get("https://www.nseindia.com", timeout=10)

    # Step 2: Fetch all index data
    r = session.get(url, timeout=10)
    r.raise_for_status()

    raw = r.json().get("data", [])

    rows = []
    for d in raw:
        rows.append({
            "Index": d.get("index"),
            "LTP": d.get("last"),
            "Change": d.get("change"),
            "Change_%": d.get("percChange"),
            "Open": d.get("open"),
            "High": d.get("high"),
            "Low": d.get("low"),
            "Prev_Close": d.get("previousClose"),
            "52W_High": d.get("yearHigh"),
            "52W_Low": d.get("yearLow"),
        })

    df = pd.DataFrame(rows)

    # Clean + sort
    df = df.dropna(subset=["Index", "Change_%"])
    df["Change_%"] = pd.to_numeric(df["Change_%"], errors="coerce")
    df = df.sort_values("Change_%", ascending=False).reset_index(drop=True)

    return df

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
    """
    Returns IPO dataframe for last 1 year with:
    Symbol | Listing Gain (%) | After Listing Gain (%)
    """

    # 1️⃣ InvestorGain IPO data
    df_ipo = fetch_listed_only_ipo_df()
    if df_ipo.empty:
        return pd.DataFrame()

    # 2️⃣ NSE + sec_list mapping
    df_nse = fetch_nse_company_symbol_df()
    df_sec = read_sec_list_csv(SEC_LIST_PATH)

    # 3️⃣ Smart symbol mapping
    ipo_master = smart_bucket_merge(df_ipo, df_nse, df_sec)
    if ipo_master.empty:
        return pd.DataFrame()

    # 4️⃣ Merge with TradingView
    ipo_tv = merge_with_tradingview(ipo_master)

    # 5️⃣ Final IPO calculations (YOUR trusted logic)
    final_df = build_table8(ipo_tv)

    return final_df[
        ["Symbol", "Listing Gain (%)", "After Listing Gain (%)"]
    ].drop_duplicates("Symbol").reset_index(drop=True)



def fetch_listing_data():
    urls = [
        "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2025/2025-26/0/ipo?search=&v=16-59",
        "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2026/2025-26/0/ipo?search=&v=17-19"
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    all_data = []

    # Pre-define columns to avoid KeyError if the list is empty
    columns = ["IPO_Name", "Listing Price", "Listing Gain (%)"]

    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=10).json()
            rows = res.get('reportTableData', [])
            for r in rows:
                # Clean the name
                raw_name = r.get('IPO', '')
                name = re.sub(r'<.*?>', '', raw_name).strip().upper()

                # Try to find a Symbol inside the JSON if it exists (some APIs provide it)
                # If not, we will rely on name matching
                status = re.sub(r'<.*?>', '', r.get('Status', ''))
                match = re.search(r'L@([\d.]+).*?\((.*?)\)', status)

                if match:
                    all_data.append({
                        "IPO_Name": name,
                        "Listing Price": float(match.group(1)),
                        "Listing Gain (%)": match.group(2)
                    })
        except Exception as e:
            print(f"Warning: Could not fetch from {url}. Error: {e}")
            continue

    return pd.DataFrame(all_data, columns=columns)  # Forces columns to exist

# ============================================================
# MAIN FUNCTION FOR TABLE 8
# ============================================================
def table8_last1year_ipo(merged):

    ipo_df = fetch_ipo_symbols_last_1_year()
    if ipo_df.empty:
        return pd.DataFrame()

    df = merged.merge(
        ipo_df,
        on="Symbol",
        how="inner"
    )

    # ---- Market Cap filter ----
    df["MarketCap(Cr)"] = df["market_cap_basic"] / 1e7
    df["ValueTrade(Cr)"] = (df["volume"] * df["LTP"]) / 1e7

    df = df[
        (df["MarketCap(Cr)"] > 500) &
        (df["ValueTrade(Cr)"] > 10)
    ]

    return df[
        [
            "Symbol",
            "LTP",
            "PcntChg",
            "Listing Gain (%)",
            "After Listing Gain (%)",
            "ValueTrade(Cr)"
        ]
    ].sort_values("ValueTrade(Cr)", ascending=False)


# ============================================================
# 🔊 TABLE 9 — VOLUME BREAKER CALCULATION
# ============================================================
def build_volume_breaker(merged, price_band_col):

    df = merged.copy()

    if "Sector" not in df.columns:
        df["Sector"] = None

    # -----------------------------
    # Keep ONLY 5 / 10 / 20 Band
    # -----------------------------
    df = df[df[price_band_col].isin([5, 10, 20])].copy()

    # -----------------------------
    # Ensure numeric safety
    # -----------------------------
    num_cols = [
        "volume",
        "AvgVol30",
        "Vol15High",
        "LTP",
        "Value.Traded",
        "PcntChg",
        "price_52_week_high"
    ]

    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # -----------------------------
    # Drop invalid AvgVol30 rows
    # -----------------------------
    df = df[df["AvgVol30"] > 0]

    # -----------------------------
    # Value in Crore
    # -----------------------------
    df["Value(Cr)"] = df["Value.Traded"] / 1_00_00_000

    # -----------------------------
    # Volume Multiple
    # -----------------------------
    df["VolX"] = (df["volume"] / df["AvgVol30"]).round(2)

    # -----------------------------
    # VOL | 10
    # -----------------------------
    df["VOL_10"] = (
        df["Vol15High"].notna() &
        (df["volume"] >= df["Vol15High"])
    )

    # -----------------------------
    # Band Label
    # -----------------------------
    df["Band"] = df[price_band_col].astype(int).astype(str) + "%"

    # -----------------------------
    # 52W High
    # -----------------------------
    df["52W High"] = df.get("price_52_week_high")

    # -----------------------------
    # MESSAGE LOGIC
    # -----------------------------
    def get_message(r):
        if r.get("VOL_10"):
            return "🔥 INSTITUTIONAL VOL | 10"
        elif (
            pd.notna(r["52W High"]) and
            abs((r["LTP"] - r["52W High"]) / r["52W High"]) <= 0.01
        ):
            return "BREAKING AVG VOL WITH 52 WEEK HIGH"
        elif r["PcntChg"] < 0:
            return "BREAKING AVG VOL WITH SELLING PRESSURE"
        elif r["PcntChg"] >= 5:
            return "BREAKING AVG VOL WITH STRONG PRICE MOVE"
        else:
            return "BREAKING AVG VOL"

    df["MESSAGE"] = df.apply(get_message, axis=1)

    # -----------------------------
    # HARD FILTERS
    # -----------------------------
    df = df[
        (df["VolX"] >= 2) &
        (df["Value(Cr)"] >= 100)
    ]

    # -----------------------------
    # FINAL OUTPUT
    # -----------------------------
    final_cols = [
        "Band",
        "Sector",
        "Industry",
        "Symbol",
        "LTP",
        "PcntChg",
        "volume",
        "AvgVol30",
        "VolX",
        "VOL_10",
        "Value(Cr)",
        "52W High",
        "MESSAGE"
    ]

    return (
        df[final_cols]
        .sort_values(
            by=["VolX", "Value(Cr)", "PcntChg"],
            ascending=[False, False, False]
        )
        .reset_index(drop=True)
    )

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
# st.set_page_config(
#     page_title="NSE Price Band Dashboard",
#     layout="wide"
# )


st.markdown("""
<style>

/* ===============================
   GLOBAL LAYOUT COMPRESSION
   =============================== */
<style>
.block-container {
    padding-top: 0.8rem !important;   /* ⬅ increased slightly */
    padding-bottom: 0.6rem !important;
    padding-left: 1.1rem !important;
    padding-right: 1.1rem !important;
    max-width: 99% !important;
}

/* Reduce gap between sections (safe) */
div[data-testid="stVerticalBlock"] > div {
    margin-bottom: 0.0rem !important;
}

/* Headers tighter */
h1, h2, h3, h4, h5 {
    margin-top: 0.4rem !important;
    margin-bottom: 0.25rem !important;
}

/* Subheaders extra-tight */
div[data-testid="stSubheader"] {
    margin-top: 0.4rem !important;
    margin-bottom: 0.2rem !important;
}

/* Buttons tighter */
/* Only normal buttons, not expanders */
button:not([data-testid="stExpander"]) {
    padding: 0.32rem 0.7rem !important;
}

/* Success / info / warning boxes */
div[data-testid="stAlert"] {
    padding: 0.55rem 0.9rem !important;
    margin-top: 0.3rem !important;
    margin-bottom: 0.4rem !important;
}

/* HR lines tighter */
hr {
    margin: 0.5rem 0 !important;
}

</style>
""", unsafe_allow_html=True)

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
st.markdown("""
<style>
.summary-box, .breadth-box, .sector-box {
    white-space: nowrap;
}
</style>
""", unsafe_allow_html=True)

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

# # REMOVE TOP SPACE / PADDING
# st.markdown("""
#     <style>
#         .block-container {
#             padding-top: 1rem;
#         }
#     </style>
# """, unsafe_allow_html=True)

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

band_none_clean = band_none.dropna(
    subset=["LTP", "PcntChg", "ValueTrade(Cr)"]
)

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
        color = get_color(val, band)

        # Decide row background
        if color == "BLUE":
            bg = "background-color: #0046FF; color:white; font-weight:600;"
        elif color == "GREEN":
            bg = "background-color: #055F6A; color:white; font-weight:600;"
        elif color == "YELLOW":
            bg = "background-color: #F0ED51; color:white; font-weight:600;"
        elif color == "RED":
            bg = "background-color: #FF3B30; color:white; font-weight:600;"
        elif color == "ORANGE":
            bg = "background-color: #363434; color:white; font-weight:600;"
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
# with st.expander("🔽 Expand / Collapse Price Band Tables"):

row1 = st.columns(4)

HEADER_STYLE = """
<h4 style="
    margin:4px 0 6px 0;
    font-weight:600;
    font-size:16px;
">
"""

with row1[0]:
    st.markdown(
        HEADER_STYLE + f"🟦 5% BAND <span style='color:#9ca3af'>(Total: {len(band_5)})</span></h4>",
        unsafe_allow_html=True
    )
    st.dataframe(style_band(band_5[final_cols], 5),
                 use_container_width=True, hide_index=True)

with row1[1]:
    st.markdown(
        HEADER_STYLE + f"🟩 10% BAND <span style='color:#9ca3af'>(Total: {len(band_10)})</span></h4>",
        unsafe_allow_html=True
    )
    st.dataframe(style_band(band_10[final_cols], 10),
                 use_container_width=True, hide_index=True)

with row1[2]:
    st.markdown(
        HEADER_STYLE + f"🟧 20% BAND <span style='color:#9ca3af'>(Total: {len(band_20)})</span></h4>",
        unsafe_allow_html=True
    )
    st.dataframe(style_band(band_20[final_cols], 20),
                 use_container_width=True, hide_index=True)

with row1[3]:
    st.markdown(
        HEADER_STYLE + f"⬜ NO BAND <span style='color:#9ca3af'>(Total: {len(band_none)})</span></h4>",
        unsafe_allow_html=True
    )
    st.dataframe(style_band(band_none_display[final_cols], 5),
                 use_container_width=True, hide_index=True)

for df in [band_5, band_10, band_20, band_none]:
    for col in round_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

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
    # ⬇️ Download 52W High & ATH CSV
    st.download_button(
        label="⬇️ 52W High & ATH CSV",
        data=tmp.to_csv(index=False),
        file_name="52W_High_ATH_Stocks.csv",
        mime="text/csv",
        use_container_width=True
    )
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
                row_style = "background-color:#055F6A; color:white; font-weight:600;"
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
    vt10 = band_10[band_10["ValueTrade(Cr)"] > 100][["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)",  "Industry"]].copy()
    vt10["Band"] = "10%"

    vt20 = band_20[band_20["ValueTrade(Cr)"] > 100][["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)",  "Industry"]].copy()
    vt20["Band"] = "20%"

    vt_all = pd.concat([vt10, vt20], ignore_index=True)
    total_vt_stocks = len(vt_all)

    st.subheader(f"💰 ValueTrade > 100 Cr  |  Stocks: {total_vt_stocks}")

    if vt_all.empty:
        st.info("No stocks with ValueTrade > 100 Cr in 10% or 20% bands.")
    else:
        vt_all = vt_all.round(2)
        vt_all = vt_all.sort_values("ValueTrade(Cr)", ascending=False).reset_index(drop=True)

        st.download_button(
            label="⬇️ ValueTrade > 100 Cr CSV",
            data=vt_all.to_csv(index=False),
            file_name="ValueTrade_Above_100Cr.csv",
            mime="text/csv",
            use_container_width=True
        )
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
                color_style = "background-color:#055F6A; color:white; font-weight:600;"

            cells = [
                f"<td style='padding:8px; white-space:nowrap'>{html.escape(str(r['Symbol']))}</td>",
                f"<td style='padding:8px; text-align:right'>{r['LTP']:.2f}</td>",
                f"<td style='padding:8px; text-align:right'>{r['PcntChg']:.2f}</td>",
                f"<td style='padding:8px; text-align:right'>{r['ValueTrade(Cr)']:.2f}</td>",
                f"<td style='padding:8px; text-align:center'>{r['Band']}</td>",
                # f"<td style='padding:8px; text-align:center'>{r['Sector']}</td>",
                f"<td style='padding:8px; text-align:center'>{r['Industry']}</td>"
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

            <th style='padding:10px; text-align:center'>Industry</th>
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
# ROW 3 — THREE TABLES
# ------------------------------------------------------------
row3 = st.columns([1.3, 0.7, 1.3], gap="medium")

# ───────────────────────────────────────────────
# 🔥 TABLE 5 —Circuit Stocks (LEFT SIDE)
# ───────────────────────────────────────────────
with row3[0]:
    st.subheader("🔥 Circuit Stocks")

    breakout_10 = band_10[band_10["PcntChg"] > 9.5][
        ["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]
    ].copy()
    breakout_10["Band"] = "10%"

    breakout_20 = band_20[band_20["PcntChg"] > 19.50][
        ["Symbol", "LTP", "PcntChg", "ValueTrade(Cr)"]
    ].copy()
    breakout_20["Band"] = "20%"

    breakout_all = pd.concat([breakout_10, breakout_20], ignore_index=True)

    if breakout_all.empty:
        st.info("No Circuit Stocks at the moment.")
    else:
        breakout_all = breakout_all.round(2)

        st.data_editor(
            breakout_all,
            hide_index=True,
            use_container_width=True,
            height=800   # 🔥 CONTROL HEIGHT HERE (px)
        )

    breadth_col, sector_col = st.columns([1, 1], gap="medium")


    # ---------------- 📈 SECTOR PERFORMANCE ----------------
import streamlit.components.v1 as components

with row3[1]:
    st.markdown("""
    <h4 style="
        text-align:center;
        margin:4px 0 6px 0;
        font-size:16px;
        font-weight:600;
    ">
    📈 Sector Perform
    </h4>
    """, unsafe_allow_html=True)

    if "Sector" in merged.columns:

        sector_perf = (
            merged.dropna(subset=["Sector", "PcntChg"])
            .groupby("Sector")["PcntChg"]
            .mean()
            .round(2)
            .sort_values(ascending=False)
        )
        # ---- FILTER LOGIC (IMPORTANT) ----
        gainers = (
            sector_perf[sector_perf > 0]
            .head(5)
            .items()
        )

        losers = (
            sector_perf[sector_perf < 0]
            .tail(5)
            .items()
        )
        gainers = list(gainers)
        losers = list(losers)

        max_rows = max(len(gainers), len(losers))

        rows = ""

        for i in range(max_rows):
            g = gainers[i] if i < len(gainers) else ("—", "")
            l = losers[i] if i < len(losers) else ("—", "")

            rows += f"""
            <tr>
                <td>{i + 1}. {g[0]}</td>
                <td style="color:#00cc44; text-align:right;">{g[1]}%</td>
                <td>{i + 1}. {l[0]}</td>
                <td style="color:#ff4d4d; text-align:right;">{l[1]}%</td>
            </tr>
            """

        html = f"""
        <style>
            table {{
                width:100%;
                border-collapse:collapse;
                font-size:15px;
                color:#e5e7eb; /* light text */
            }}
            th {{
                text-align:left;
                padding:6px 8px;
                border-bottom:1px solid rgba(255,255,255,0.25);
                color:#f8fafc;
                font-weight:600;
            }}
            td {{
                padding:6px 8px;
                white-space:nowrap;
                color:#e5e7eb;
            }}
        </style>

        <table>
            <thead>
                <tr>
                    <th>🟢 Gainers</th>
                    <th style="text-align:right;">%</th>
                    <th>🔴 Losers</th>
                    <th style="text-align:right;">%</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
        """

        components.html(html, height=160)

    else:
        st.markdown("_Sector data not available_")


    import requests
    import pandas as pd
    import streamlit as st

    # =========================================================
    # NSE INDEX DEFINITIONS
    # =========================================================

    SECTORAL_INDICES = {
        "NIFTY AUTO", "NIFTY BANK", "NIFTY FMCG", "NIFTY IT",
        "NIFTY MEDIA", "NIFTY METAL", "NIFTY PHARMA",
        "NIFTY PSU BANK", "NIFTY REALTY",
        "NIFTY FINANCIAL SERVICES", "NIFTY PRIVATE BANK",
        "NIFTY HEALTHCARE INDEX", "NIFTY OIL & GAS",
        "NIFTY CONSUMER DURABLES", "NIFTY MIDSMALL FINANCIAL SERVICES",
        "NIFTY MICROCAP 250"
    }

    BROADER_MARKET_INDICES = {
        "NIFTY 50", "NIFTY NEXT 50", "NIFTY 100", "NIFTY 200",
        "NIFTY 500", "NIFTY MIDCAP 50", "NIFTY MIDCAP 100",
        "NIFTY MIDCAP 150", "NIFTY SMALLCAP 50",
        "NIFTY SMALLCAP 100", "NIFTY SMALLCAP 250",
        "NIFTY MIDSMALL 400"
    }

    DERIVATIVE_INDICES = {
        "NIFTY 50", "NIFTY BANK",
        "NIFTY FINANCIAL SERVICES", "NIFTY MIDCAP SELECT"
    }

    MAIN_INDICES_ORDER = [
        "NIFTY 50",
        "NIFTY MIDCAP 100",
        "NIFTY SMALLCAP 250",
        "NIFTY MICROCAP 250"
    ]


    # =========================================================
    # FETCH NSE DATA (ONCE)
    # =========================================================
    @st.cache_data(ttl=30)
    def fetch_all_nse_indices():
        url = "https://www.nseindia.com/api/allIndices"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/",
        }

        session = requests.Session()
        session.headers.update(headers)
        session.get("https://www.nseindia.com", timeout=10)

        r = session.get(url, timeout=10)
        r.raise_for_status()

        rows = []
        for d in r.json().get("data", []):
            if not isinstance(d.get("last"), (int, float)):
                continue

            rows.append({
                "Index": d.get("index"),
                "LTP": d.get("last"),
                "Prev_Close": d.get("previousClose"),
                "Change_%": d.get("percentChange"),
            })
        df = pd.DataFrame(rows)

        df["Change_%"] = pd.to_numeric(df["Change_%"], errors="coerce")
        df["LTP"] = pd.to_numeric(df["LTP"], errors="coerce")
        df["Prev_Close"] = pd.to_numeric(df["Prev_Close"], errors="coerce")

        # ✅ THIS LINE CREATES THE COLUMN YOU NEED
        df["LTP_minus_Close"] = df["LTP"] - df["Prev_Close"]

        return df.dropna()


    # =========================================================
    # BUILD FINAL INDEX DATAFRAME
    # =========================================================
    def build_final_index_df(df):

        allowed = (
                SECTORAL_INDICES |
                BROADER_MARKET_INDICES |
                DERIVATIVE_INDICES
        )

        df = df[df["Index"].isin(allowed)].copy()

        main_df = (
            df[df["Index"].isin(MAIN_INDICES_ORDER)]
            .set_index("Index")
            .reindex(MAIN_INDICES_ORDER)
            .reset_index()
        )

        other_df = (
            df[~df["Index"].isin(MAIN_INDICES_ORDER)]
            .sort_values("Change_%", ascending=False)
        )

        return pd.concat([main_df, other_df], ignore_index=True)


    # =========================================================
    # STREAMLIT UI
    # =========================================================
    # st.set_page_config(layout="wide")

    # ---- FETCH DATA ----
    raw_index_df = fetch_all_nse_indices()
    final_df = build_final_index_df(raw_index_df)
    # =========================================================
    # 📊 INDEX VIEW UI
    # =========================================================
    st.markdown("<h4 style='text-align:center;'>📊 Index View</h4>", unsafe_allow_html=True)

    if final_df.empty:
        st.info("Index data not available")
        st.stop()

    # ---- MAIN INDICES ----
    main_df = (
        final_df[final_df["Index"].isin(MAIN_INDICES_ORDER)]
        .set_index("Index")
        .reindex(MAIN_INDICES_ORDER)
        .reset_index()
    )

    other_df = final_df[~final_df["Index"].isin(MAIN_INDICES_ORDER)]
    table_html = """<style>
    .index-table {
        width:100%;
        border-collapse:collapse;
        font-size:15px;
        color:#e5e7eb;
    }
    .index-table tr {
        border-bottom:1px solid rgba(255,255,255,0.08);
    }
    .index-table td {
        padding:10px 12px;
        white-space:nowrap;
    }
    .idx-name {
        text-align:left;
        font-weight:600;
    }
    .idx-val {
        text-align:right;
        font-weight:600;
    }
    .idx-pts {
        text-align:right;
        font-weight:500;
    }
    </style>

    <table class="index-table">
    """
    for _, r in main_df.iterrows():
        pct = r["Change_%"]
        pts = r["LTP_minus_Close"]

        pct_color = "#22c55e" if pct > 0 else "#ef4444" if pct < 0 else "#9ca3af"
        pts_color = "#22c55e" if pts > 0 else "#ef4444" if pts < 0 else "#9ca3af"

        arrow = "▲" if pct > 0 else "▼" if pct < 0 else "•"

        table_html += f"""
        <tr>
            <td class="idx-name">{r['Index']}</td>
            <td class="idx-val">{r['LTP']:.2f}</td>
            <td class="idx-val" style="color:{pct_color};">
                {arrow} {pct:.2f}%
            </td>
            <td class="idx-pts" style="color:{pts_color};">
                {pts:+.2f}
            </td>
        </tr>
        """

    table_html += "</table>"
    components.html(table_html, height=240, scrolling=False)

    st.markdown("<div style='height:1px'></div>", unsafe_allow_html=True)

    # ---------------- 📈 MARKET BREADTH (SINGLE COLUMN TABLE) ----------------
    with row3[1]:
        # ------------------ LIQUID UNIVERSE (ValueTrade >= 10 Cr) ------------------
        liq_df = merged[pd.to_numeric(merged["ValueTrade(Cr)"], errors="coerce") >= 10].copy()

        # st.markdown(
        #     "<h4 style='text-align:center;'>📈 Market Breadth</h4>",
        #     unsafe_allow_html=True
        # )
        total = len(merged)

        # ---------- USER INPUT FOR NET_R_% ----------
        user_pct = st.number_input(
            "NET_R_% [ValueTrade ≥ 10 Cr]",
            min_value=0.5,
            max_value=20.0,
            value=3.0,
            step=0.5,
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
                width:100%;
                border-collapse:collapse;
                font-size:15px;
                line-height:1.4;
                color:#e5e7eb;
            }}
            .breadth-table td {{
                padding:6px 8px;          /* MATCH sector */
                white-space:nowrap;
                font-weight:500;          /* MATCH sector */
            }}
            .breadth-label {{
                color:#f8fafc;
                font-weight:500;
            }}
            .breadth-value {{
                text-align:center;
                font-weight:500;
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
                <td class="breadth-label">A / D</td>
                <td class="breadth-value" style="color:{ad_color};">
                    {adv} / {dec} ({adv_pct}%)
                </td>
            </tr>
            <tr>
                <td class="breadth-label">Net_Return</td>
                <td class="breadth-value" style="color:{net_color}; font-weight:700;">
                    {net_breadth}%
                </td>
            </tr>
            <tr>
            <td class="breadth-label">NET_R_% (±{user_pct}%)</td>
            <td class="breadth-value" style="color:{net_r_color}; font-weight:700;">
                {NET_R_pct}%
            </td>
        </tr>

        </table>
        """

        st.markdown(table_html, unsafe_allow_html=True)
    #
    # # ✅ CLEAN EXPANDER (NO ARROW TEXT)
    # with st.expander("Show all other indices"):
    #     st.dataframe(other_df, use_container_width=True, hide_index=True)

# ───────────────────────────────────────────────
# 🟣 TABLE 8 — IPO Last 1 Year (RIGHT SIDE)
# ───────────────────────────────────────────────
with row3[2]:
    st.subheader("🟣 IPO ( 1Y) – M.Cap > 500 Cr")

    table8 = table8_last1year_ipo(merged)

    if table8.empty:
        st.info("No IPO matches filters.")
    else:
        # ================================
        # 📊 IPO AFTER LISTING SUMMARY
        # ================================
        tmp = table8.copy()

        tmp["AfterListingNum"] = (
            tmp["After Listing Gain (%)"]
            .astype(str)
            .str.replace("%", "", regex=False)
        )

        tmp["AfterListingNum"] = pd.to_numeric(
            tmp["AfterListingNum"], errors="coerce"
        )

        total_stocks = len(tmp)
        positive_cnt = (tmp["AfterListingNum"] > 0).sum()
        negative_cnt = (tmp["AfterListingNum"] < 0).sum()

        # ================================
        # 🔔 SUMMARY HEADER (LIKE IMAGE 2)
        # ================================
        st.markdown(
            f"""
            <div style="
                display:flex;
                gap:18px;
                align-items:center;
                font-size:16px;
                font-weight:600;
                margin-bottom:8px;
            ">
                <span>📊 Total: <b>{total_stocks}</b></span>
                <span style="color:#00cc44;">🟢 Positive: <b>{positive_cnt}</b></span>
                <span style="color:#ff4d4d;">🔴 Negative: <b>{negative_cnt}</b></span>
            </div>
            """,
            unsafe_allow_html=True
        )

        # 🔽 existing table rendering continues below
        table8 = table8.round(2)
        st.download_button(
            label="⬇️ IPO CSV",
            data=table8.to_csv(index=False),
            file_name="IPO_Last_1Y_MarketCap_Above_500Cr.csv",
            mime="text/csv",
            use_container_width=True
        )

        # Build header
        header_html = """
        <thead>
            <tr>
                <th style='padding:10px;text-align:left;'>Symbol</th>
                <th style='padding:10px;text-align:right;'>LTP</th>
                <th style='padding:10px;text-align:right;'>PcntChg</th>
                <th style='padding:10px;text-align:right;'>Listing Gain (%)</th>
                <th style='padding:10px;text-align:right;'>After Listing Gain (%)</th>
                <th style='padding:10px;text-align:right;'>ValueTrade(Cr)</th>
            </tr>
        </thead>
        """

        import html
        rows_html = []

        for _, r in table8.iterrows():
            # ---- Parse After Listing Gain ----
            try:
                after_gain = float(
                    str(r["After Listing Gain (%)"]).replace("%", "")
                )
            except:
                after_gain = None

            v = float(r["ValueTrade(Cr)"])

            # ---- FINAL PRIORITY COLOR LOGIC ----
            if after_gain is not None and after_gain < 0:
                # 🔴 LOSS AFTER LISTING
                row_style = "background-color:#8B0000; color:white; font-weight:600;"

            elif after_gain is not None and after_gain > 0 and v > 100:
                # 🔵 STRONG POSITIVE + HIGH LIQUIDITY
                row_style = "background-color:#0046FF; color:white; font-weight:600;"

            elif after_gain is not None and after_gain > 0 and 50 <= v <= 100:
                # 🟢 POSITIVE + MEDIUM LIQUIDITY
                row_style = "background-color:#055F6A; color:white; font-weight:600;"

            else:
                row_style = ""

            # Build row cells
            cells = [
                f"<td style='padding:8px;white-space:nowrap'>{html.escape(str(r['Symbol']))}</td>",
                f"<td style='padding:8px;text-align:right'>{r['LTP']:.2f}</td>",
                f"<td style='padding:8px;text-align:right'>{r['PcntChg']:.2f}</td>",
                f"<td style='padding:8px;text-align:right'>{r['Listing Gain (%)']}</td>",
                f"<td style='padding:8px;text-align:right'>{r['After Listing Gain (%)']}</td>",
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

        components.html(html_table, height=700, scrolling=True)

# ───────────────────────────────────────────────
# 🟣 TABLE 9 — VOLUME BREAKER
# ───────────────────────────────────────────────
row4 = st.columns([1])

with row4[0]:
    st.subheader("🔊 Volume Breaker (Vol × ≥ 2 & Value ≥ 100 Cr)")

    vol_df = build_volume_breaker(merged, price_band_col)

    if vol_df.empty:
        st.info("No volume breakout stocks right now.")
    else:
        st.download_button(
            label="⬇️ Download Volume Breaker CSV",
            data=vol_df.to_csv(index=False),
            file_name="Volume_Breaker_Vol2x_Value100Cr.csv",
            mime="text/csv",
            use_container_width=True
        )

        import html
        rows_html = []

        for _, r in vol_df.iterrows():

            base = merged.loc[merged["Symbol"] == r["Symbol"]].iloc[0]

            chg = float(r["PcntChg"])
            ltp = float(r["LTP"])
            # ---------------- VOL | 10 DISPLAY ----------------
            # if r["VOL_10"]:
            #     vol10_text = "🔵 VOL > 10 "
            #     vol10_style = "color:#00BFFF; font-weight:700;"  # Gold
            # else:
            #     vol10_text = "-"
            #     vol10_style = "color:#666;"

            high_52w = (
                float(base["price_52_week_high"])
                if "price_52_week_high" in base and pd.notna(base["price_52_week_high"])
                else None
            )

            # ================= MESSAGE LOGIC =================
            if high_52w and abs((ltp - high_52w) / high_52w) * 100 <= 1:
                message = "BREAKING AVG VOL WITH 52 WEEK HIGH"
            elif chg < 0:
                message = "BREAKING AVG VOL WITH SELLING PRESSURE"
            else:
                message = "BREAKING AVG VOLUME"

            # ================= MESSAGE COLOR =================
            if "DAY LOW" in message:
                msg_style = "color:#8B0000; font-weight:700;"  # 🔴 Dark Red
            elif "SELLING PRESSURE" in message:
                msg_style = "color:#FF6B6B; font-weight:700;"  # 🔴 Light Red
            elif "52 WEEK HIGH" in message:
                msg_style = "color:#B36BFF; font-weight:700;"  # 🟣 Purple
            elif "DAY HIGH" in message:
                msg_style = "color:#00BFFF; font-weight:700;"  # 🔵 Blue
            else:
                msg_style = "color:#00CC44; font-weight:700;"  # 🟢 Green

            # ================= ROW STYLE =================
            row_style = "background-color:#000000; color:white;"

            cells = [
                f"<td style='text-align:center'>{r['Band']}</td>",
                f"<td style='text-align:left'>{html.escape(r['Symbol'])}</td>",
                f"<td style='text-align:center'>{r['LTP']:.2f}</td>",
                f"<td style='text-align:center'>{r['PcntChg']:.2f}</td>",
                f"<td style='text-align:center'>{int(r['volume']):,}</td>",
                f"<td style='text-align:center'>{int(r['AvgVol30']):,}</td>",
                f"<td style='text-align:center'>{r['VolX']:.2f}×</td>",
                # f"<td style='text-align:center; {vol10_style}'>{vol10_text}</td>",  # 👈 NEW
                f"<td style='text-align:center'>{r['Value(Cr)']:.2f}</td>",
                # f"<td style='text-align:left'>{html.escape(str(r['Sector']))}</td>",
                f"<td style='text-align:right'>{html.escape(str(r['Industry']))}</td>",
                f"<td style='text-align:left; padding-left:10px; {msg_style}'>{message}</td>",
            ]

            rows_html.append(f"<tr style='{row_style}'>" + "".join(cells) + "</tr>")

        header_html = """
        <thead>
          <tr>
            <th style="text-align:center;">Band</th>
            <th style="text-align:left;">Symbol</th>
            <th style="text-align:center;">LTP</th>
            <th style="text-align:center;">PcntChg</th>
            <th style="text-align:center;">Volume</th>
            <th style="text-align:center;">Avg Vol (30)</th>
            <th style="text-align:center;">Vol ×</th>

            <th style="text-align:center;">Value(Cr)</th>

            <th style="text-align:center;">Industry</th>
            <th style="text-align:center;">MESSAGE</th>
          </tr>
        </thead>
        """

        table_css = """
        <style>
          .vol-table {
            width:100%;
            border-collapse:separate;
            border-spacing:0;
            background: rgba(255,255,255,0.02);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.06);
            font-family: Cambria, serif;
          }
          .vol-table th {
            background: rgba(255,255,255,0.04);
            padding:10px;
            color:#ddd;
            font-weight:600;
          }
          .vol-table td {
            padding:8px;
            color:#ddd;
            border-bottom:1px solid rgba(255,255,255,0.03);
          }
          .vol-table tr:hover td {
            background: rgba(255,255,255,0.05);
          }
        </style>
        """
        # -------------------------------
        # Dynamic height calculation
        # -------------------------------
        ROW_HEIGHT = 32  # px per row (match CSS)
        HEADER_HEIGHT = 40  # table header height
        MAX_HEIGHT = 700  # max allowed height

        row_count = len(rows_html)

        dynamic_height = min(
            HEADER_HEIGHT + row_count * ROW_HEIGHT,
            MAX_HEIGHT
        )

        # -------------------------------
        # Render HTML table
        # -------------------------------
        components.html(
            f"""
            {table_css}
            <table class="vol-table">
                {header_html}
                <tbody>
                    {''.join(rows_html)}
                </tbody>
            </table>
            """,
            height=dynamic_height,
            scrolling=(row_count * ROW_HEIGHT > MAX_HEIGHT)
        )

# ───────────────────────────────────────────────
# 🟣 TABLE 10 — SECTOR / INDUSTRY ANALYSIS
# ───────────────────────────────────────────────

with st.expander("🔽Market Data Analysis"):

    row5 = st.columns([1])

    with row5[0]:
        st.subheader("📊 Sector & Industry Analysis ")

        # -----------------------------
        # Safety check
        # -----------------------------
        if merged.empty or "Sector" not in merged.columns:
            st.info("Sector data not available.")
        else:

            df = merged.copy()

            # -----------------------------
            # Numeric safety
            # -----------------------------
            num_cols = ["PcntChg", "ValueTrade(Cr)"]
            for c in num_cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            # -----------------------------
            # Sector + Industry aggregation
            # -----------------------------
            sector_df = (
                df.dropna(subset=["Sector", "Industry", "PcntChg"])
                .groupby(["Sector", "Industry"])
                .agg(
                    Total_Stocks=("Symbol", "count"),
                    Advancers=("PcntChg", lambda x: (x > 0).sum()),
                    Decliners=("PcntChg", lambda x: (x < 0).sum()),
                    Avg_Change=("PcntChg", "mean"),
                    Value_Cr=("ValueTrade(Cr)", "sum"),
                )
                .reset_index()
            )

            # -----------------------------
            # Derived metrics
            # -----------------------------
            sector_df["Breadth"] = (
                sector_df["Advancers"] - sector_df["Decliners"]
            )

            def trend(row):
                if row["Avg_Change"] >= 1 and row["Breadth"] > 0:
                    return "🟢 Bullish"
                elif row["Avg_Change"] <= -1 and row["Breadth"] < 0:
                    return "🔴 Bearish"
                else:
                    return "🟡 Neutral"

            sector_df["Trend"] = sector_df.apply(trend, axis=1)

            # -----------------------------
            # Formatting
            # -----------------------------
            sector_df["Avg_Change"] = sector_df["Avg_Change"].round(2)
            sector_df["Value_Cr"] = sector_df["Value_Cr"].round(2)

            # -----------------------------
            # Sorting (Money + Momentum)
            # -----------------------------
            sector_df = sector_df.sort_values(
                by=["Value_Cr", "Avg_Change"],
                ascending=[False, False]
            ).reset_index(drop=True)

            # -----------------------------
            # Display columns
            # -----------------------------
            display_cols = [
                "Industry",
                "Total_Stocks",
                "Advancers",
                "Decliners",
                "Breadth",
                "Avg_Change",
                "Value_Cr",
                "Trend",
            ]

            # -----------------------------
            # Show table
            # -----------------------------
            st.dataframe(
                sector_df[display_cols],
                use_container_width=True,
                hide_index=True
            )

    # ───────────────────────────────────────────────
    # 🔵 ROW 6 — BROADER MARKET CAP ANALYSIS
    # ───────────────────────────────────────────────
    row6 = st.columns([1])

    with row6[0]:
        st.subheader("📊 Broader Market Cap Performance (All Stocks)")

        if merged.empty or "market_cap_basic" not in merged.columns:
            st.info("Market cap data not available.")
        else:
            df = merged.copy()

            # -----------------------------
            # Numeric safety
            # -----------------------------
            df["market_cap_basic"] = pd.to_numeric(df["market_cap_basic"], errors="coerce")
            df["PcntChg"] = pd.to_numeric(df["PcntChg"], errors="coerce")

            df = df.dropna(subset=["market_cap_basic", "PcntChg"])

            # -----------------------------
            # Market Cap in Crores
            # -----------------------------
            df["MktCap(Cr)"] = (df["market_cap_basic"] / 1e7).round(0)

            # # -----------------------------
            # # Sort by Market Cap (DESC)
            # # -----------------------------
            # df = df.sort_values("market_cap_basic", ascending=False).reset_index(drop=True)
            #
            # # -----------------------------
            # # Assign Cap Buckets (Liquidity Aware)
            # # -----------------------------
            # df["CapBucket"] = "Others"
            #
            # df.loc[:99, "CapBucket"] = "Large Cap"
            # df.loc[100:249, "CapBucket"] = "Mid Cap"
            # df.loc[250:399, "CapBucket"] = "Small Cap"
            #
            # # Micro Cap = ALL remaining but liquid only
            # df.loc[
            #     (df.index >= 400) & (df["ValueTrade(Cr)"] > 10),
            #     "CapBucket"
            # ] = "Micro Cap"
            # -----------------------------
            # Sort by Market Cap (DESC)
            # -----------------------------
            df = df.sort_values("market_cap_basic", ascending=False).reset_index(drop=True)

            # -----------------------------
            # Assign Cap Buckets (PURE MCAP)
            # -----------------------------
            df["CapBucket"] = "Micro Cap"  # default = micro

            df.loc[:99, "CapBucket"] = "Large Cap"
            df.loc[100:249, "CapBucket"] = "Mid Cap"
            df.loc[250:499, "CapBucket"] = "Small Cap"

            # -----------------------------
            # Summary per Cap
            # -----------------------------
            cap_summary = (
                df[df["CapBucket"] != "Others"]
                .groupby("CapBucket")
                .agg(
                    Stocks=("Symbol", "count"),
                    Sum_Return=("PcntChg", "sum"),
                    Advancers=("PcntChg", lambda x: (x > 0).sum()),
                    Decliners=("PcntChg", lambda x: (x < 0).sum()),
                )
                .reset_index()
            )

            cap_summary["Net_Avg_Return"] = (
                cap_summary["Sum_Return"] / cap_summary["Stocks"]
            ).round(2)

            def trend(val):
                if val >= 0.5:
                    return "🟢 Bullish"
                elif val <= -0.5:
                    return "🔴 Bearish"
                else:
                    return "🟡 Neutral"

            cap_summary["Trend"] = cap_summary["Net_Avg_Return"].apply(trend)

            # -----------------------------
            # DISPLAY CAPS — 2 PER ROW
            # -----------------------------
            cap_pairs = [
                ("Large Cap", "Mid Cap"),
                ("Small Cap", "Micro Cap")
            ]

            for cap_left, cap_right in cap_pairs:

                col1, col2 = st.columns(2, gap="medium")

                # ========= LEFT CAP =========
                with col1:
                    cap = cap_left
                    cap_df = df[df["CapBucket"] == cap].copy()

                    if not cap_df.empty:
                        s = cap_summary[cap_summary["CapBucket"] == cap].iloc[0]

                        nav = s["Net_Avg_Return"]
                        nav_color = "#00cc44" if nav > 0 else "#ff4d4d" if nav < 0 else "#9ca3af"

                        st.markdown(
                            f"""
                            ### {cap}
                            **Stocks:** {s['Stocks']} |
                            **Net Avg Return:** <span style="color:{nav_color}; font-weight:700;">{nav}%</span> |
                            **A/D:** {s['Advancers']} / {s['Decliners']} |
                            **Trend:** {s['Trend']}
                            """,
                            unsafe_allow_html=True
                        )

                        cap_df = cap_df.sort_values("PcntChg", ascending=False)

                        st.dataframe(
                            cap_df[
                                ["Symbol", "LTP", "PcntChg", "MktCap(Cr)", "ValueTrade(Cr)", "Industry"]
                            ].round(2),
                            height=450,
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info(f"No {cap} stocks")

                # ========= RIGHT CAP =========
                with col2:
                    cap = cap_right
                    cap_df = df[df["CapBucket"] == cap].copy()

                    if not cap_df.empty:
                        s = cap_summary[cap_summary["CapBucket"] == cap].iloc[0]

                        nav = s["Net_Avg_Return"]
                        nav_color = "#00cc44" if nav > 0 else "#ff4d4d" if nav < 0 else "#9ca3af"

                        st.markdown(
                            f"""
                            ### {cap}
                            **Stocks:** {s['Stocks']} |
                            **Net Avg Return:** <span style="color:{nav_color}; font-weight:700;">{nav}%</span> |
                            **A/D:** {s['Advancers']} / {s['Decliners']} |
                            **Trend:** {s['Trend']}
                            """,
                            unsafe_allow_html=True
                        )

                        cap_df = cap_df.sort_values("PcntChg", ascending=False)

                        st.dataframe(
                            cap_df[
                                ["Symbol", "LTP", "PcntChg", "MktCap(Cr)", "ValueTrade(Cr)", "Industry"]
                            ].round(2),
                            height=450,
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info(f"No {cap} stocks")



    # ───────────────────────────────────────────────
    # 🟣 TABLE 7 — INDUSTRY × MARKET CAP ANALYSIS
    # (USER SELECTED | SINGLE TABLE)
    # ───────────────────────────────────────────────

    st.subheader("🏭 Industry-wise Market Cap Action")

    # -----------------------------
    # Industry Selector (Dynamic)
    # -----------------------------
    industry_list = sorted(
        df["Industry"].dropna().unique().tolist()
    )

    selected_industry = st.selectbox(
        "Select Industry",
        industry_list
    )

    # -----------------------------
    # Filter Selected Industry
    # -----------------------------
    ind_df = df[df["Industry"] == selected_industry].copy()

    if ind_df.empty:
        st.warning("No data available for selected industry.")
        st.stop()

    # -----------------------------
    # Cap-wise Summary
    # -----------------------------
    cap_summary = (
        ind_df[ind_df["CapBucket"] != "Others"]
        .groupby("CapBucket")
        .agg(
            Stocks=("Symbol", "count"),
            Sum_Return=("PcntChg", "sum"),
            Advancers=("PcntChg", lambda x: (x > 0).sum()),
            Decliners=("PcntChg", lambda x: (x < 0).sum()),
        )
        .reset_index()
    )

    cap_summary["Net_Avg_Return"] = (
        cap_summary["Sum_Return"] / cap_summary["Stocks"]
    ).round(2)

    def trend(val):
        if val >= 0.5:
            return "🟢 Bullish"
        elif val <= -0.5:
            return "🔴 Bearish"
        else:
            return "🟡 Neutral"

    cap_summary["Trend"] = cap_summary["Net_Avg_Return"].apply(trend)

    # -----------------------------
    # Show Cap Summary Table
    # -----------------------------
    st.markdown("### 📌 Cap-wise Performance Summary")

    st.dataframe(
        cap_summary[
            ["CapBucket", "Stocks", "Net_Avg_Return", "Advancers", "Decliners", "Trend"]
        ],
        use_container_width=True,
        hide_index=True
    )

    # -----------------------------
    # Prepare Final Combined Table
    # -----------------------------
    cap_order = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]

    ind_df["CapBucket"] = pd.Categorical(
        ind_df["CapBucket"],
        categories=cap_order,
        ordered=True
    )

    # Sort by CapBucket → %Change
    ind_df = ind_df.sort_values(
        ["CapBucket", "PcntChg"],
        ascending=[True, False]
    )

    # -----------------------------
    # Display Final Industry Table
    # -----------------------------
    st.markdown(f"### 📊 {selected_industry} — All Market Caps")

    st.dataframe(
        ind_df[
            [
                "CapBucket",
                "Symbol",
                "LTP",
                "PcntChg",
                "MktCap(Cr)",
                "ValueTrade(Cr)",
                "Industry",
            ]
        ].round(2),
        height=500,
        use_container_width=True,
        hide_index=True
    )
