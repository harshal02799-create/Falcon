import streamlit as st
from nav import nav_menu
from streamlit_autorefresh import st_autorefresh

# -----------------------------------
# PAGE CONFIG MUST BE FIRST
# -----------------------------------
st.set_page_config(page_title="BSE Dashboard", layout="wide")

# -----------------------------------
# AUTO REFRESH (every 15 sec)
# -----------------------------------
st_autorefresh(interval=15_000, key="bse_ltp_refresh")

nav_menu()

# ---------------------------
# BSE: Download / Update module
# ---------------------------
import os
import requests
import zipfile
import shutil
from datetime import datetime
import streamlit as st

# Determine project root (same logic you used elsewhere)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# bse_files folder (auto-created)
BSE_FOLDER = os.path.join(PROJECT_ROOT, "bse_files")
SCRIP_DATA = os.path.join(BSE_FOLDER, "SCRIP_DATA")
os.makedirs(SCRIP_DATA, exist_ok=True)   # creates bse_files/ and SCRIP_DATA/ if missing

# status file to store last update date
STATUS_FILE = os.path.join(SCRIP_DATA, "last_update.txt")

def get_last_update():
    if os.path.exists(STATUS_FILE):
        try:
            return open(STATUS_FILE, "r", encoding="utf-8").read().strip()
        except:
            return None
    return None

def set_last_update(date_str):
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        f.write(date_str)

def download_and_update_scrip(st_context=None):
    """
    Downloads SCRIP.zip from BSE, extracts SCRIP folder, and appends/moves files
    into bse_files/SCRIP_DATA. Returns (True, msg) on success, (False, msg) on error.
    """
    url = "https://www.bseindia.com/downloads/help/SCRIP.zip"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bseindia.com/downloads/help/file/",
    }

    today = datetime.now().strftime("%Y-%m-%d")
    zip_path = os.path.join(BSE_FOLDER, f"SCRIP_{today}.zip")
    temp_extract = os.path.join(BSE_FOLDER, "TEMP_EXTRACT")
    os.makedirs(temp_extract, exist_ok=True)

    # Download
    try:
        r = requests.get(url, headers=headers, timeout=30)
    except Exception as e:
        return False, f"Download error: {e}"

    if r.status_code != 200:
        return False, f"HTTP {r.status_code} while downloading SCRIP.zip"

    try:
        with open(zip_path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        return False, f"Failed writing zip: {e}"

    # Extract
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(temp_extract)
    except Exception as e:
        # cleanup zip
        if os.path.exists(zip_path):
            os.remove(zip_path)
        shutil.rmtree(temp_extract, ignore_errors=True)
        return False, f"Failed to extract zip: {e}"

    extracted_scrip = os.path.join(temp_extract, "SCRIP")
    if not os.path.isdir(extracted_scrip):
        # Some zips may put the folder in different path; try to search
        found = None
        for root, dirs, files in os.walk(temp_extract):
            if os.path.basename(root).upper() == "SCRIP":
                found = root
                break
        if found:
            extracted_scrip = found
        else:
            # cleanup
            shutil.rmtree(temp_extract, ignore_errors=True)
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False, "SCRIP folder not found inside ZIP."

    # Process files: if dest exists -> append, else move
    try:
        for fname in os.listdir(extracted_scrip):
            src = os.path.join(extracted_scrip, fname)
            dest = os.path.join(SCRIP_DATA, fname)

            # ensure we only process files (skip nested dirs)
            if os.path.isdir(src):
                # if nested folder, skip or optionally process recursively (skip here)
                continue

            if os.path.exists(dest):
                # append in binary mode with newline separator
                with open(src, "rb") as f_src, open(dest, "ab") as f_dest:
                    # only add newline if dest not empty and last byte isn't newline
                    try:
                        f_dest.seek(0, os.SEEK_END)
                        if f_dest.tell() > 0:
                            f_dest.write(b"\n")
                    except:
                        f_dest.write(b"\n")
                    f_dest.write(f_src.read())
            else:
                # first time: move file into SCRIP_DATA
                shutil.move(src, dest)
    except Exception as e:
        shutil.rmtree(temp_extract, ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return False, f"Failed processing extracted files: {e}"

    # cleanup
    shutil.rmtree(temp_extract, ignore_errors=True)
    if os.path.exists(zip_path):
        os.remove(zip_path)

    # mark update
    set_last_update(today)
    return True, f"SCRIP updated: {today}"

def bse_download_button_ui():
    """
    Render button at top. If already updated today, show success and return True.
    If not updated, show download button. Returns True if updated today (already or just now).
    """
    # st.header("📦 BSE SCRIP Data (Daily)")

    today = datetime.now().strftime("%Y-%m-%d")
    last = get_last_update()

    if last == today:
        st.success(f"✔ SCRIP Updated Today ({today})")
        return True

    st.info("SCRIP not yet updated today.")
    if st.button("⬇ Download Today's SCRIP File"):
        with st.spinner("Downloading and updating SCRIP..."):
            ok, msg = download_and_update_scrip()
        if ok:
            st.success(msg)
            return True
        else:
            st.error(msg)
            return False

    # user hasn't clicked yet and not updated today
    return False

# ---------------------------
# End module
# ---------------------------

# CALL UI at top — user chose option A (top of page)
updated_today = bse_download_button_ui()

# If not updated yet (and not already updated), stop execution so app waits until user downloads
if not updated_today:
    st.warning("Please download today's SCRIP file to continue.")
    st.stop()

# Now SCRIP_DATA contains the files; set SCRIP_FOLDER variable used later in your code
SCRIP_FOLDER = SCRIP_DATA
print("Using SCRIP folder:", SCRIP_FOLDER)

import os
import pandas as pd
def glossy_html_table(df, band, height=520):
    if df.empty:
        return "<div>No data</div>"

    # Color rules based on band
    def band_color(val):
        try:
            v = float(val)
        except:
            return ""

        if band == 5:
            if v >= 4.5: return "background-color:#0046FF; color:white;"
            if v >= 3:   return "background-color:#00CC44; color:white;"
            if v <= -4.5: return "background-color:#FF3B30; color:white;"
            if v <= -3:   return "background-color:#055F6A; color:white;"

        if band == 10:
            if v >= 9: return "background-color:#0046FF; color:white;"
            if v >= 6: return "background-color:#00CC44; color:white;"
            if v <= -9: return "background-color:#FF3B30; color:white;"
            if v <= -6: return "background-color:#055F6A; color:white;"

        if band == 20:
            if v >= 19: return "background-color:#0046FF; color:white;"
            if v >= 16: return "background-color:#00CC44; color:white;"
            if v >= 12: return "background-color:#055F6A; color:white;"
            if v <= -12: return "background-color:#FF3B30; color:white;"

        return ""

    cols = [c for c in ["TckrSymb", "Ser", "LTP", "PcntChg", "ValueTrade(Cr)"] if c in df.columns]

    rows_html = []
    for _, r in df.iterrows():
        style = band_color(r.get("PcntChg", 0))
        cells = []
        for c in cols:
            v = r.get(c, "-")
            if pd.isna(v):
                v = "-"
            elif isinstance(v, (int, float)):
                v = f"{v:.2f}"
            else:
                v = str(v)

            if c == "TckrSymb":
                align = "left"
            else:
                align = "center"
            cells.append(f"<td style='padding:8px; text-align:{align}'>{v}</td>")

        rows_html.append(f"<tr style='{style}'>{''.join(cells)}</tr>")

    header_html = "<thead><tr>" + "".join(
        [f"<th style='padding:10px; text-align:center'>{c}</th>" for c in cols]
    ) + "</tr></thead>"

    css = """
    <style>
      .glossy-table {
        width:100%;
        border-collapse:separate;
        border-spacing:0;
        background: rgba(255,255,255,0.02);
        border-radius: 12px;
        overflow:hidden;
        border: 1px solid rgba(255,255,255,0.06);
        font-family: Cambria, serif;
        color:#ddd;
      }
      .glossy-table td {
        border-bottom: 1px solid rgba(255,255,255,0.03);
        white-space:nowrap;
      }
      .glossy-table th {
        background: rgba(255,255,255,0.05);
        color:#ddd;
      }
    </style>
    """

    table = f"""
    {css}
    <table class='glossy-table'>
        {header_html}
        <tbody>
            {''.join(rows_html)}
        </tbody>
    </table>
    """

    return table
import os
#
# def find_scrip_folder(start_path):
#     target = "BSE_EXTRACT"
#     for root, dirs, files in os.walk(start_path):
#         if target in dirs:
#             scrip_path = os.path.join(root, target, "SCRIP")
#             if os.path.exists(scrip_path):
#                 return scrip_path
#     return None
#
# # start searching from current project root
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# SCRIP_FOLDER = find_scrip_folder(PROJECT_ROOT)
#
# print("🔍 Searching SCRIP folder inside:", PROJECT_ROOT)
# print("📂 Found:", SCRIP_FOLDER)
#
# if not SCRIP_FOLDER:
#     raise Exception("❌ Could not find BSE_EXTRACT/SCRIP folder in this project structure!")


# =============================================================
# 2️⃣ AUTO-DETECT FILES
# =============================================================
csv_file = None
dp_file = None

for file in os.listdir(SCRIP_FOLDER):

    # BSE Equity file
    if file.startswith("BSE_EQ_SCRIP") and file.endswith(".csv"):
        csv_file = os.path.join(SCRIP_FOLDER, file)

    # DP file (txt or no extension)
    if file.startswith("DP") and (file.endswith(".txt") or "." not in file):
        dp_file = os.path.join(SCRIP_FOLDER, file)

print("\n📌 Files Found:")
print("BSE_EQ CSV:", csv_file)
print("DP File   :", dp_file)

if not csv_file or not dp_file:
    raise Exception("❌ Required files not found inside SCRIP folder.")


# =============================================================
# 3️⃣ LOAD + CLEAN BSE_EQ FILE
# =============================================================
df_eq = pd.read_csv(csv_file, dtype=str)
df_eq.columns = df_eq.columns.str.strip()

required_eq_cols = ["FinInstrmId", "TckrSymb", "SctySrs"]
df_eq = df_eq[required_eq_cols]
#
# print("\n📄 CLEAN BSE_EQ Columns:")
# print(df_eq.head())


# =============================================================
# 4️⃣ LOAD + CLEAN DP FILE (Auto-detect delimiter)
# =============================================================
possible_delims = [",", "|", "\t", " "]
delimiter_used = None

for d in possible_delims:
    try:
        temp = pd.read_csv(dp_file, sep=d, dtype=str, engine="python")
        if temp.shape[1] > 1:
            delimiter_used = d
            break
    except:
        pass

if delimiter_used:
    df_dp = pd.read_csv(dp_file, sep=delimiter_used, dtype=str, engine="python")
    print(f"\n🧩 DP delimiter detected: {repr(delimiter_used)}")
else:
    df_dp = pd.read_fwf(dp_file, dtype=str)
    print("\n⚠ Fallback to fixed-width format DP file.")

df_dp.columns = df_dp.columns.str.strip()

# Find DP required columns (names vary)
col_scrip_code = next((c for c in df_dp.columns if "scrip" in c.lower() and "code" in c.lower()), None)
col_upper_circuit = next((c for c in df_dp.columns if "upper" in c.lower() and "circuit" in c.lower()), None)

if not col_scrip_code or not col_upper_circuit:
    raise Exception("❌ Could not find required DP columns in DP file!")

df_dp = df_dp[[col_scrip_code, col_upper_circuit]]
df_dp = df_dp.rename(columns={
    col_scrip_code: "ScripCode",
    col_upper_circuit: "UpperCircuit"
})


# =============================================================
# 5️⃣ FINAL CLEAN DATAFRAMES
# =============================================================
eq = df_eq.copy()
dp = df_dp.copy()

eq["FinInstrmId"] = pd.to_numeric(eq["FinInstrmId"], errors="coerce")
dp["ScripCode"] = pd.to_numeric(dp["ScripCode"], errors="coerce")

eq = eq.dropna(subset=["FinInstrmId"])
dp = dp.dropna(subset=["ScripCode"])

eq["FinInstrmId"] = eq["FinInstrmId"].astype(int)
dp["ScripCode"] = dp["ScripCode"].astype(int)


# =============================================================
# 6️⃣ MERGE EQ + DP  (FinInstrmId ↔ ScripCode)
# =============================================================
merged = eq.merge(dp, left_on="FinInstrmId", right_on="ScripCode", how="left")


# =============================================================
# 7️⃣ REMOVE (NAV, ETF, #)
# =============================================================
merged["TckrSymb"] = merged["TckrSymb"].astype(str)

remove_patterns = ("NAV", "ETF", "#")
merged = merged[~merged["TckrSymb"].str.upper().str.endswith(remove_patterns)]


# =============================================================
# 8️⃣ KEEP ONLY VALID SERIES
# =============================================================
valid_series = {"A", "B", "X", "XT", "M", "MT", "T"}

merged = merged[
    merged["SctySrs"].astype(str).str.upper().isin(valid_series)
].reset_index(drop=True)


import requests
from io import StringIO

# =====================================================
# 🔽 DOWNLOAD NSE sec_list.csv
# =====================================================
url = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com"
}

print("\n🌐 Downloading NSE sec_list.csv ...")
resp = requests.get(url, headers=headers)

if resp.status_code != 200:
    raise Exception(f"❌ Failed to download sec_list.csv — HTTP {resp.status_code}")

df_nse = pd.read_csv(StringIO(resp.text), dtype=str)
df_nse.columns = df_nse.columns.str.strip()

if "Symbol" not in df_nse.columns:
    raise Exception("❌ 'Symbol' column missing in sec_list.csv")

# Clean NSE Symbol
df_nse["Symbol"] = df_nse["Symbol"].astype(str).str.strip().str.upper()
nse_symbols = set(df_nse["Symbol"].tolist())

print(f"🔹 Total NSE symbols loaded: {len(nse_symbols)}")


# =====================================================
# 🧹 CLEAN merged TckrSymb BEFORE COMPARISON
# =====================================================
merged["TckrSymb"] = (
    merged["TckrSymb"]
    .astype(str)
    .str.replace("#", "", regex=False)
    .str.strip()
    .str.upper()
)

# =====================================================
# ❌ REMOVE ROWS PRESENT IN NSE SYMBOL LIST
# =====================================================
before = len(merged)
merged = merged[~merged["TckrSymb"].isin(nse_symbols)].reset_index(drop=True)
after = len(merged)

print(f"🧹 Removed rows based on NSE symbol match: {before - after}")
print(f"✅ Final merged rows: {after}")
print("\n📌 FINAL MERGED + CLEANED PREVIEW:")
print(merged.head(50).to_string())

# =============================================================
# 9️⃣ CLEAN AND NORMALIZE UpperCircuit COLUMN
# =============================================================

# Convert UpperCircuit to numeric safely
merged["UpperCircuit"] = (
    merged["UpperCircuit"]
        .astype(str)              # convert everything to string
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace("", pd.NA)       # empty -> NA
)

merged["UpperCircuit"] = pd.to_numeric(merged["UpperCircuit"], errors="coerce").astype("Int64")

# Any missing values → treat as 0 (no band)
merged["UpperCircuit"] = merged["UpperCircuit"].fillna(0).astype("Int64")


# =============================================================
# 🔟 CREATE 5%, 10%, 20% BAND DATAFRAMES
# =============================================================

band5  = merged[merged["UpperCircuit"] == 5].copy()
band10 = merged[merged["UpperCircuit"] == 10].copy()
band20 = merged[merged["UpperCircuit"] == 20].copy()

# BSE_Dashboard.py
import os
import pandas as pd
import requests
from io import StringIO
from datetime import datetime
from tradingview_screener import Query
import html
import streamlit as st
import streamlit.components.v1 as components

# -------------------------------
# NOTE:
# This script expects that earlier in this same file (or imported)
# you have already produced `merged` dataframe that contains:
#  - TckrSymb (symbol)
#  - FinInstrmId / ScripCode
#  - SctySrs
#  - FinInstrmNm
#  - UpperCircuit
#
# If you build merged earlier (as in your posted code), leave as is.
# Otherwise import merged from your merge script.
# -------------------------------

# -------------------------------
# Defensive: ensure merged exists (if not, stop with helpful message)
# -------------------------------
if "merged" not in globals():
    st.error("`merged` DataFrame not found in this namespace. Make sure you run the BSE merge code above or import `merged` into this file.")
    st.stop()

# make a copy to avoid accidental mutation
merged = merged.copy()
# Rename SctySrs to Ser
merged = merged.rename(columns={"SctySrs": "Ser"})

# -------------------------------
# CLEAN / NORMALIZE UpperCircuit and TckrSymb
# -------------------------------
# UpperCircuit sometimes is string like '20' or ' 20 ' — normalize to int
merged["UpperCircuit"] = merged["UpperCircuit"].astype(str).str.strip().replace("", pd.NA)
merged["UpperCircuit"] = pd.to_numeric(merged["UpperCircuit"], errors="coerce").astype("Int64")

# Symbol normalization
merged["TckrSymb"] = merged["TckrSymb"].astype(str).str.replace("#", "", regex=False).str.strip().str.upper()

# Remove rows w/o symbol or with invalid series (already done earlier but double-check)
merged = merged.dropna(subset=["TckrSymb"]).reset_index(drop=True)

# -------------------------------
# TRADINGVIEW FETCH (BSE FIRST)
# -------------------------------
def fetch_tradingview_data():
    # Query TradingView scanner
    n_rows, tv = (
        Query()
        .select(
            'name', 'exchange', 'close', 'change',
            'price_52_week_high', 'High.All', 'volume',
            'Value.Traded', 'market_cap_basic'
        )
        .set_markets('india')
        .limit(9000)
        .get_scanner_data()
    )

    tv = tv.rename(columns={
        'name': 'Symbol',
        'close': 'LTP',
        'change': 'PcntChg',
        'price_52_week_high': '52W_High',
        'High.All': 'ATH'
    })

    # Uppercase symbol
    tv["Symbol"] = tv["Symbol"].astype(str).str.upper()

    # Prioritize BSE rows (so BSE symbols override NSE duplicates)
    tv = tv.sort_values(
        by='exchange',
        key=lambda x: x.eq('BSE'),
        ascending=False
    ).drop_duplicates(subset=['Symbol'], keep='first').reset_index(drop=True)

    # numeric conversions
    tv["LTP"] = pd.to_numeric(tv.get("LTP"), errors="coerce")
    tv["PcntChg"] = pd.to_numeric(tv.get("PcntChg"), errors="coerce")
    tv["52W_High"] = pd.to_numeric(tv.get("52W_High"), errors="coerce")
    tv["ATH"] = pd.to_numeric(tv.get("ATH"), errors="coerce")
    tv["volume"] = pd.to_numeric(tv.get("volume"), errors="coerce")

    # ValueTrade in Cr if present
    if "Value.Traded" in tv.columns:
        tv["ValueTrade(Cr)"] = pd.to_numeric(tv["Value.Traded"], errors="coerce") / 1e7
    else:
        tv["ValueTrade(Cr)"] = pd.NA

    # fallback: if ValueTrade missing, compute from volume * LTP
    def compute_vt(row):
        vt = row.get("ValueTrade(Cr)")
        if pd.isna(vt):
            v = row.get("volume")
            p = row.get("LTP")
            if pd.notna(v) and pd.notna(p):
                try:
                    return (float(v) * float(p)) / 1e7
                except:
                    return pd.NA
            return pd.NA
        return vt

    tv["ValueTrade(Cr)"] = tv.apply(compute_vt, axis=1)

    return tv
# -------------------------------
# MERGE BSE merged with TradingView
# -------------------------------
def merge_bse_with_tv(merged):
    tv = fetch_tradingview_data()

    # Normalize TV symbol to match BSE
    tv["Symbol"] = (
        tv["Symbol"]
        .str.upper()
        .str.replace("BSE:", "", regex=False)
        .str.strip()
    )

    # Remove symbols that are only numeric (avoid wrong merge)
    tv = tv[tv["Symbol"].str.contains("[A-Z]", regex=True)]

    merged["TckrSymb"] = merged["TckrSymb"].astype(str).str.upper().str.strip()

    # Merge BSE + TV
    final = merged.merge(tv, left_on="TckrSymb", right_on="Symbol", how="left")

    # Convert numeric fields
    for c in ["LTP", "PcntChg", "52W_High", "ATH", "ValueTrade(Cr)", "volume"]:
        final[c] = pd.to_numeric(final.get(c), errors="coerce")

    # fallback ValueTrade
    final["ValueTrade(Cr)"] = final.apply(
        lambda r: (r["volume"] * r["LTP"]) / 1e7
        if pd.isna(r["ValueTrade(Cr)"]) and pd.notna(r["volume"]) and pd.notna(r["LTP"])
        else r["ValueTrade(Cr)"],
        axis=1
    )

    # -------------------------------
    # RENAME COLUMN HERE
    # -------------------------------
    if "SctySrs" in final.columns:
        final = final.rename(columns={"SctySrs": "Ser"})

    return final

# -------------------------------
# STYLING HELPERS (reused/compact)
# -------------------------------
def get_color_for_band(pcnt, band):
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

def style_band_fullrow(df, band):
    # make defensive copy
    df = df.copy()
    for c in ["LTP", "PcntChg", "ValueTrade(Cr)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
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
    styler = df.style.apply(lambda _: styles, axis=None)
    styler = styler.format({
        "LTP": "{:.2f}",
        "PcntChg": "{:.2f}",
        "ValueTrade(Cr)": "{:.2f}"
    }).set_properties(**{'text-align': 'center'}).set_table_styles([{'selector': 'th', 'props':[('text-align','center')]}])
    return styler

# -------------------------------
# Breakout message helper
# -------------------------------
def get_breakout_msg(row):
    try:
        ltp = float(row.get("LTP")) if pd.notna(row.get("LTP")) else None
        ath = float(row.get("ATH")) if pd.notna(row.get("ATH")) else None
        high52 = float(row.get("52W_High")) if pd.notna(row.get("52W_High")) else None
    except:
        return None

    if ltp is None:
        return None
    if ath is not None and ltp >= ath:
        return "🔥 Breaking All-Time High"
    if high52 is not None and ltp >= high52:
        return "📈 Breaking 52W High"
    return None

# -------------------------------
# Streamlit UI
# -------------------------------
# st.set_page_config(page_title="BSE Dashboard", layout="wide")
st.markdown("""
<style>
    * { font-family: Cambria, serif !important; font-size: 15px !important; }
</style>
""", unsafe_allow_html=True)

# st.title("📊 BSE Dashboard (UpperCircuit → Bands, TradingView LTP)")

with st.spinner("Fetching TradingView & preparing data..."):
    final = merge_bse_with_tv(merged)
# Remove duplicate symbols after merging with TradingView
final = final.drop_duplicates(subset=["TckrSymb"], keep="first")

# 🚫 Remove rows where ValueTrade(Cr) is less than 50 lakhs (0.50 Cr)
final["ValueTrade(Cr)"] = pd.to_numeric(final["ValueTrade(Cr)"], errors="coerce")
final = final[final["ValueTrade(Cr)"] >= 0.50].copy()

# 🔥 FIX UpperCircuit completely
final["UpperCircuit"] = (
    final["UpperCircuit"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace("", pd.NA)
)

final["UpperCircuit"] = pd.to_numeric(final["UpperCircuit"], errors="coerce").astype("Int64")

# Correct band filtering
band5  = final[final["UpperCircuit"] == 5].copy()
band10 = final[final["UpperCircuit"] == 10].copy()
band20 = final[final["UpperCircuit"] == 20].copy()

# ========================
# Row 1: 5 / 10 / 20 band tables
# ========================
# st.header("Row 1 — Price Bands (from UpperCircuit)")

r1c1, r1c2, r1c3 = st.columns(3)

final_cols = ["TckrSymb", "LTP", "PcntChg", "ValueTrade(Cr)"]

with r1c1:
    st.subheader("🟦 5% BAND")
    if band5.empty:
        st.info("No 5% band symbols.")
    else:
        html_code = glossy_html_table(band5.sort_values("PcntChg", ascending=False), band=5)
        components.html(html_code, height=520, scrolling=True)


with r1c2:
    st.subheader("🟩 10% BAND")
    if band10.empty:
        st.info("No 10% band symbols.")
    else:
        html_code = glossy_html_table(band10.sort_values("PcntChg", ascending=False), band=10)
        components.html(html_code, height=520, scrolling=True)

with r1c3:
    st.subheader("🟧 20% BAND")
    if band20.empty:
        st.info("No 20% band symbols.")
    else:
        html_code = glossy_html_table(band20.sort_values("PcntChg", ascending=False), band=20)
        components.html(html_code, height=520, scrolling=True)
#
# st.divider()
# ========================
# Row 2 — 52W High / ATH & ValueTrade ≥ 1 Cr
# ========================
# st.header("Row 2 — 52W High / ATH & ValueTrade ≥ 1 Cr")

c2a, c2b = st.columns(2)

# ---------- COL A: 52W High / ATH ----------
with c2a:
    st.subheader("📈 52W High / ATH")

    tmp = final.copy()
    tmp["Dist_52W(%)"] = tmp.apply(
        lambda r: ((r["LTP"] - r["52W_High"]) / r["52W_High"] * 100)
        if pd.notna(r["LTP"]) and pd.notna(r["52W_High"]) and r["52W_High"] != 0 else pd.NA,
        axis=1
    )
    tmp = tmp.sort_values("Dist_52W(%)", ascending=False)

    cols_show = ["TckrSymb" , "Ser" , "LTP", "PcntChg", "52W_High", "ATH", "ValueTrade(Cr)", "Dist_52W(%)"]

    html_code = glossy_html_table(tmp[cols_show], band=20)
    components.html(html_code, height=520, scrolling=True)


# ---------- COL B: ValueTrade ≥ 1 Cr ----------
with c2b:
    st.subheader("💰 ValueTrade ≥ 1 Cr")

    vt = final.copy()
    vt["ValueTrade(Cr)"] = pd.to_numeric(vt["ValueTrade(Cr)"], errors="coerce")

    vt_filtered = vt[vt["ValueTrade(Cr)"] >= 1].sort_values(
        "ValueTrade(Cr)", ascending=False
    )

    cols_show = ["TckrSymb", "Ser", "LTP", "PcntChg", "ValueTrade(Cr)"]

    if vt_filtered.empty:
        st.info("No symbols with ValueTrade ≥ 1 Cr")
    else:
        html_code = glossy_html_table(vt_filtered[cols_show], band=20)
        components.html(html_code, height=520, scrolling=True)

# ========================
# Row 3: Breakout Candidates
# ========================
# st.header("Row 3 — Breakout Candidates (Breaking 52W / ATH)")

final["BreakoutMSG"] = final.apply(get_breakout_msg, axis=1)
bo = final[final["BreakoutMSG"].notna()].copy()
bo = bo.sort_values("PcntChg", ascending=False)

cols_show = ["TckrSymb","Ser" ,"LTP", "PcntChg", "52W_High", "ATH", "ValueTrade(Cr)", "BreakoutMSG"]

if bo.empty:
    st.info("No breakout candidates right now.")
else:
    rows_html = []
    for _, r in bo.iterrows():
        msg = str(r.get("BreakoutMSG", ""))

        # ------------------------
        # ⭐ NEW COLOR LOGIC
        # ------------------------
        if "All-Time High" in msg:
            style = "background-color:#0046FF; color:white; font-weight:600;"   # BLUE
        elif "52W High" in msg:
            style = "background-color:#00CC44; color:white; font-weight:600;"   # GREEN
        else:
            style = ""

        cells = []
        for c in cols_show:
            v = r.get(c, "")
            if pd.isna(v):
                display = "-"
            else:
                display = f"{v:.2f}" if isinstance(v, (int, float)) else html.escape(str(v))

            align = "center" if c in ["LTP", "PcntChg", "ValueTrade(Cr)", "52W_High", "ATH"] else "left"
            cells.append(f"<td style='padding:8px; text-align:{align}; white-space:nowrap'>{display}</td>")

        rows_html.append(f"<tr style='{style}'>" + "".join(cells) + "</tr>")

    header_html = "<thead><tr>" + "".join(
        [f"<th style='padding:10px;text-align:left'>{html.escape(c)}</th>" for c in cols_show]
    ) + "</tr></thead>"

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
        color:#ddd;
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
    <table class='breakout-table'>
      {header_html}
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    """

    components.html(html_table, height=560, scrolling=True)

# Footer summary
st.markdown(f"""
**Summary (BSE)**  
Total BSE merged rows: **{len(merged)}** — After merge: **{len(final)}**  
5% band: **{len(band5)}** | 10% band: **{len(band10)}** | 20% band: **{len(band20)}**
""")

