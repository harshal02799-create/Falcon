# import requests
# import pandas as pd
# import re
# import csv
# import io
# from datetime import datetime, timedelta
# import os
# from tradingview_screener import Query, Column
#
#
# # =========================================================
# # 🔵 TRADINGVIEW DATA (NSE ONLY)
# # =========================================================
# def fetch_tradingview_data():
#
#     _, tv = (
#         Query()
#         .select(
#             'name',
#             'exchange',
#             'close',
#             'change',
#             'volume',
#             'market_cap_basic'
#         )
#         .set_markets('india')
#         .where(Column('exchange') == 'NSE')
#         .limit(9000)
#         .get_scanner_data()
#     )
#
#     tv = tv.rename(columns={
#         'name': 'Symbol',
#         'close': 'LTP',
#         'change': 'PcntChg'
#     })
#
#     tv["Symbol"] = tv["Symbol"].astype(str).str.upper()
#     tv = tv.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
#
#     return tv
#
#
# # =========================================================
# # 0️⃣ READ sec_list.csv (OPTIONAL)
# # =========================================================
# def read_sec_list_csv(path):
#
#     if not os.path.exists(path):
#         print(f"⚠️ sec_list.csv NOT FOUND: {path}")
#         return pd.DataFrame(columns=["SYMBOL", "SECURITY_NAME"])
#
#     df = pd.read_csv(path)
#     df.columns = [c.strip().upper() for c in df.columns]
#     df = df.rename(columns={"SECURITY NAME": "SECURITY_NAME"})
#
#     df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
#     df["SECURITY_NAME"] = df["SECURITY_NAME"].astype(str).str.strip()
#
#     return df
#
#
# # =========================================================
# # 1️⃣ INVESTORGAIN — LISTED IPO DATA
# # =========================================================
# def fetch_listed_only_ipo_df():
#
#     urls = [
#         "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2025/2025-26/0/ipo?search=&v=16-59",
#         "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2026/2025-26/0/ipo?search=&v=17-19"
#     ]
#
#     headers = {"User-Agent": "Mozilla/5.0"}
#
#     dfs = []
#     for url in urls:
#         try:
#             r = requests.get(url, headers=headers, timeout=10)
#             rows = r.json().get("reportTableData", [])
#             if rows:
#                 dfs.append(pd.DataFrame(rows))
#         except:
#             continue
#
#     if not dfs:
#         return pd.DataFrame()
#
#     df = pd.concat(dfs, ignore_index=True)
#
#     df = df.apply(lambda col: col.map(
#         lambda x: re.sub(r"<.*?>", "", str(x)).strip()
#     ))
#
#     status = df["Status"].str.extract(r"L@([\d.]+).*?\((.*?)\)")
#     df["Listing Price"] = status[0]
#     df["Listing Gain (%)"] = status[1]
#
#     df["IPO Price"] = df["IPO Price"].str.replace("₹", "").str.strip()
#     df = df.dropna(subset=["Listing Price"])
#
#     return df[["IPO", "Listing Price", "Listing Gain (%)"]].copy()
#
#
# # =========================================================
# # 2️⃣ NSE — COMPANY NAME ↔ SYMBOL
# # =========================================================
# def fetch_nse_company_symbol_df():
#
#     today = datetime.now()
#     from_date = (today - timedelta(days=365)).strftime("%d-%m-%Y")
#     to_date = today.strftime("%d-%m-%Y")
#
#     url = (
#         "https://www.nseindia.com/api/public-past-issues"
#         f"?from_date={from_date}&to_date={to_date}"
#         f"&security_type=Equity&csv=true"
#     )
#
#     headers = {"User-Agent": "Mozilla/5.0"}
#     session = requests.Session()
#     session.get("https://www.nseindia.com", headers=headers)
#
#     r = session.get(url, headers=headers)
#     csv_text = r.content.decode("utf-8-sig")
#
#     reader = csv.DictReader(io.StringIO(csv_text))
#     rows = []
#
#     for row in reader:
#         if row.get("COMPANY NAME"):
#             rows.append({
#                 "COMPANY NAME": row["COMPANY NAME"].strip(),
#                 "SYMBOL": row["Symbol"].strip().upper()
#             })
#
#     return pd.DataFrame(rows)
#
#
# # =========================================================
# # 3️⃣ SMART IPO → SYMBOL MAPPING
# # =========================================================
# def smart_bucket_merge(df_ipo, df_nse, df_sec):
#
#     def fw(x): return x.split()[0].upper()
#
#     out = []
#
#     for _, r in df_ipo.iterrows():
#         ipo = r["IPO"].upper()
#         symbol = None
#
#         for _, n in df_nse.iterrows():
#             if fw(n["COMPANY NAME"]) == fw(ipo):
#                 symbol = n["SYMBOL"]
#                 break
#
#         if symbol is None:
#             for _, s in df_sec.iterrows():
#                 if fw(s["SECURITY_NAME"]) == fw(ipo):
#                     symbol = s["SYMBOL"]
#                     break
#
#         if symbol is None:
#             symbol = ipo.replace(" ", "")[:15]
#
#         out.append({
#             "Symbol": symbol,
#             "Listing Price": float(r["Listing Price"]),
#             "Listing Gain (%)": r["Listing Gain (%)"]
#         })
#
#     return pd.DataFrame(out)
#
#
# # =========================================================
# # 4️⃣ MERGE WITH TRADINGVIEW
# # =========================================================
# def merge_with_tradingview(ipo_df):
#
#     tv = fetch_tradingview_data()
#
#     df = ipo_df.merge(
#         tv,
#         on="Symbol",
#         how="left"
#     )
#
#     return df
#
#
# # =========================================================
# # 5️⃣ CALCULATIONS + FINAL TABLE
# # =========================================================
# def build_table8(df):
#
#     df["LTP"] = pd.to_numeric(df["LTP"], errors="coerce")
#     df["Listing Price"] = pd.to_numeric(df["Listing Price"], errors="coerce")
#
#     df["ValueTrade(Cr)"] = (
#         pd.to_numeric(df["volume"], errors="coerce") * df["LTP"]
#     ) / 1_00_00_000
#
#     df["After Listing Gain (%)"] = (
#         (df["LTP"] - df["Listing Price"]) / df["Listing Price"] * 100
#     ).round(2).astype(str) + "%"
#
#     return df[
#         [
#             "Symbol",
#             "LTP",
#             "PcntChg",
#             "ValueTrade(Cr)",
#             "Listing Gain (%)",
#             "After Listing Gain (%)"
#         ]
#     ].sort_values("ValueTrade(Cr)", ascending=False)
#
#
# # =========================================================
# # 🚀 RUN EVERYTHING
# # =========================================================
# if __name__ == "__main__":
#
#     SEC_LIST_PATH = r"C:\Users\freedom\Desktop\complet final codes\falcon code\nse_files\PRICE_BAND_DATA\sec_list.csv"
#
#     df_ipo = fetch_listed_only_ipo_df()
#     df_nse = fetch_nse_company_symbol_df()
#     df_sec = read_sec_list_csv(SEC_LIST_PATH)
#
#     ipo_master = smart_bucket_merge(df_ipo, df_nse, df_sec)
#     ipo_tv = merge_with_tradingview(ipo_master)
#     table8 = build_table8(ipo_tv)
#
#     print("\n" + "=" * 120)
#     print(table8.to_string(index=False))
#     print("=" * 120)


import requests
import pandas as pd
import re
import csv
import io
from datetime import datetime, timedelta
from tradingview_screener import Query, Column
import os

# =========================
# TRADINGVIEW
# =========================
def fetch_tradingview_data():
    _, tv = (
        Query()
        .select("name", "exchange", "close", "change", "volume")
        .set_markets("india")
        .where(Column("exchange") == "NSE")
        .limit(9000)
        .get_scanner_data()
    )

    tv = tv.rename(columns={
        "name": "Symbol",
        "close": "LTP",
        "change": "PcntChg"
    })

    tv["Symbol"] = tv["Symbol"].str.upper()
    return tv.drop_duplicates("Symbol")


# =========================
# INVESTORGAIN IPO DATA
# =========================
def fetch_listed_only_ipo_df():
    urls = [
        "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2025/2025-26/0/ipo",
        "https://webnodejs.investorgain.com/cloud/new/report/data-read/394/1/2/2026/2025-26/0/ipo",
    ]

    rows = []
    for url in urls:
        try:
            r = requests.get(url, timeout=10).json()
            rows.extend(r.get("reportTableData", []))
        except:
            pass

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.apply(lambda c: c.map(lambda x: re.sub("<.*?>", "", str(x)).strip()))

    status = df["Status"].str.extract(r"L@([\d.]+).*?\((.*?)\)")
    df["Listing Price"] = pd.to_numeric(status[0], errors="coerce")
    df["Listing Gain (%)"] = status[1]

    return df[["IPO", "Listing Price", "Listing Gain (%)"]].dropna()


# =========================
# NSE SYMBOL MAP
# =========================
def fetch_nse_company_symbol_df():
    today = datetime.now()
    from_date = (today - timedelta(days=365)).strftime("%d-%m-%Y")
    to_date = today.strftime("%d-%m-%Y")

    url = (
        "https://www.nseindia.com/api/public-past-issues"
        f"?from_date={from_date}&to_date={to_date}&security_type=Equity&csv=true"
    )

    session = requests.Session()
    session.get("https://www.nseindia.com")

    csv_text = session.get(url).content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(csv_text))

    rows = []
    for r in reader:
        rows.append({
            "Symbol": r["Symbol"].strip().upper(),
            "COMPANY": r["COMPANY NAME"].strip().upper()
        })

    return pd.DataFrame(rows)


# =========================
# FINAL PUBLIC FUNCTION 🔥
# =========================
def get_ipo_listing_gains():
    ipo = fetch_listed_only_ipo_df()
    nse = fetch_nse_company_symbol_df()
    tv = fetch_tradingview_data()

    ipo["KEY"] = ipo["IPO"].str.split().str[0]
    nse["KEY"] = nse["COMPANY"].str.split().str[0]

    ipo = ipo.merge(nse[["Symbol", "KEY"]], on="KEY", how="left")
    ipo = ipo.dropna(subset=["Symbol"])

    df = ipo.merge(tv, on="Symbol", how="left")

    df["After Listing Gain (%)"] = (
        (df["LTP"] - df["Listing Price"]) / df["Listing Price"] * 100
    ).round(2).astype(str) + "%"

    return df[
        ["Symbol", "Listing Gain (%)", "After Listing Gain (%)"]
    ].drop_duplicates("Symbol")
