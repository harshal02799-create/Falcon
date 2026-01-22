from tradingview_screener import Query, Column
import pandas as pd
import time
from datetime import datetime


# ===============================
# FETCH TRADINGVIEW DATA (LIVE)
# ===============================
def fetch_tradingview_data():
    _, df = (
        Query()
        .select(
            "name",
            "close",
            "change",
            "volume",
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
            "volume": "Volume",
        }
    )

    df["Symbol"] = df["Symbol"].str.strip().str.upper()
    df = df.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)

    return df


# ===============================
# CALCULATE MARKET BREADTH
# ===============================
def run_market_breadth():
    df = fetch_tradingview_data()

    if df.empty:
        print("❌ No data received")
        return

    # Value traded in Crores
    df["Value_Cr"] = (df["LTP"] * df["Volume"]) / 1e7

    total_stocks = len(df)

    plus_3 = df[
        (df["PcntChg"] >= 3) &
        (df["Value_Cr"] >= 10)
    ]

    minus_3 = df[
        (df["PcntChg"] <= -3) &
        (df["Value_Cr"] >= 10)
    ]

    plus_count = len(plus_3)
    minus_count = len(minus_3)

    avg_plus_pct = round((plus_count / total_stocks) * 100, 2)
    avg_minus_pct = round((minus_count / total_stocks) * 100, 2)

    result = pd.DataFrame([{
        "Time": datetime.now().strftime("%H:%M:%S"),
        "Total Stocks": total_stocks,
        "+3% Stocks (≥10Cr)": plus_count,
        "-3% Stocks (≥10Cr)": minus_count,
        "Avg +%": avg_plus_pct,
        "Avg -%": avg_minus_pct,
    }])

    print("\n" + "=" * 60)
    print(result.to_string(index=False))
    print("=" * 60)


# ===============================
# RUN EVERY 1 MINUTE (LIVE)
# ===============================
if __name__ == "__main__":
    print("🚀 Live Market Breadth Started (Every 1 Minute)")
    while True:
        try:
            run_market_breadth()
            time.sleep(60)  # 🔥 WAIT 1 MINUTE
        except Exception as e:
            print("⚠️ Error:", e)
            time.sleep(60)












# import pandas as pd
#
# url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
# symbols_df = pd.read_csv(url)
#
# symbols = symbols_df["SYMBOL"].unique().tolist()
# symbols_yf = [s + ".NS" for s in symbols]
#
# print("Total NSE Stocks:", len(symbols_yf))
#
# import os
# import time
# import pandas as pd
# import yfinance as yf
# from tqdm import tqdm
#
# # ---------------- SETTINGS ----------------
# SAVE_PATH = r"C:\Users\freedom\Desktop\nse_ohlcv"
# os.makedirs(SAVE_PATH, exist_ok=True)
#
# FILE_PATH = f"{SAVE_PATH}\\NSE_ALL_OHLCV_2020_TILL_DATE.csv"
# START_DATE = "2020-01-01"
#
# CHUNK_PERCENT = 10  # save every 10%
# # ------------------------------------------
#
# # Load symbols
# url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
# symbols_df = pd.read_csv(url)
# symbols = symbols_df["SYMBOL"].unique().tolist()
# symbols_yf = [s + ".NS" for s in symbols]
#
# total = len(symbols_yf)
# chunk_size = int(total * CHUNK_PERCENT / 100)
#
# print("Total NSE Stocks:", total)
# print("Saving every", chunk_size, "stocks")
#
# buffer = []
# written_once = False
#
# for i, symbol in enumerate(tqdm(symbols_yf), start=1):
#     try:
#         df = yf.download(
#             symbol,
#             start=START_DATE,
#             interval="1d",
#             progress=False,
#             auto_adjust=False
#         )
#
#         if df.empty:
#             continue
#
#         df.reset_index(inplace=True)
#         df["Symbol"] = symbol.replace(".NS", "")
#         df = df[["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]]
#
#         buffer.append(df)
#         time.sleep(0.25)
#
#     except Exception:
#         continue
#
#     # 🔥 SAVE EVERY 10%
#     if i % chunk_size == 0 or i == total:
#         chunk_df = pd.concat(buffer, ignore_index=True)
#
#         chunk_df.to_csv(
#             FILE_PATH,
#             mode="a",
#             header=not written_once,
#             index=False
#         )
#
#         print(f"✅ Saved up to {int((i/total)*100)}%")
#
#         buffer.clear()        # FREE RAM
#         written_once = True
#
# print("🎉 DONE — ALL DATA SAVED SAFELY")
