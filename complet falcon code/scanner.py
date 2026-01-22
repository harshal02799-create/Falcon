# from tradingview_screener import Query, Column
# import pandas as pd
#
#
# def fetch_tradingview_data():
#     try:
#         n_rows, tradingview = (
#             Query()
#             .select(
#                 'name',
#                 'exchange',
#                 'close',
#                 'change',
#                 'volume',
#                 'sector',
#                 'volume|15',
#                 'volume|30',
#                 'Value.Traded',
#                 'high',
#                 'average_volume_30d_calc',
#                 'price_52_week_high',
#                 'High.All',
#                 'market_cap_basic'
#             )
#             .set_markets('india')
#             .where(Column('exchange') == 'NSE')
#             .limit(9000)
#             .get_scanner_data()
#         )
#
#         if tradingview is None or tradingview.empty:
#             print("❌ No data received from TradingView")
#             return pd.DataFrame()
#
#         tradingview = tradingview.rename(columns={
#             'name': 'Symbol',
#             'sector': 'Sector',
#             'close': 'LTP',
#             'change': 'PcntChg',
#             'average_volume_30d_calc': 'AvgVol30',
#             'volume|15': 'Vol15High'
#         })
#
#         tradingview["Symbol"] = tradingview["Symbol"].str.upper()
#         tradingview = tradingview.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
#
#         return tradingview
#
#     except Exception as e:
#         print("❌ Error fetching TradingView data:", e)
#         return pd.DataFrame()
#
#
# # =========================
# # MAIN EXECUTION
# # =========================
# if __name__ == "__main__":
#
#     df = fetch_tradingview_data()
#
#     if df.empty:
#         print("❌ DataFrame is empty")
#     else:
#         print(f"\n✅ Total rows fetched: {len(df)}")
#         print("\n📌 Columns:")
#         print(df.columns.tolist())
#
#         print("\n📊 SAMPLE DATA (Top 20 by % Change):\n")
#         print(
#             df.sort_values("PcntChg", ascending=False)
#               .head(20)
#               .to_string(index=False)
#         )










import pandas as pd
import json

# File paths
excel_path = r"C:\Users\freedom\Downloads\Company master_02.01.25.xlsx"
json_path = r"C:\Users\freedom\Downloads\Company_master.json"

# Read Excel file (first sheet)
df = pd.read_excel(excel_path)

# Convert to JSON
data = df.to_dict(orient="records")

# Save JSON
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("Excel converted to JSON successfully")
