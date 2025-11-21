import streamlit as st
# your other imports...


# import streamlit as st
# from chart_utils import show_yfinance_chart

# # =========================================
# # PAGE CONFIG
# # =========================================
# st.set_page_config(page_title="📈 Stock Chart", layout="wide")

# st.title("📈 Stock Chart Analysis")

# # =========================================
# # TOP INPUT BAR – All in one line
# # =========================================
# col1, col2, col3, col4 = st.columns([3, 1.2, 1.2, 1.5])

# with col1:
#     symbol = st.text_input("Enter Symbol", value="CPPLUS").upper()
#     raw_symbol = st.text_input("Enter Symbol", value="CPPLUS").upper()

#     # Auto-append .NS for Indian stocks
#     if raw_symbol and not raw_symbol.endswith(".NS"):
#         symbol = raw_symbol + ".NS"
#     else:
#         symbol = raw_symbol

# with col2:
#     period = st.selectbox(
#         "Period",
#         ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "max"],
#         index=4
#     )

# with col3:
#     interval = st.selectbox(
#         "Interval",
#         ["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"],
#         index=5
#     )

# with col4:
#     chart_type = st.selectbox("Chart Type", ["Candlestick", "Bar"], index=0)


# # =========================================
# # INDICATORS TOGGLES – All in one line
# # =========================================
# i1, i2, i3, i4, i5, i6, i7 = st.columns(7)

# with i1:
#     sma10 = st.checkbox("SMA10")
# with i2:
#     sma20 = st.checkbox("SMA20")
# with i3:
#     sma50 = st.checkbox("SMA50")
# with i4:
#     sma100 = st.checkbox("SMA100")
# with i5:
#     sma200 = st.checkbox("SMA200")
# with i6:
#     rsi = st.checkbox("RSI")
# with i7:
#     macd = st.checkbox("MACD")


# # =========================================
# # AUTO UPDATE CHART (NO BUTTON)
# # Rerun when any input changes
# # =========================================

# if symbol:
#     show_yfinance_chart(
#         symbol=symbol,
#         period=period,
#         interval=interval,
#         chart_type=chart_type,
#         show_volume=True,
#         show_sma10=sma10,
#         show_sma20=sma20,
#         show_sma50=sma50,
#         show_sma100=sma100,
#         show_sma200=sma200,
#         show_rsi=rsi,
#         show_macd=macd
#     )
# else:
#     st.info("Enter a symbol to load chart.")


import streamlit as st
from nav import nav_menu

nav_menu()

# st.title("📈 Stock Chart")



import streamlit as st
from chart_utils import show_yfinance_chart

st.set_page_config(page_title="📈 Stock Chart", layout="wide")
# st.title("📈 Stock Chart Analysis")

import yfinance as yf

col1, col2, col3, col4 = st.columns([3, 1.2, 1.2, 1.5])

with col1:
    raw_symbol = st.text_input("Enter Symbol").strip().upper()

with col2:
    period = st.selectbox("Period",
        ["1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","max"], index=4)

with col3:
    interval = st.selectbox("Interval",
        ["1m","5m","15m","30m","1h","1d","1wk","1mo"], index=5)

with col4:
    chart_type = st.selectbox("Chart Type", ["Candlestick","Bar"])

# Indicators
i1,i2,i3,i4,i5,i6,i7 = st.columns(7)
sma10 = i1.checkbox("SMA10")
sma20 = i2.checkbox("SMA20")
sma50 = i3.checkbox("SMA50")
sma100 = i4.checkbox("SMA100")
sma200 = i5.checkbox("SMA200")
rsi = i6.checkbox("RSI")
macd = i7.checkbox("MACD")

if raw_symbol == "":
    st.info("Enter a stock symbol to show chart.")

else:
    show_yfinance_chart(
        symbol=raw_symbol,
        period=period,
        interval=interval,
        chart_type=chart_type,
        show_volume=True,
        show_sma10=sma10,
        show_sma20=sma20,
        show_sma50=sma50,
        show_sma100=sma100,
        show_sma200=sma200,
        show_rsi=rsi,
        show_macd=macd
    )











