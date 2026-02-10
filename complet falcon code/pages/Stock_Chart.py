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

st.set_page_config(page_title="Stock Chart", layout="wide")


from chart_utils import show_yfinance_chart

# merged_tv_chart.py
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import json
import time
from datetime import datetime, timedelta
DEFAULT_INDICATOR_COLORS = {
    "SMA10": "#FFD700",
    "SMA20": "#00BFFF",
    "SMA50": "#FF69B4",
    "SMA100": "#FFA500",
    "SMA200": "#ADFF2F",
    "EMA20": "#8e44ad",
    "EMA50": "#9b59b6",
    "VWAP": "#00bcd4",
}

# ----------------- Asset (uploaded image) -----------------
# Provided local file path (the environment will transform this into a URL)
ASSET_PATH = "sandbox:/mnt/data/201f7d2c-1780-48d3-8235-281df27c6e80.png"

# ----------------- Helpers / Indicators -----------------
def sma(series, length):
    return series.rolling(length).mean()

def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def vwap(df):
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    return (tp * df['Volume']).cumsum() / df['Volume'].cumsum()

def rsi(series, length=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(com=(length - 1), adjust=False).mean()
    ma_down = down.ewm(com=(length - 1), adjust=False).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    fast_ema = series.ewm(span=fast, adjust=False).mean()
    slow_ema = series.ewm(span=slow, adjust=False).mean()
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

# ---------- Data fetch ----------
def fetch_ohlcv(symbol: str, period="full", interval="1d"):

    # Always fetch full history using start & end
    start_date = "1900-01-01"
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


    for suffix in [".NS", ".BO", ""]:
        try:
            ticker = yf.Ticker(symbol + suffix)

            df = ticker.history(
                start=start_date,
                end=end_date,
                interval=interval,
                auto_adjust=False,
                back_adjust=False
            )

            if not df.empty:
                df = df[['Open','High','Low','Close','Volume']].copy()
                df.index = pd.to_datetime(df.index)
                df = df.dropna()
                return df

        except Exception as e:
            print("Fetch error:", e)

    # If ALL failed → return empty df
    return pd.DataFrame()

# ---------- Convert dataframe to Lightweight Charts JSON ----------
def df_to_lwjs(df: pd.DataFrame):
    bars = []
    candles = []
    volumes = []

    for ts, row in df.iterrows():

        # Use actual timestamp (VERY IMPORTANT)
        ts = pd.Timestamp(ts).tz_localize(None)
        t = int(ts.timestamp())  # correct for all intervals

        o, h, l, c, v = (
            float(row['Open']),
            float(row['High']),
            float(row['Low']),
            float(row['Close']),
            int(row['Volume'])
        )

        candles.append({"time": t, "open": o, "high": h, "low": l, "close": c})
        bars.append({"time": t, "open": o, "high": h, "low": l, "close": c})
        volumes.append({
            "time": t,
            "value": v,
            "color": "#26a69a" if c >= o else "#ef5350"
        })

    return bars, candles, volumes



# ---------- Lightweight-charts HTML (final) ----------
LW_CHART_HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>LW Chart</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>

<style>
    html,body {
        height: 100%;
        width: 100%;
        margin: 0;
        padding: 0;
        background: #000;
        color: #e6eef6;
        overflow: hidden;
    }
    #mainChart {
        width: 100%;
        height: 100vh;
    }
</style>
</head>

<body>

<div id="mainChart"></div>

<script>
const INIT = window.INIT_DATA || {};
let savedLogicalRange = null;
let userInteracted = false;

// ---------------- MAIN CHART ----------------
const chart = LightweightCharts.createChart(document.getElementById('mainChart'), {
    layout: { backgroundColor: '#000000', textColor: '#e6eef6' },
    grid: { vertLines: { visible: false }, horzLines: { visible: false } },
    timeScale: {
        timeVisible: false,     // hide intraday time
        secondsVisible: false
    }

});

// 🔒 Prevent auto-scroll / right shift on refresh
chart.timeScale().applyOptions({
    rightBarStaysOnScroll: false,
    fixRightEdge: false
});

// -------- PRICE SERIES (candles or bars) --------
let mainSeries;
function createMainSeries(type) {
    try { if (mainSeries) chart.removeSeries(mainSeries); } catch(e){}

    if (type === "Bars") {
        mainSeries = chart.addBarSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            thinBars: false
        });
    } else {
        mainSeries = chart.addCandlestickSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            wickUpColor: '#26a69a',
            wickDownColor: '#ef5350'
        });
    }
}


// -------- VOLUME INSIDE SAME CHART --------
const volumeSeries = chart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    priceScaleId: 'volume',
    scaleMargins: { top: 0.75, bottom: 0 },
    lastValueVisible: false,
    priceLineVisible: false
});

// adjust price scale height for candles
chart.applyOptions({
    rightPriceScale: {
        scaleMargins: {
            top: 0.05,
            bottom: 0.25
        }
    }
});


// -------- LOAD INITIAL DATA --------
function loadAll() {
    createMainSeries(INIT.chart_type || "Candles");

    if (INIT.candles) mainSeries.setData(INIT.candles);
    if (INIT.volumes && INIT.show_volume) volumeSeries.setData(INIT.volumes);

    // overlays
    if (Array.isArray(INIT.overlays)) {
        INIT.overlays.forEach(o => {
            const s = chart.addLineSeries({
                color: o.color || '#f1c40f',
                lineWidth: 2,
                priceLineVisible: false,
                lastValueVisible: false
            });
            s.setData(o.data || []);
        });
    }
}

loadAll();
// Track if user moved chart

// -------- UPDATE FUNCTION (fixed: don't move chart on refresh) --------
window.updateData = function (payload) {
    try {
        const lastCandle = payload.candles?.[payload.candles.length - 1];
        const lastVolume = payload.volumes?.[payload.volumes.length - 1];

        if (mainSeries && lastCandle) {
            mainSeries.update(lastCandle);
        }

        if (volumeSeries && lastVolume) {
            volumeSeries.update(lastVolume);
        }

        // 🔒 Restore user view ONLY if user moved chart
        if (userInteracted && savedLogicalRange) {
            chart.timeScale().setVisibleLogicalRange(savedLogicalRange);
        }

    } catch (e) {
        console.error("updateData error", e);
    }
};


// Reset behavior when user clicks timescale right edge
chart.timeScale().subscribeClick(() => {
    userInteracted = false;
});



// -------- RESIZE HANDLER --------
window.addEventListener("resize", () => {
    chart.applyOptions({
        width: window.innerWidth,
        height: window.innerHeight
    });
});
</script>

</body>
</html>
"""

st.set_page_config(layout="wide", page_title="Chart")

# ---- REMOVE SCROLLBARS FROM IFRAME ----
st.markdown("""
<style>
iframe {
    overflow: hidden !important;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
/* Remove all Streamlit padding */
.main > div {
    padding-top: 0rem !important;
    padding-left: 0rem !important;
    padding-right: 0rem !important;
}

/* Center the chart component */
.block-container {
    padding: 0rem !important;
    margin-left: auto !important;
    margin-right: auto !important;
}

/* Force iframe (chart) to center */
iframe {
    display: block;
    margin-left: auto !important;
    margin-right: auto !important;
}
</style>
""", unsafe_allow_html=True)

# Simple header with uploaded image (optional)
col_a, col_b = st.columns([0.08, 0.92])
with col_a:
    try:
        st.image(ASSET_PATH, width=64)  # will work in the environment that maps sandbox: paths
    except Exception:
        pass
# with col_b:
#     st.markdown("<h2 style='color:#00BFFF; font-family:Cambria; margin:4px 0'>TradingView-style Chart</h2>", unsafe_allow_html=True)

# Persistent state for a couple UI fields
if "selected_symbol" not in st.session_state:
    st.session_state.selected_symbol = ""

if "portfolio" not in st.session_state:
    st.session_state.portfolio = ["-- Select from Portfolio --"]

# ----------------- LOAD WATCHLIST CSV -----------------
# --------------------- SIDEBAR UPLOAD ---------------------
with st.sidebar:
    st.header("📂 Portfolio / Watchlist")

    uploaded_file = st.file_uploader(
        "Upload Portfolio CSV",
        type=["csv"],
        key="portfolio_upload"
    )

    if uploaded_file is not None:
        # Read CSV safely even if there is no header
        df_csv = pd.read_csv(uploaded_file, header=None)
        df_csv.columns = ["symbol"]

        st.session_state.portfolio = ["-- Select from Portfolio --"] + \
                                     df_csv["symbol"].dropna().astype(str).str.upper().tolist()

    st.write("Total Symbols:", len(st.session_state.portfolio) - 1)


# --- ONE CLEAN ROW FOR ALL CONTROLS ---
row = st.columns([3, 3, 1, 1, 1])   # adjust widths as needed

show_volume = True      # Always ON but hidden from UI
auto_refresh = True     # Always ON but hidden from UI

# -------- COLUMN 1 (WATCHLIST) --------
with row[0]:
    watchlist_symbol = st.selectbox(
        "Select from Portfolio",
        st.session_state.portfolio,
        index=0,
        key="watchlist_select"
    )

# -------- COLUMN 2 (MANUAL SEARCH) --------
with row[1]:
    manual_symbol = st.text_input(
        "Manual Search (Type Symbol)",
        value=st.session_state.get("selected_symbol", ""),
        key="manual_input"   # <-- FIXED: unique key
    ).strip().upper()

# -------- COLUMN 3 (PERIOD) --------
with row[2]:
    period = st.selectbox(
        "Chart Period",
        ["Full", "1mo", "3mo", "6mo", "1y", "2y", "5y"],
        index=0  # default = Full
    )

# -------- COLUMN 4 (INTERVAL) --------
with row[3]:
    raw_interval = st.selectbox(
        "Candle Interval",
        ["1m","3m","5m","15m","30m","60m","1d","1wk","1mo"],
        index=6
    )

# -------- COLUMN 5 (TYPE) --------
with row[4]:
    chart_type = st.selectbox(
        "Chart Type",
        ["Candlestick","Bars"],
        index=1
    )

# ------- FINAL SYMBOL DECISION -------
if manual_symbol:
    symbol = manual_symbol
elif watchlist_symbol != "-- Select from Portfolio --":
    symbol = watchlist_symbol
else:
    symbol = ""

st.session_state.selected_symbol = symbol

with st.expander("⚙️  Chart Settings (Indicators)", expanded=False):
    col1, col2, col3 = st.columns(3)

    # -------- INDICATOR TOGGLES --------
    with col1:
        sma10 = st.toggle("SMA 10", value=False) if hasattr(st, "toggle") else st.checkbox("SMA 10", False)
        sma20 = st.checkbox("SMA 20", False)
        sma50 = st.checkbox("SMA 50", False)

    with col2:
        sma100 = st.checkbox("SMA 100", False)
        sma200 = st.checkbox("SMA 200", False)
        show_vwap = st.checkbox("VWAP", False)

    with col3:
        show_rsi = st.checkbox("RSI (14)", False)
        show_macd = st.checkbox("MACD (12,26,9)", False)
        show_ema = st.checkbox("EMA (20/50)", False)

    # -------- COLOR PICKERS (ADD HERE) --------
    st.markdown("### 🎨 Indicator Colors")

    DEFAULT_INDICATOR_COLORS = {
        "SMA10": "#FFD700",
        "SMA20": "#00BFFF",
        "SMA50": "#FF69B4",
        "SMA100": "#FFA500",
        "SMA200": "#ADFF2F",
        "EMA20": "#8e44ad",
        "EMA50": "#9b59b6",
        "VWAP": "#00bcd4",
    }

    color_cols = st.columns(3)
    user_colors = {}

    i = 0
    for name, default in DEFAULT_INDICATOR_COLORS.items():
        with color_cols[i % 3]:
            user_colors[name] = st.color_picker(
                label=name,
                value=default,
                key=f"color_{name}"
            )
        i += 1

# symbol guard
if not st.session_state.selected_symbol:
    st.info("🔍 Please select or enter a symbol to view its chart.")
    st.stop()

symbol = st.session_state.selected_symbol

# Fetch data
df = fetch_ohlcv(symbol, period=period, interval=raw_interval)
if df.empty:
    st.warning("No data returned; check symbol or try different interval/period.")
    st.stop()

# indicators: compute requested
if sma10:
    df['SMA10'] = sma(df['Close'], 10)
if sma20:
    df['SMA20'] = sma(df['Close'], 20)
if sma50:
    df['SMA50'] = sma(df['Close'], 50)
if sma100:
    df['SMA100'] = sma(df['Close'], 100)
if sma200:
    df['SMA200'] = sma(df['Close'], 200)
if show_ema:
    df['EMA20'] = ema(df['Close'], 20)
    df['EMA50'] = ema(df['Close'], 50)
if show_vwap:
    try:
        df['VWAP'] = vwap(df)
    except Exception:
        pass
if show_rsi:
    df['RSI'] = rsi(df['Close'], 14)
if show_macd:
    macd_line, signal, hist = macd(df['Close'])
    df['MACD'] = macd_line
    df['MACD_SIGNAL'] = signal
    df['MACD_HIST'] = hist

# build overlays list for main chart (SMA/EMA/VWAP)
overlays = []
for k,color in [
    ("SMA10","#FFD700"), ("SMA20","#00BFFF"), ("SMA50","#FF69B4"),
    ("SMA100","#FFA500"), ("SMA200","#ADFF2F"), ("EMA20","#8e44ad"), ("EMA50","#9b59b6"),
    ("VWAP","#00bcd4")
]:
    if k in df.columns:
        arr = []
        for idx,val in df[k].dropna().items():
            ts = pd.Timestamp(idx).replace(tzinfo=None)
            arr.append({"time": int(ts.timestamp()), "value": float(val)})

        overlays.append({"id": k, "color": color, "data": arr})

# indicators payload for lower panes
# indicators payload for lower panes
indicators_payload = {}

# ---- FIXED RSI ----
if show_rsi and "RSI" in df.columns:
    indicators_payload['rsi'] = {}
    for idx, v in df['RSI'].dropna().items():
        ts = pd.Timestamp(idx).replace(tzinfo=None)   # ensure naive timestamp
        indicators_payload['rsi'][str(int(ts.timestamp()))] = float(v)

# ---- FIXED MACD ----
if show_macd and "MACD" in df.columns:
    indicators_payload['macd'] = {"macd": {}, "signal": {}, "hist": {}}

    for idx, v in df['MACD'].dropna().items():
        ts = pd.Timestamp(idx).replace(tzinfo=None)
        indicators_payload['macd']['macd'][str(int(ts.timestamp()))] = float(v)

    for idx, v in df['MACD_SIGNAL'].dropna().items():
        ts = pd.Timestamp(idx).replace(tzinfo=None)
        indicators_payload['macd']['signal'][str(int(ts.timestamp()))] = float(v)

    for idx, v in df['MACD_HIST'].dropna().items():
        ts = pd.Timestamp(idx).replace(tzinfo=None)
        indicators_payload['macd']['hist'][str(int(ts.timestamp()))] = float(v)

# convert ohlcv
bars, candles, volumes = df_to_lwjs(df)
# payload for JS
payload = {
    "symbol": symbol,
    "interval": raw_interval,
    "candles": candles,
    "bars": bars,
    "volumes": volumes,
    "overlays": overlays,
    "indicators": indicators_payload,
    "chart_type": "Bars" if chart_type == "Bars" else "Candles",
    "show_volume": bool(show_volume),
    "show_rsi": bool(show_rsi),
    "show_macd": bool(show_macd),
    "title": f"{symbol} - {chart_type} ({period}, {raw_interval})"
}

# inject JS
injected = "<script>window.INIT_DATA = " + json.dumps(payload) + ";</script>\n"
html = injected + LW_CHART_HTML

st.components.v1.html(html, height=850, scrolling=False)
# -------- KEYBOARD ARROW CONTROL FOR WATCHLIST -------------
symbols_js_list = st.session_state.portfolio

js_code = f"""
<script>
document.addEventListener('keydown', function(e) {{
    let list = {symbols_js_list};
    let current = "{symbol}";

    if (e.key === "ArrowDown") {{
        let idx = list.indexOf(current);
        if (idx >= 0 && idx < list.length - 1) {{
            let next = list[idx + 1];
            window.parent.postMessage({{type: 'updateSymbol', value: next}}, "*");
        }}
    }}

    if (e.key === "ArrowUp") {{
        let idx = list.indexOf(current);
        if (idx > 1) {{
            let prev = list[idx - 1];
            window.parent.postMessage({{type: 'updateSymbol', value: prev}}, "*");
        }}
    }}
}});
</script>
"""

st.components.v1.html(js_code, height=0)

# Receiver for symbol update
receiver = """
<script>
window.addEventListener("message", (event) => {
    if (event.data.type === "updateSymbol") {
        const newSym = event.data.value;
        const input = window.parent.document.querySelector('input[id="manual_symbol"]');
        if (input) {
            input.value = newSym;
            input.dispatchEvent(new Event('input', { bubbles: true }));
        }
    }
});
</script>
"""
st.components.v1.html(receiver, height=0)

# Auto refresh (simple rerun) - optional
if auto_refresh:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=10 * 1000, limit=None)  # refresh every 10s
    except Exception:
        if st.button("Refresh now"):
            st.experimental_rerun()






















# st.title("📈 Stock Chart Analysis")

# import yfinance as yf
#
# col1, col2, col3, col4 = st.columns([3, 1.2, 1.2, 1.5])
#
# with col1:
#     raw_symbol = st.text_input("Enter Symbol").strip().upper()
#
# with col2:
#     period = st.selectbox("Period",
#         ["1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","max"], index=4)
#
# with col3:
#     interval = st.selectbox("Interval",
#         ["1m","5m","15m","30m","1h","1d","1wk","1mo"], index=5)
#
# with col4:
#     chart_type = st.selectbox("Chart Type", ["Candlestick","Bar"])
#
# # Indicators
# i1,i2,i3,i4,i5,i6,i7 = st.columns(7)
# sma10 = i1.checkbox("SMA10")
# sma20 = i2.checkbox("SMA20")
# sma50 = i3.checkbox("SMA50")
# sma100 = i4.checkbox("SMA100")
# sma200 = i5.checkbox("SMA200")
# rsi = i6.checkbox("RSI")
# macd = i7.checkbox("MACD")
#
# if raw_symbol == "":
#     st.info("Enter a stock symbol to show chart.")
#
# else:
#     show_yfinance_chart(
#         symbol=raw_symbol,
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

