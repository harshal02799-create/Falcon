import altair as alt
import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.graph_objects as go
from datetime import datetime, timedelta, time
from tradingview_screener import Query, Column
from data1 import UpstoxNSEDownloader
import asyncio
import streamlit.components.v1 as components


# 1. Initialize at the very top of your main loop/function
live_df = pd.DataFrame()
hist_tail = pd.DataFrame()

try:
    # 2. Your logic to populate the data
    # If this fails, the variables remain as empty DataFrames
    live_df = load_live_data()
    hist_tail = load_history_data()

except Exception as e:
    print(f"Data load failed: {e}")

# 3. This check will now work without crashing
if not live_df.empty and not hist_tail.empty:
    # Perform your merging/MTM logic
    pass
# ---------- CONFIG & PATHS ----------
st.set_page_config(page_title="Money Flow Index", layout="wide")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "nse_analytics_clean.db")


# ---------- SQLITE HELPERS ----------
# def sqlite_connect():
#     return sqlite3.connect(DB_PATH, timeout=60, isolation_level=None)
def sqlite_connect():
    # 'timeout=20' tells SQLite to wait 20 seconds if the DB is busy
    conn = sqlite3.connect(DB_PATH, timeout=20)

    # Enable WAL mode (Write-Ahead Logging)
    # This is the industry standard for preventing "Database is locked" errors
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    return conn

def get_last_date():
    conn = sqlite_connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(date) FROM ohlc_enriched")
        last_date = cur.fetchone()[0]
    except Exception:
        last_date = None
    finally:
        conn.close()
    return last_date


def merge_ohlc_into_enriched():
    conn = sqlite_connect()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO ohlc_enriched (symbol, date, open, high, low, close, volume, avg_price, turnover, change_pct)
    SELECT o.symbol, o.date, o.open, o.high, o.low, o.close, o.volume,
        ROUND((o.open + o.high + o.low + o.close) / 4.0, 2),
        ROUND(((o.open + o.high + o.low + o.close) / 4.0) * o.volume / 1e7, 2),
        ROUND((o.close - p.close) * 100.0 / p.close, 2)
    FROM ohlc o
    JOIN ohlc_enriched p ON o.symbol = p.symbol 
    WHERE p.date = (SELECT MAX(date) FROM ohlc_enriched WHERE symbol = o.symbol)
    AND NOT EXISTS (SELECT 1 FROM ohlc_enriched e WHERE e.symbol = o.symbol AND e.date = o.date);
    """)
    conn.close()


# ---------- UPDATE LOGIC ----------
async def run_daily_update():
    last_date = get_last_date()
    if not last_date: return
    start_date = (datetime.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date > end_date: return

    downloader = UpstoxNSEDownloader(db_path=DB_PATH, max_per_sec=5, max_per_min=500, concurrency=2,
                                     db_batch_size=10000)
    downloader.start_date, downloader.end_date = start_date, end_date
    await downloader.run()
    merge_ohlc_into_enriched()


def trigger_db_update():
    try:
        asyncio.run(run_daily_update())
        st.sidebar.success("Database Updated Successfully!")
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Update Failed: {e}")


# ---------- DATA FETCHING ----------
def fetch_tradingview_data():
    try:
        _, df = Query().select("name", "close", "change", "volume").set_markets("india").where(
            Column("exchange") == "NSE").limit(9000).get_scanner_data()
        df = df.rename(columns={"name": "Symbol", "close": "LTP", "change": "PcntChg", "volume": "Volume"})
        df["Symbol"] = df["Symbol"].str.strip().str.upper()
        return df.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()
def init_live_db():
    conn = sqlite_connect()

    # 🔴 DROP OLD TABLE (OLD DATA REMOVED)
    conn.execute("DROP TABLE IF EXISTS live_pulse_history")

    # ✅ CREATE NEW LIVE TABLE (FINAL SCHEMA)
    conn.execute("""
        CREATE TABLE live_pulse_history (
            timestamp DATETIME PRIMARY KEY,
            date_ref TEXT,
            total_stocks INTEGER,
            gainers INTEGER,
            losers INTEGER,
            strength REAL,
            weakness REAL,
            spread REAL,
            gl_ratio REAL
        )
    """)

    conn.commit()
    conn.close()

@st.cache_data(ttl=300)
def load_breadth_data(start_date, turnover_min, pct_threshold):
    if not os.path.exists(DB_PATH): return pd.DataFrame()
    conn = sqlite_connect()
    df = pd.read_sql_query("""
        SELECT date, COUNT(*) AS total,
        SUM(CASE WHEN change_pct >= ? AND turnover >= ? THEN 1 ELSE 0 END) AS gainers,
        SUM(CASE WHEN change_pct <= -? AND turnover >= ? THEN 1 ELSE 0 END) AS losers
        FROM ohlc_enriched WHERE date >= ? GROUP BY date ORDER BY date ASC
    """, conn, params=(pct_threshold, turnover_min, pct_threshold, turnover_min, start_date))
    conn.close()
    return df

def patch_database():
    conn = sqlite3.connect(DB_PATH)
    try:
        # Check if the column exists; if not, add it
        conn.execute("ALTER TABLE live_pulse_history ADD COLUMN total_stocks INTEGER DEFAULT 0")
        print("Successfully patched database with 'total_stocks' column.")
    except sqlite3.OperationalError:
        # Column already exists, do nothing
        pass
    finally:
        conn.close()

# Run the patch
patch_database()
def fix_database_schema():
    conn = sqlite3.connect(DB_PATH)
    try:
        # This command adds the missing column if it doesn't exist
        conn.execute("ALTER TABLE live_pulse_history ADD COLUMN weakness REAL DEFAULT 0")
        st.success("✅ Database patched: 'weakness' column added.")
    except sqlite3.OperationalError:
        # If the column already exists, SQLite throws an error we can safely ignore
        pass
    finally:
        conn.close()

# Run the fix
fix_database_schema()
from datetime import datetime, time

@st.fragment(run_every=60)
def display_live_pulse():
    now_dt = datetime.now()
    today_date = now_dt.strftime("%Y-%m-%d")

    # 1. Open connection safely
    conn = sqlite_connect()
    try:
        # Create Table (Ensuring 9 columns exist)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_pulse_history (
                timestamp DATETIME PRIMARY KEY,
                date_ref TEXT,
                total_stocks INTEGER,
                gainers INTEGER,
                losers INTEGER,
                strength REAL,
                weakness REAL,
                spread REAL,
                gl_ratio REAL
            )
        """)
        conn.commit()

        # 2. Data Collection (only during market hours)
        market_open, market_close = time(9, 15), time(15, 30)
        if market_open <= now_dt.time() <= market_close:
            live_df = fetch_tradingview_data()
            if not live_df.empty:
                total_live = len(live_df)
                live_df["Value_Cr"] = (live_df["LTP"] * live_df["Volume"]) / 1e7
                g_count = len(live_df[(live_df["PcntChg"] >= 3) & (live_df["Value_Cr"] >= 10)])
                l_count = len(live_df[(live_df["PcntChg"] <= -3) & (live_df["Value_Cr"] >= 10)])

                s_val = round((g_count / total_live) * 100, 2)
                w_val = round((l_count / total_live) * 100, 2)
                sp_val = round(s_val - w_val, 2)
                r_val = round(g_count / (l_count + 0.001), 2)

                conn.execute("""
                    INSERT OR REPLACE INTO live_pulse_history 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (now_dt.strftime("%Y-%m-%d %H:%M:%S"), today_date, total_live,
                      g_count, l_count, s_val, w_val, sp_val, r_val))
                conn.commit()

        # 3. Load Data for UI
        live_hist = pd.read_sql_query("SELECT * FROM live_pulse_history ORDER BY timestamp ASC", conn)

    finally:
        # ALWAYS close the connection to prevent "Database is locked"
        conn.close()

    if not live_hist.empty:
        live_hist['timestamp'] = pd.to_datetime(live_hist['timestamp'])

        # --- UI VIEW SELECTION ---
        st.subheader(f"📡 Live Money Flow ({now_dt.strftime('%H:%M:%S')})")
        view_mode = st.radio("View", ["⚡ Strength", "📋 Table", "🌊 Net Spread", "📈 Ratio"], horizontal=True)
        # st.divider()

        # ================= 4. METRICS MATRIX (NEW) =================
        l1, l2, l3, l4, l5 = st.columns(5)

        # Check if we have live data from the current session or need to pull from history
        # We check 's_val' which is defined in your Data Collection block
        if 's_val' in locals():
            m_strength, m_weakness, m_spread, m_ratio = s_val, w_val, sp_val, r_val
        elif not live_hist.empty:
            last = live_hist.iloc[-1]
            m_strength = last.get("strength")
            m_weakness = last.get("weakness")
            m_spread = last.get("spread")
            m_ratio = last.get("gl_ratio")
        else:
            m_strength = m_weakness = m_spread = m_ratio = None

        # --- Market Zone Calculation ---
        if m_spread is not None:
            if m_spread >= 10:
                market_zone, zone_desc = "📈 OVERBOUGHT", "Extreme Strength"
            elif 7 <= m_spread < 10:
                market_zone, zone_desc = "🚀 BULLISH", "Strong Participation"
            elif 5 <= m_spread < 7:
                market_zone, zone_desc = "⚖️ Neutral", "Neutral / Range"
            elif 2 <= m_spread < 5:
                market_zone, zone_desc = "🔴 BEARISH", "Weakness / Selling"
            else:
                market_zone, zone_desc = "🛡️ OVERSOLD", "Extreme Weakness"
        else:
            market_zone, zone_desc = "—", ""

        # --- Render Metrics ---
        l1.metric("Strength %", f"{m_strength:.2f}%" if m_strength is not None else "—")
        l2.metric("Weakness %", f"{m_weakness:.2f}%" if m_weakness is not None else "—", delta_color="inverse")
        l3.metric("Net Spread", f"{m_spread:.2f}" if m_spread is not None else "—")
        l4.metric("G/L Ratio", f"{m_ratio:.2f}" if m_ratio is not None else "—")
        l5.metric("Market Zone", market_zone, zone_desc)
        #
        # st.write("---")  # Second separator before the chart selection
        #


        if view_mode == "📋 Table":
            # --- Improved Table UI ---
            if view_mode == "📋 Table":
                custom_css = """
                <style>
                    .blue-table-container { 
                        font-family: 'Inter', 'Segoe UI', sans-serif; 
                        width: 100%; color: #e0e0e0; background-color: #0e1117;
                        border-radius: 8px; overflow: hidden; border: 1px solid #1e2130;
                    }
                    .blue-table { width: 100%; border-collapse: collapse; font-size: 14px; }
                    .blue-table th { 
                        background-color: #161b22; color: #58a6ff; padding: 14px; 
                        text-align: center; border-bottom: 2px solid #2e5bff;
                        position: sticky; top: 0; z-index: 10;
                    }
                    .blue-table td { padding: 12px; text-align: center; border-bottom: 1px solid #161b22; }
                    .blue-table tr:hover { background-color: rgba(46, 91, 255, 0.12); transition: 0.2s; }

                    /* Value Colors */
                    .pos-val { color: #39d353; font-weight: 600; }
                    .neg-val { color: #f85149; font-weight: 600; }
                    .neutral-val { color: #f2cc60; font-weight: 600; }
                    .ts-col { color: #8b949e; font-family: monospace; }
                </style>
                """

                rows_html = ""
                live_display = live_hist.sort_values("timestamp", ascending=False)

                for _, row in live_display.iterrows():
                    # Better timestamp formatting
                    ts = pd.to_datetime(row['timestamp']).strftime('%H:%M:%S')
                    r_val, s_val = row["gl_ratio"], row["spread"]

                    # Dynamic Classes
                    ratio_class = 'class="pos-val"' if r_val > 1.5 else 'class="neg-val"' if r_val < 0.7 else 'class="neutral-val"'
                    spread_class = 'class="pos-val"' if s_val > 5 else 'class="neg-val"' if s_val < -5 else 'class="neutral-val"'

                    rows_html += f"""
                        <tr>
                            <td class="ts-col">{ts}</td>
                            <td>{row['strength']:.2f}%</td>
                            <td>{row['weakness']:.2f}%</td>
                            <td {spread_class}>{s_val:.2f}</td>
                            <td {ratio_class}>{r_val:.2f}</td>
                        </tr>"""

                table_html = f"""
                    <div class="blue-table-container">
                        <table class="blue-table">
                            <thead>
                                <tr>
                                    <th>Time</th><th>Strength %</th><th>Weakness %</th>
                                    <th>Net Spread</th><th>G/L Ratio</th>
                                </tr>
                            </thead>
                            <tbody>{rows_html}</tbody>
                        </table>
                    </div>"""

                components.html(custom_css + table_html, height=500, scrolling=True)
        else:
            # 1. Prepare Data
            live_hist['timestamp'] = pd.to_datetime(live_hist['timestamp'])
            # --- YOUR ORIGINAL CODE (UNTOUCHED) ---
            if view_mode == "⚡ Strength":
                target, label, line_color = "strength", "STRENGTH %", "#2E5BFF"
            elif view_mode == "🌊 Net Spread":
                target, label, line_color = "spread", "NET SPREAD", "#00FF88"
            else:
                target, label, line_color = "gl_ratio", "G/L RATIO", "#FFD700"

            chart = alt.Chart(live_hist).mark_line(
                point=False,
                color=line_color,
                strokeWidth=3,
                interpolate='monotone'
            ).encode(
                x=alt.X("timestamp:T", title="Time", axis=alt.Axis(grid=False)),
                y=alt.Y(f"{target}:Q", title=label, scale=alt.Scale(zero=False), axis=alt.Axis(grid=False)),
            ).properties(
                height=400
            ).interactive()

            # --- ADDING VISIBILITY LAYERS WITH HOVER DATA ---
            # 1. Create the hover selection
            hover_selection = alt.selection_point(
                fields=['timestamp'], nearest=True, on='mouseover', empty=False
            )

            # 2. Add vertical line WITH Tooltips (Added format for cleaner look)
            hover_rule = alt.Chart(live_hist).mark_rule(color='white', strokeWidth=1, opacity=0.3).encode(
                x='timestamp:T',
                opacity=alt.condition(hover_selection, alt.value(0.5), alt.value(0)),
                tooltip=[
                    alt.Tooltip("timestamp:T", title="Time", format="%H:%M:%S"),
                    alt.Tooltip(f"{target}:Q", title=label, format=".2f")
                ]
            ).add_params(hover_selection)

            # 3. Add a snapping dot to the line
            hover_dot = alt.Chart(live_hist).mark_circle(size=100, color=line_color).encode(
                x='timestamp:T',
                y=f"{target}:Q",
                opacity=alt.condition(hover_selection, alt.value(1), alt.value(0))
            )

            # 4. Final Display
            st.altair_chart(chart + hover_rule + hover_dot, use_container_width=True)
    else:
        st.info("Market data accumulation will begin at 09:15 AM today.")



    # ---------- MAIN DASHBOARD ----------
def main():
    # 1. SIDEBAR
    st.sidebar.header("📥 Data Management")
    if st.sidebar.button("🔄 Sync Latest NSE Data", use_container_width=True):
        with st.sidebar.status("Syncing...", expanded=True):
            trigger_db_update()

    st.sidebar.divider()
    st.sidebar.header("Chart Settings")
    view_start = st.sidebar.date_input("View History From:", value=pd.to_datetime("2020-01-01").date())
    sma_window = st.sidebar.slider("SMA Window", 5, 50, 10)

    # 2. LIVE SECTION (Fragmented)
    # st.title("🚀 Market Breadth")
    display_live_pulse()

    # 3. HISTORICAL SECTION
    raw_df = load_breadth_data("2020-01-01", 10.0, 3.0)
    if raw_df.empty:
        st.warning("Database empty. Please Sync Data.")
        return

    raw_df["date"] = pd.to_datetime(raw_df["date"])
    df = raw_df[raw_df["date"] >= pd.to_datetime(view_start)].copy()

    # -----------------------------
    # SIMPLE DAILY CALCULATIONS
    # (USING SAME COLUMN NAMES)
    # -----------------------------

    df["stock_up_3"] = df["gainers"].fillna(0)
    df["stock_down_3"] = df["losers"].fillna(0)

    # G/L Ratio = LOSERS / GAINERS
    df["ratio"] = (df["stock_up_3"] / (df["stock_down_3"] + 0.001)).round(2)

    # Strength % = (Gainers / Total) * 100
    df["+3%_Average"] = ((df["stock_up_3"] / df["total"]) * 100).round(2)

    # Weakness % = (Losers / Total) * 100
    df["-3%_Average"] = ((df["stock_down_3"] / df["total"]) * 100).round(2)

    # Net Spread = Strength - Weakness
    df["Spread"] = (df["+3%_Average"] - df["-3%_Average"]).round(2)

    st.subheader(" ₹ Money Flow Index")
    latest, prev = df.iloc[-1], df.iloc[-2]

    # --- Enhanced Sentiment Logic ---
    val = latest['Spread']

    if val >= 10:
        sentiment = "📉 OVERBOUGHT"
        s_delta = "Extreme Strength"
    elif 7 <= val < 10:
        sentiment = "🚀 BULLISH"
        s_delta = "Strong Participation"
    elif 5 <= val < 7:
        sentiment = "⚖️ Neutral"
        s_delta = "Neutral / Range"
    elif 2 <= val < 5:
        sentiment = "🔴 BEARISH"
        s_delta = "Weakness / Selling"
    else:  # val < 2
        sentiment = "🛡️ OVERSOLD"
        s_delta = "Extreme Weakness"

    # --- Metrics Display ---
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Strength %", f"{latest['+3%_Average']}%", f"{round(latest['+3%_Average'] - prev['+3%_Average'], 2)}%")

    c2.metric("Weakness %", f"{latest['-3%_Average']}%", f"{round(latest['-3%_Average'] - prev['-3%_Average'], 2)}%",
              delta_color="inverse")

    c3.metric("Net Spread", f"{latest['Spread']}", f"{round(latest['Spread'] - prev['Spread'], 2)}")

    c4.metric("G/L Ratio", f"{latest['ratio']}", f"{round(latest['ratio'] - prev['ratio'], 2)}")

    # Updated Sentiment Metric
    # We use s_delta as the 'delta' label to provide context
    c5.metric("Market Zone", sentiment, s_delta, delta_color="off")

    # ----------------------------
    # 4. TOGGLED MARKET BREADTH CHARTS (SCROLLABLE)
    # ----------------------------
    st.subheader("📈 Breadth Analysis")

    # Selection for the scroll/zoom behavior
    zoom_selection = alt.selection_interval(
        bind='scales',
        encodings=['x']
    )

    chart_selection = st.pills(
        "Select View:",
        options=["Positive Strength", "Negative Breadth"],
        default="Positive Strength"
    )

    if chart_selection == "Positive Strength":
        # --- POSITIVE STRENGTH CHART (+3%) ---
        y_min, y_max = 0, max(df["+3%_Average"].max() + 5, 15)
        total_range = y_max - y_min
        off_5 = 5 / total_range
        off_7 = 7 / total_range

        precise_gradient = alt.Gradient(
            gradient='linear',
            stops=[
                alt.GradientStop(color='#FF3E3E', offset=0),
                alt.GradientStop(color='#FF3E3E', offset=off_5),
                alt.GradientStop(color='#FFF3A3', offset=off_5),
                alt.GradientStop(color='#FFF3A3', offset=off_7),
                alt.GradientStop(color='#2E5BFF', offset=off_7),
                alt.GradientStop(color='#2E5BFF', offset=1),
            ],
            x1=1, x2=1, y1=1, y2=0
        )

        base = alt.Chart(df).encode(
            x=alt.X("date:T", title="", axis=alt.Axis(grid=False, labelColor='#888', domain=False)),
            y=alt.Y("+3%_Average:Q", title="STRENGTH %",
                    scale=alt.Scale(domain=[y_min, y_max]),
                    axis=alt.Axis(grid=False, labelColor='#888', domain=False)),
            tooltip=[alt.Tooltip("date:T", title="Date"),
                     alt.Tooltip("+3%_Average:Q", title="Strength %", format=".2f")]
        )

        glow_line = base.mark_line(strokeWidth=8, opacity=0.1, interpolate='monotone', color='white')
        main_line = base.mark_line(strokeWidth=3, interpolate='monotone', strokeCap='round', color=precise_gradient)
        threshold_lines = alt.Chart(pd.DataFrame({'y': [5, 7]})).mark_rule(color='white', opacity=0.1,
                                                                           strokeDash=[4, 4]).encode(y='y:Q')

        # Combine and add the zoom parameter
        final_chart = (glow_line + main_line + threshold_lines).add_params(zoom_selection).properties(height=400)
        st.altair_chart(final_chart, use_container_width=True)

    else:
        # --- NEGATIVE BREADTH CHART (-3%) ---
        y_max_neg = max(df["-3%_Average"].max() + 5, 15)
        off_5_n = 5 / y_max_neg
        off_7_n = 7 / y_max_neg

        gradient_neg = alt.Gradient(
            gradient='linear',
            stops=[
                alt.GradientStop(color='#2E5BFF', offset=0),
                alt.GradientStop(color='#2E5BFF', offset=off_5_n),
                alt.GradientStop(color='#FFF3A3', offset=off_5_n),
                alt.GradientStop(color='#FFF3A3', offset=off_7_n),
                alt.GradientStop(color='#FF3E3E', offset=off_7_n),
                alt.GradientStop(color='#FF3E3E', offset=1),
            ],
            x1=1, x2=1, y1=1, y2=0
        )

        base_neg = alt.Chart(df).encode(
            x=alt.X("date:T", title="", axis=alt.Axis(grid=False, labelColor='#888')),
            y=alt.Y("-3%_Average:Q", title="Weakness %", scale=alt.Scale(domain=[0, y_max_neg]),
                    axis=alt.Axis(grid=False, labelColor='#888')),
            tooltip=[alt.Tooltip("date:T", title="Date"),
                     alt.Tooltip("-3%_Average:Q", title="Weakness %", format=".2f")]
        )

        # Combine and add the zoom parameter
        final_chart_neg = (
                base_neg.mark_line(strokeWidth=8, opacity=0.1, color='white') +
                base_neg.mark_line(strokeWidth=3, color=gradient_neg)
        ).add_params(zoom_selection).properties(height=400)

        st.altair_chart(final_chart_neg, use_container_width=True)

    # ----------------------------
    # 5. ENHANCED NET SPREAD & G/L RATIO (INTEGRATED LIVE DATA)
    # ----------------------------
    # st.subheader("🌋 Market Spread & Ratio")

    # --- STEP 1: PREPARE TODAY'S LIVE DATA ROW ---
    live_df_for_plot = fetch_tradingview_data()
    if not live_df_for_plot.empty:
        live_df_for_plot["Value_Cr"] = (live_df_for_plot["LTP"] * live_df_for_plot["Volume"]) / 1e7
        total_live = len(live_df_for_plot)
        live_gainers = len(live_df_for_plot[(live_df_for_plot["PcntChg"] >= 3) & (live_df_for_plot["Value_Cr"] >= 10)])
        live_losers = len(live_df_for_plot[(live_df_for_plot["PcntChg"] <= -3) & (live_df_for_plot["Value_Cr"] >= 10)])

        # Calculate Live Metrics
        l_strength = (live_gainers / total_live) * 100
        l_weakness = (live_losers / total_live) * 100
        l_spread = round(l_strength - l_weakness, 2)

        # Calculate Live G/L Ratio (using historical tail from your df)
        # We take the last 9 days from DB and add today
        hist_gainers_tail = df["gainers"].tail(9).tolist() + [live_gainers]
        hist_losers_tail = df["losers"].tail(9).tolist() + [live_losers]
        l_ratio = round((sum(hist_gainers_tail) / 10) / (sum(hist_losers_tail) / 10 + 0.001), 2)

        # Create a single row DataFrame for Today
        today_row = pd.DataFrame({
            "date": [pd.Timestamp(datetime.now().date())],
            "Spread": [l_spread],
            "ratio": [l_ratio]
        })

        # --- STEP 2: APPEND TODAY TO HISTORICAL DF ---
        # Ensure only necessary columns exist for plotting to avoid conflicts
        plot_df = pd.concat([df[["date", "Spread", "ratio"]], today_row], ignore_index=True)
        # Drop duplicates in case DB already has today's date (after a sync)
        plot_df = plot_df.drop_duplicates(subset=['date'], keep='last').sort_values("date")
    else:
        plot_df = df  # Fallback if live fetch fails

    # --- STEP 3: PLOT THE COMBINED DATAFRAME (ALTAIR) ---

    # We wrap the entire chart logic in an expander.
    # set 'expanded=False' if you want it hidden by default.
    with st.expander("📊 Market Momentum & Ratio Chart", expanded=False  ):

        # Optional vertical expansion toggle
        is_large = st.toggle("Full View Chart)", value=False)
        chart_height = 700 if is_large else 400

        # 1. Interaction: Scroll to Zoom & Drag to Pan
        zoom_pan = alt.selection_interval(
            bind='scales',
            encodings=['x'],
            on="[mousedown, window:mouseup] > window:mousemove!",
            translate="[mousedown, window:mouseup] > window:mousemove!",
            zoom="wheel!"
        )

        # 2. Hover Selector: Captures the nearest date for the shared hover
        nearest = alt.selection_point(
            nearest=True, on='mouseover', fields=['date'], empty=False
        )

        # 3. Base Chart (Shared X-Axis)
        base = alt.Chart(plot_df).encode(
            x=alt.X("date:T", title="", axis=alt.Axis(grid=False, labelColor='#888', domain=False))
        ).add_params(zoom_pan)

        # 4. DATA 1: Net Spread Mountain (Gradient Area)
        spread_chart = base.mark_area(
            line={'color': '#00d4ff', 'width': 1.5},
            color=alt.Gradient(
                gradient='linear',
                stops=[
                    alt.GradientStop(color='#ff3e3e', offset=0),  # Red bottom
                    alt.GradientStop(color='rgba(0,0,0,0)', offset=0.5),  # Mid-point
                    alt.GradientStop(color='#00d4ff', offset=1)  # Blue top
                ],
                x1=1, x2=1, y1=1, y2=0
            )
        ).encode(
            y=alt.Y("Spread:Q", title="NET SPREAD", scale=alt.Scale(domainMid=0), axis=alt.Axis(grid=False))
        )

        # 5. DATA 2: G/L Ratio Line (Thick Gold Line)
        ratio_chart = base.mark_line(color='#FFD700', strokeWidth=2.5).encode(
            y=alt.Y("ratio:Q", title="G/L RATIO", axis=alt.Axis(grid=False, orient='right'))
        )

        # 6. UNIFIED HOVER LAYERS
        # This ensures that as you move your mouse, both metrics show in one tooltip
        selectors = base.mark_point().encode(
            opacity=alt.value(0),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("Spread:Q", title="Net Spread", format=".2f"),
                alt.Tooltip("ratio:Q", title="G/L Ratio", format=".2f")
            ]
        ).add_params(nearest)

        # Vertical crosshair line
        rule = base.mark_rule(color='white', opacity=0.2, strokeWidth=1).encode(
            opacity=alt.condition(nearest, alt.value(0.5), alt.value(0))
        )

        # 7. Final Combined Layering
        final_chart = alt.layer(
            spread_chart,
            rule,
            ratio_chart,
            selectors
        ).resolve_scale(
            y='independent'  # Allows Ratio and Spread to coexist clearly
        ).properties(
            height=chart_height,
            background='transparent'
        ).configure_view(
            strokeOpacity=0
        )

        # Render the chart inside the expander
        st.altair_chart(final_chart, use_container_width=True)


    #
    # # ----------------------------
    # # 🗄️ SCALABLE BREADTH EXPLORER (FIXED)
    # # ----------------------------
    # st.divider()

    with st.expander("📊 Advanced Breadth Table (Chart Data)", expanded=False):
        # 1. Table Controls
        col_b, col_c = st.columns([1, 1])
        with col_b:
            page_size = st.number_input("Rows per page", min_value=10, max_value=1000, value=50)
        with col_c:
            # Calculate total pages based on the main 'df'
            total_rows = len(df)
            total_pages = (total_rows // page_size) + 1
            page_num = st.number_input(f"Page (of {total_pages})", min_value=1, max_value=total_pages, value=1)

        # 2. Slice the main 'df' (the one used for charts) for pagination
        # We sort by date DESC so the newest data is at the top
        table_df = df.sort_values("date", ascending=False).copy()

        offset = (page_num - 1) * page_size
        display_df = table_df.iloc[offset: offset + page_size].copy()

        if not display_df.empty:
            # Format dates for display
            display_df["Date"] = display_df["date"].dt.strftime('%Y-%m-%d')

            # Select and Rename columns to match the chart metrics
            # +3%_Average and -3%_Average are the "Strength" and "Weakness" plotted
            final_cols = ["Date", "total", "gainers", "losers", "ratio", "+3%_Average", "-3%_Average", "Spread"]
            display_df = display_df[final_cols]
    
            # Rename for UI clarity
            display_df.columns = ["Date", "Total Stocks", "Gainers (+3%)", "Losers (-3%)", "G/L Ratio",
                                  "Strength %", "Weakness %", "Net Spread"]

            # 3. Define CSS for the Blue Theme
            custom_css = """
            <style>
                .blue-table-container {
                    font-family: 'Cambria', serif;
                    width: 100%;
                    border-collapse: collapse;
                    margin: 10px 0;
                    font-size: 15px;
                    color: #e0e0e0;
                    background-color: #0e1117;
                }
                .blue-table {
                    width: 100%;
                    border: 1px solid #2e5bff;
                }
                .blue-table th {
                    background-color: #1e2130;
                    color: #00d4ff;
                    padding: 12px;
                    text-align: center;
                    border-bottom: 2px solid #2e5bff;
                }
                .blue-table td {
                    padding: 10px;
                    text-align: center;
                    border-bottom: 1px solid #1e2130;
                }
                .blue-table tr:hover {
                    background-color: rgba(46, 91, 255, 0.2) !important;
                }
                .blue-table tr:nth-child(even) {
                    background-color: #161a24;
                }
                .pos-val { color: #00ff88; font-weight: bold; }
                .neg-val { color: #ff3e3e; font-weight: bold; }
                .neutral-val { color: #ffd700; font-weight: bold; }
            </style>
            """
            # 4. Build HTML Table String (Optimized)
            rows_html = ""
            for _, row in display_df.iterrows():
                # Conditional formatting logic
                r_val = row["G/L Ratio"]
                s_val = row["Net Spread"]

                ratio_class = 'class="pos-val"' if r_val > 1.5 else 'class="neg-val"' if r_val < 0.7 else 'class="neutral-val"'
                spread_class = 'class="pos-val"' if s_val > 5 else 'class="neg-val"' if s_val < -5 else ""

                # Using f-strings to build each row cleanly
                rows_html += f"""
                <tr>
                    <td>{row['Date']}</td>
                    <td>{int(row['Total Stocks'])}</td>
                    <td>{int(row['Gainers (+3%)'])}</td>
                    <td>{int(row['Losers (-3%)'])}</td>
                    <td {ratio_class}>{r_val:.2f}</td>
                    <td>{row['Strength %']:.2f}%</td>
                    <td>{row['Weakness %']:.2f}%</td>
                    <td {spread_class}>{s_val:.2f}</td>
                </tr>"""

            # Assemble the final table string
            table_html = f"""
            <div class="blue-table-container">
                <table class="blue-table">
                    <thead>
                        <tr>{"".join([f"<th>{c}</th>" for c in display_df.columns])}</tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
            """
            # ... (keep your rows_html and table_html construction the same)

            # 5. Render to Streamlit using Components (Avoids the 'Raw Text' bug)
            import streamlit.components.v1 as components

            # Combine CSS and Table
            full_html_content = f"{custom_css}{table_html}"

            # Calculate height based on rows (approx 45px per row + header)
            dynamic_height = min(len(display_df) * 45 + 100, 800)

            components.html(full_html_content, height=dynamic_height, scrolling=True)

            st.write(f"Showing rows **{offset + 1}** to **{offset + len(display_df)}** of **{total_rows}**")


    # ===============================
    # 📚 HTML STYLED USER MANUAL (CAMBRIA 14px)
    # ===============================
    # st.divider()

    with st.expander("📖 Click here for the User Manual: How to Read This Dashboard", expanded=False):
        # Custom CSS for Cambria font, 14px size, and card layouts
        st.markdown("""
            <style>
                @import url('https://fonts.cdnfonts.com/css/cambria');

                .manual-container {
                    font-family: 'Cambria', serif;
                    font-size: 14px;
                }
                .manual-card {
                    padding: 18px;
                    border-radius: 10px;
                    margin-bottom: 10px;
                    border-center: 5px solid;
                    height: 100%;
                    background-color: #1e2130;
                    line-height: 1.6;
                }
                .live-card { border-center-color: #00d4ff; }
                .chart-card { border-center-color: #00ff88; }
                .table-card { border-center-color: #ffd700; padding: 10px; }

                .manual-header {
                    color: #ffffff;
                    font-size: 16px; /* Slightly larger for headers */
                    font-weight: bold;
                    margin-bottom: 12px;
                    text-decoration: underline;
                }
                .manual-text {
                    color: #cccccc;
                }
                .highlight {
                    color: #ffffff;
                    font-weight: bold;
                }
                /* Styling the markdown table to match Cambria 14 */
                .styled-table table {
                    font-family: 'Cambria', serif;
                    font-size: 14px;
                    color: #cccccc;
                }
            </style>
        """, unsafe_allow_html=True)

        # st.markdown('<div class="manual-container">', unsafe_allow_html=True)

        col_a, col_b, col_c = st.columns(3)

        with col_a:
            st.markdown("""
                <div class="manual-card live-card">
                    <div class="manual-header">🏁 The 'Quick Score' (Live Pulse)</div>
                    <div class="manual-text">
                        <b>1. Live Strength %:</b> Measures the <b>'Engine'</b> of the market.<br>
                        • <span style="color:#00ff88"><b>Above 7%:</b></span> Roaring engine. <b>High participation.</b><br>
                        • <span style="color:#ff4b4b"><b>Below 5%:</b></span> Stalling engine. <b>Few stocks moving.</b><br><br>
                        <b>2. Live G/L Ratio:</b> The <b>Tug-of-War</b> score.<br>
                        • <span class="highlight">> 1.0:</span> <b>Bulls</b> are pulling harder.<br>
                        • <span class="highlight">< 1.0:</span> <b>Bears</b> are winning today.<br><br>
                        <b>3. Sentiment:</b> Your Traffic Light.<br>
                        • <span style="color:#00ff88"><b>🚀 BULLISH:</b></span> Green light. <b>Look for buys.</b><br>
                        • <span style="color:#ffd700"><b>⚖️ NEUTRAL:</b></span> Yellow light. <b>Be selective.</b><br>
                        • <span style="color:#ff4b4b"><b>🔴 BEARISH:</b></span> Red light. <b>Stay cautious.</b>
                    </div>
                </div>
            """, unsafe_allow_html=True)

        with col_b:
            st.markdown("""
                <div class="manual-card chart-card">
                    <div class="manual-header">📈 The Charts (The Big Picture)</div>
                    <div class="manual-text">
                        <b>1. Participation Strength:</b><br>
                        • <span style="color:#00d4ff"><b>🔵 Blue Zone:</b></span> Market is <b>Healthy</b>. Broad rally.<br>
                        • <span style="color:#ff4b4b"><b>🔴 Red Zone:</b></span> Market is <b>Sick</b>. Dangerous to buy.<br><br>
                        <b>2. Net Spread (Mountain):</b><br>
                        Shows <b>Net Pressure</b>. <b>Growing mountain</b> = Buyers dominating. <b>Shrinking</b> = Buyers fleeing.<br><br>
                        <b>3. G/L Ratio (Gold Line):</b><br>
                        Your <b>Confirmation</b>. If Nifty goes up but this Gold line goes down, the rally is <b>'fake'</b> and a crash may be coming.
                    </div>
                </div>
            """, unsafe_allow_html=True)

        with col_c:
            st.markdown('<div class="manual-card table-card">', unsafe_allow_html=True)
            st.markdown("<b>📊 Summary Action Table</b>", unsafe_allow_html=True)
            st.markdown("""
            <div class="styled-table">

            | Condition | Status | Action |
            | :--- | :--- | :--- |
            | **Str > 7%** | 🔥 **Strong** | **Aggressive Longs** |
            | **Str < 5%** | ⚠️ **Weak** | **Sit on Cash** |
            | **G/L > 1.5** | 💪 **Trend** | **Hold Winners** |
            | **G/L < 0.7** | 📉 **Panic** | **No Buying** |

            </div>
            """, unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # st.markdown('</div>', unsafe_allow_html=True)  # End manual-container


if __name__ == "__main__":
    main()