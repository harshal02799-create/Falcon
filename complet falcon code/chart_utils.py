import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import streamlit as st


def show_yfinance_chart(
    symbol: str,
    period: str = "6mo",
    interval: str = "1d",
    chart_type: str = "Candlestick",
    show_volume: bool = True,
    show_sma10: bool = False,
    show_sma20: bool = False,
    show_sma50: bool = False,
    show_sma100: bool = False,
    show_sma200: bool = False,
    show_rsi: bool = False,
    show_macd: bool = False
):

    try:
        # Try NSE → BSE → GLOBAL
        for suffix in [".NS", ".BO", ""]:
            data = yf.download(symbol + suffix, period=period, interval=interval, progress=False)
            if not data.empty:
                break
        else:
            st.warning(f"⚠️ No data found for {symbol}")
            return

        # ===== Normalize column names safely =====
        fixed_cols = []
        for col in data.columns:
            if isinstance(col, tuple):
                col = col[0]
            fixed_cols.append(str(col).upper())

        data.columns = fixed_cols
        data = data.ffill().bfill()

        # ===== Indicators =====
        if show_sma10: data["SMA10"] = data["CLOSE"].rolling(10).mean()
        if show_sma20: data["SMA20"] = data["CLOSE"].rolling(20).mean()
        if show_sma50: data["SMA50"] = data["CLOSE"].rolling(50).mean()
        if show_sma100: data["SMA100"] = data["CLOSE"].rolling(100).mean()
        if show_sma200: data["SMA200"] = data["CLOSE"].rolling(200).mean()

        if show_rsi:
            delta = data["CLOSE"].diff()
            gain = np.where(delta > 0, delta, 0)
            loss = np.where(delta < 0, -delta, 0)
            avg_gain = pd.Series(gain).rolling(14).mean()
            avg_loss = pd.Series(loss).rolling(14).mean()
            rs = avg_gain / avg_loss
            data["RSI"] = 100 - (100 / (1 + rs))

        if show_macd:
            exp1 = data["CLOSE"].ewm(span=12, adjust=False).mean()
            exp2 = data["CLOSE"].ewm(span=26, adjust=False).mean()
            data["MACD"] = exp1 - exp2
            data["SIGNAL"] = data["MACD"].ewm(span=9, adjust=False).mean()

        # ===== Subplots Layout =====
        rows = 1 + int(show_volume) + int(show_rsi) + int(show_macd)
        fig = make_subplots(
            rows=rows, cols=1, shared_xaxes=True,
            row_heights=[0.6] + [0.2] * (rows - 1),
            vertical_spacing=0.02
        )

        # ===== Price Chart =====
        if chart_type == "Candlestick":
            fig.add_trace(
                go.Candlestick(
                    x=data.index,
                    open=data["OPEN"],
                    high=data["HIGH"],
                    low=data["LOW"],
                    close=data["CLOSE"],
                    increasing_line_color="#00C853",
                    decreasing_line_color="#D50000",
                ),
                row=1, col=1
            )
        else:
            colors = np.where(data["CLOSE"] >= data["OPEN"], "#00C853", "#D50000")
            fig.add_trace(go.Bar(x=data.index, y=data["CLOSE"], marker_color=colors, name="Price"),
                          row=1, col=1)

        # ===== SMA Lines =====
        for sma_col, color in [
            ("SMA10", "#FFD700"),
            ("SMA20", "#00BFFF"),
            ("SMA50", "#FF69B4"),
            ("SMA100", "#FFA500"),
            ("SMA200", "#ADFF2F"),
        ]:
            if sma_col in data.columns:
                fig.add_trace(go.Scatter(x=data.index, y=data[sma_col], mode="lines",
                                         name=sma_col, line=dict(width=1.4, color=color)), row=1, col=1)

        # ===== Volume =====
        current_row = 2
        if show_volume:
            colors = np.where(data["CLOSE"] >= data["OPEN"], "#00C853", "#D50000")
            fig.add_trace(go.Bar(x=data.index, y=data["VOLUME"], marker_color=colors, opacity=0.5,
                                 name="Volume"), row=current_row, col=1)
            current_row += 1

        # ===== RSI =====
        if show_rsi:
            fig.add_trace(go.Scatter(x=data.index, y=data["RSI"], name="RSI",
                                     line=dict(color="#FFD700", width=1.4)),
                          row=current_row, col=1)
            fig.add_hline(y=70, line_dash="dot", line_color="#FF4444", row=current_row, col=1)
            fig.add_hline(y=30, line_dash="dot", line_color="#44FF44", row=current_row, col=1)
            current_row += 1

        # ===== MACD =====
        if show_macd:
            fig.add_trace(go.Scatter(x=data.index, y=data["MACD"], line=dict(color="#00BFFF", width=1.5),
                                     name="MACD"), row=current_row, col=1)
            fig.add_trace(go.Scatter(x=data.index, y=data["SIGNAL"], line=dict(color="#FFB6C1", width=1),
                                     name="SIGNAL"), row=current_row, col=1)

        # ===== Layout =====
        # fig.update_layout(
        #     template="plotly_dark",
        #     title=f"📈 {symbol} ({period}, {interval})",
        #     height=900,
        #     margin=dict(l=20, r=20, t=50, b=20),
        #     hovermode="x unified",
        #     xaxis_rangeslider_visible=False,
        #     plot_bgcolor="#000000",
        #     paper_bgcolor="#000000",
        #     font=dict(family="Cambria", size=14, color="#DDD"),
        # )
        fig.update_layout(
            dragmode="pan",                     # 🟢 Drag to move chart  
            hovermode="x unified",
            xaxis_rangeslider_visible=False,
            
            xaxis=dict(
                showgrid=False,
                linecolor="#444",
                fixedrange=False,               # 🟢 allow zoom with mouse scroll
                tickfont=dict(size=12),         # 🟢 axis stays stable
                automargin=True,
                constrain="domain"              # ⭐ axis does NOT stretch
            ),
            
            yaxis=dict(
                showgrid=False,
                linecolor="#444",
                fixedrange=False,               # 🟢 allow zoom vertically
                tickfont=dict(size=12),         # 🟢 axis stays stable
                automargin=True,
                scaleratio=1,                   
                constrain="domain"              # ⭐ axis does NOT stretch
            ),

            template="plotly_dark",
            title=f"📈 {symbol} ({period}, {interval})",
            height=900,
            margin=dict(l=20, r=20, t=50, b=20),
            plot_bgcolor="#000000",
            paper_bgcolor="#000000",
            font=dict(family="Cambria", size=14, color="#DDD"),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Chart Error: {e}")
