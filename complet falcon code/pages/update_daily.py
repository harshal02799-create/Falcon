import asyncio
import sqlite3
import os
from datetime import datetime, timedelta
from data1 import UpstoxNSEDownloader


# ---------- PATH ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "nse_analytics_clean.db")


# ---------- SQLITE HELPERS ----------
def sqlite_connect():
    return sqlite3.connect(
        DB_PATH,
        timeout=60,
        isolation_level=None
    )


def get_last_date():
    conn = sqlite_connect()
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM ohlc_enriched")
    last_date = cur.fetchone()[0]
    conn.close()
    return last_date


def merge_ohlc_into_enriched():
    conn = sqlite_connect()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO ohlc_enriched (
        symbol, date, open, high, low, close, volume,
        avg_price, turnover, change_pct
    )
    SELECT
        o.symbol,
        o.date,
        o.open,
        o.high,
        o.low,
        o.close,
        o.volume,

        ROUND((o.open + o.high + o.low + o.close) / 4.0, 2),

        ROUND(
            ((o.open + o.high + o.low + o.close) / 4.0)
            * o.volume / 10000000.0, 2
        ),

        ROUND(
            (
                o.close - (
                    SELECT p.close
                    FROM ohlc_enriched p
                    WHERE p.symbol = o.symbol
                      AND p.date < o.date
                    ORDER BY p.date DESC
                    LIMIT 1
                )
            ) * 100.0 /
            (
                SELECT p.close
                FROM ohlc_enriched p
                WHERE p.symbol = o.symbol
                  AND p.date < o.date
                ORDER BY p.date DESC
                LIMIT 1
            ),
            2
        )
    FROM ohlc o
    WHERE NOT EXISTS (
        SELECT 1
        FROM ohlc_enriched e
        WHERE e.symbol = o.symbol
          AND e.date = o.date
    );
    """)

    rows = cur.rowcount
    conn.commit()
    conn.close()
    return rows


# ---------- MAIN ----------
async def run_daily_update():
    last_date = get_last_date()

    if not last_date:
        print("❌ DB empty. Run historical downloader once.")
        return

    start_date = (
        datetime.fromisoformat(last_date) + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date > end_date:
        print("✅ DB already up-to-date")
        return

    print(f"📥 Downloading {start_date} → {end_date}")

    downloader = UpstoxNSEDownloader(
        db_path=DB_PATH,
        max_per_sec=5,
        max_per_min=500,
        concurrency=2,
        db_batch_size=10000,
    )

    downloader.start_date = start_date
    downloader.end_date = end_date

    # 🔥 ONLY async work here
    await downloader.run()

    # 🔥 async finished, DB unlocked
    print("🔁 Merging into ohlc_enriched...")
    inserted = merge_ohlc_into_enriched()

    print(f"✅ Update complete | Rows added: {inserted}")


# ---------- RUN ----------
if __name__ == "__main__":
    asyncio.run(run_daily_update())
