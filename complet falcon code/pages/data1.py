import asyncio
import aiohttp
import aiosqlite
import json
import logging
import time
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm_asyncio

logging.basicConfig(level=logging.INFO, format="%(message)s")


class UpstoxNSEDownloader:
    def __init__(
        self,
        db_path="nse_data.db",
        json_path="OdinMasterData.CompanyMaster.json",
        max_per_sec=7,
        max_per_min=800,
        concurrency=2,
        db_batch_size=50000,   # 🔥 chunk size
    ):
        self.db_path = db_path
        self.json_path = json_path
        self.base_url = "https://api.upstox.com/v2/historical-candle/{}/day/{}/{}"

        # rate limits
        self.semaphore = asyncio.Semaphore(concurrency)
        self.max_per_sec = max_per_sec
        self.max_per_min = max_per_min
        self.db_batch_size = db_batch_size

        self.sec_times = []
        self.min_times = []
        self.rate_lock = asyncio.Lock()

        self.start_date = "2020-01-01"
        self.end_date = datetime.now().strftime("%Y-%m-%d")


    # ------------------ STORAGE ------------------ #
    async def init_storage(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS ohlc (
                    symbol TEXT,
                    date TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    PRIMARY KEY (symbol, date)
                )
                """
            )
            await db.commit()

    # ------------------ INSTRUMENTS ------------------ #
    def get_instruments(self):
        with open(self.json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        instruments = []
        for x in data:
            if (
                x.get("NSEStatus") == "Active"
                and x.get("nsesymbol")
                and x.get("isin")
            ):
                instruments.append(
                    {
                        "symbol": x["nsesymbol"],
                        "key": f"NSE_EQ|{x['isin']}",
                    }
                )
        return instruments

    # ------------------ RATE LIMITER ------------------ #
    async def rate_limit(self):
        async with self.rate_lock:
            now = time.monotonic()

            self.sec_times = [t for t in self.sec_times if now - t < 1]
            self.min_times = [t for t in self.min_times if now - t < 60]

            if len(self.sec_times) >= self.max_per_sec:
                await asyncio.sleep(1 - (now - self.sec_times[0]))

            if len(self.min_times) >= self.max_per_min:
                await asyncio.sleep(60 - (now - self.min_times[0]))

            now = time.monotonic()
            self.sec_times.append(now)
            self.min_times.append(now)

    # ------------------ FETCH ------------------ #
    async def fetch_candle(self, session, inst):
        url = self.base_url.format(inst["key"], self.end_date, self.start_date)

        async with self.semaphore:
            await self.rate_limit()

            try:
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "success":
                            return inst["symbol"], data["data"].get("candles", [])
                    elif resp.status == 429:
                        logging.warning(f"429 Rate limit: {inst['symbol']}")
            except Exception as e:
                logging.warning(f"Fetch error {inst['symbol']}: {e}")

        return inst["symbol"], []

    # ------------------ CHUNKED DB WRITER ------------------ #
    async def writer(self, queue, db):
        buffer = []

        while True:
            item = await queue.get()

            if item is None:
                break

            symbol, candles = item
            for c in candles:
                buffer.append(
                    (
                        symbol,
                        c[0].split("T")[0],
                        c[1],
                        c[2],
                        c[3],
                        c[4],
                        c[5],
                    )
                )

            if len(buffer) >= self.db_batch_size:
                await self.flush(db, buffer)
                buffer.clear()

            queue.task_done()

        # final flush
        if buffer:
            await self.flush(db, buffer)

    async def flush(self, db, buffer):
        try:
            await db.executemany(
                "INSERT OR IGNORE INTO ohlc VALUES (?,?,?,?,?,?,?)",
                buffer,
            )
            await db.commit()
        except Exception as e:
            logging.warning(f"DB batch insert error: {e}")

    # ------------------ PROCESS STOCK ------------------ #
    async def process_stock(self, session, inst, queue):
        symbol, candles = await self.fetch_candle(session, inst)
        if candles:
            await queue.put((symbol, candles))

    # ------------------ RUN ------------------ #
    async def run(self):
        await self.init_storage()

        print("Loading instruments...")
        loop = asyncio.get_running_loop()
        instruments = await loop.run_in_executor(None, self.get_instruments)

        print(f"Total NSE stocks: {len(instruments)}")

        write_queue = asyncio.Queue(maxsize=100)  # 🔥 queue back-pressure

        async with aiosqlite.connect(self.db_path) as db:
            writer_task = asyncio.create_task(self.writer(write_queue, db))

            async with aiohttp.ClientSession() as session:
                tasks = [
                    self.process_stock(session, inst, write_queue)
                    for inst in instruments
                ]

                await tqdm_asyncio.gather(
                    *tasks,
                    desc="Downloading NSE OHLC",
                    total=len(tasks),
                )

            await write_queue.put(None)
            await writer_task

        print("✅ Download completed safely with chunked DB writes.")


# ------------------ MAIN ------------------ #
if __name__ == "__main__":
    downloader = UpstoxNSEDownloader(
        max_per_sec=7,
        max_per_min=999,
        concurrency=3,
        db_batch_size=50000,  # adjust if needed
    )
    # asyncio.run(downloader.run())
