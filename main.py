"""
Coinbase REST MVP bot (dedup‑safe)
---------------------------------
• Pulls 15‑minute BTC/USDC candles from Coinbase Advanced REST
• Inserts **unique** rows using SQLite constraints (`INSERT OR IGNORE`)
• Drops a dummy limit order then cancels it, recording fills with `INSERT OR REPLACE`
• Safe to stop/start without duplicating data
"""

import os
import time
import datetime as dt
import sqlite3
from typing import Optional

import ccxt
from dotenv import load_dotenv


PAIR: str = "BTC/USD"
TIMEFRAME: str = "15m"
DB_FILE: str = "bot_log.db"
ORDER_QTY: float = 0.0001  # ~ $6 @ $60k BTC – adjust to your test size
PRICE_OFFSET_PCT: float = -0.01  # 1 % below last price (buy‑side)
SLEEP_SEC: int = 30  # loop pause between pulls

load_dotenv()
API_KEY = os.getenv("COINBASE_API_KEY")
API_SECRET = os.getenv("COINBASE_API_SECRET")
API_PASSPHRASE = ""  # No passphrase available for Coinbase Pro API

exchange = ccxt.coinbase({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "password": API_PASSPHRASE,
    "enableRateLimit": True,
})

TIMEFRAME_MS = exchange.parse_timeframe(TIMEFRAME) * 1000  # 15m → 900 000 ms


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS candles (
                ts       INTEGER NOT NULL,
                pair     TEXT    NOT NULL,
                timeframe TEXT   NOT NULL,
                open     REAL,
                high     REAL,
                low      REAL,
                close    REAL,
                volume   REAL,
                UNIQUE (ts, pair, timeframe)
            );"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS orders (
                id       TEXT PRIMARY KEY,
                ts       INTEGER,
                pair     TEXT,
                side     TEXT,
                price    REAL,
                qty      REAL,
                status   TEXT
            );"""
    )
    conn.commit()
    return conn


def last_candle_ts(conn: sqlite3.Connection) -> Optional[int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(ts) FROM candles WHERE pair = ? AND timeframe = ?;",
        (PAIR, TIMEFRAME),
    )
    res = cur.fetchone()
    return res[0] if res and res[0] else None


def upsert_candles(conn: sqlite3.Connection):
    since_ts = last_candle_ts(conn)

    # Advance one full bar so we don't re‑request the last candle
    if since_ts is not None:
        next_since = since_ts + TIMEFRAME_MS
        now_ms = int(time.time() * 1000)
        # Coinbase rejects future "start"; reset if next_since ≥ now
        since_ts = next_since if next_since < now_ms else None

    candles = exchange.fetch_ohlcv(
        PAIR,
        timeframe=TIMEFRAME,
        since=since_ts,
        limit=500,
    )

    if not candles:
        print("[i] No new candles available yet")
        return

    cur = conn.cursor()
    for ts, o, h, l, c, v in candles:
        cur.execute(
            """INSERT OR IGNORE INTO candles
                   (ts, pair, timeframe, open, high, low, close, volume)
                   VALUES (?,?,?,?,?,?,?,?);""",
            (ts, PAIR, TIMEFRAME, o, h, l, c, v),
        )
    conn.commit()
    last_bar_time = dt.datetime.utcfromtimestamp(candles[-1][0] / 1000)
    print(f"[+] Stored {len(candles)} candles up to {last_bar_time}")


def demo_order(conn: sqlite3.Connection):
    ticker = exchange.fetch_ticker(PAIR)
    mark_price = ticker["last"]
    buy_price = round(mark_price * (1 + PRICE_OFFSET_PCT), 2)
    print(f"[i] Placing limit‑buy {ORDER_QTY} {PAIR} at {buy_price}")
    order = exchange.create_limit_buy_order(PAIR, ORDER_QTY, buy_price)
    record_order(conn, order)
    time.sleep(2)  # give the exchange a breath
    exchange.cancel_order(order["id"], PAIR)
    canceled = exchange.fetch_order(order["id"], PAIR)
    record_order(conn, canceled)
    print("[+] Dummy order placed & canceled")


def record_order(conn: sqlite3.Connection, o: dict):
    cur = conn.cursor()
    cur.execute(
        """INSERT OR REPLACE INTO orders
               (id, ts, pair, side, price, qty, status)
               VALUES (?,?,?,?,?,?,?);""",
        (
            o["id"],
            int(time.time() * 1000),
            PAIR,
            o["side"],
            o.get("price") or o.get("price_average"),
            o["filled"],
            o["status"],
        ),
    )
    conn.commit()


def main():
    conn = init_db(DB_FILE)
    while True:
        try:
            upsert_candles(conn)
            # demo_order(conn)
        except Exception as e:
            print("[!] Error:", e)
        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
