"""
Coinbase REST MVP bot v0.4 BUY-LOGIC
--------------------------------------
Simple 20/50-EMA crossover long strategy on 15-minute BTC/USDC.
• Dedup-safe SQLite storage (candles, orders)
• Restart-safe candle fetch (no future `since`)
• Position sizing = 1 % of account equity
• Entry: bullish crossover just closed (EMA20 now > EMA50 and previously ≤)
• Exit logic not yet implemented (next milestone)

USAGE
-----
$ python coinbase_rest_mvp.py --live     # real trade
$ python coinbase_rest_mvp.py --paper    # log only

ENV
---
COINBASE_API_KEY, COINBASE_API_SECRET, COINBASE_API_PASSPHRASE
"""

import os
import time
import datetime as dt
import argparse
import math
import sqlite3
from typing import Tuple

import pandas as pd
import ccxt
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("COINBASE_API_KEY")
API_SECRET = os.getenv("COINBASE_API_SECRET")
API_PASSPHRASE = ""  # No passphrase available for Coinbase Pro API
PAIR: str = "BTC/USD"
TIMEFRAME: str = "15m"
DB_FILE: str = "bot_log.db"
RISK_PCT = 0.01  # 1 % per position
MIN_QTY = 0.0001  # Coinbase min size for BTC

exchange = ccxt.coinbase({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "password": API_PASSPHRASE,
    "enableRateLimit": True,
})

TIMEFRAME_MS = exchange.parse_timeframe(TIMEFRAME) * 1000  # 15m → 900 000 ms


def init_db(db_file: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Create/connect to SQLite DB and ensure schema exists."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS candles (
        ts INTEGER,
        pair TEXT,
        timeframe TEXT,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        UNIQUE (ts, pair, timeframe))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,
        ts INTEGER,
        side TEXT,
        price REAL,
        amount REAL,
        status TEXT)""")
    con.commit()
    return con, cur


# Initialize DB
con, cur = init_db(DB_FILE)

# Candle Fetching


def fetch_and_store_new_candles():
    # find last stored ts
    cur.execute(
        "SELECT COALESCE(MAX(ts), 0) FROM candles WHERE pair=? AND timeframe=?", (PAIR, TIMEFRAME))
    last_ts = cur.fetchone()[0]
    # ccxt wants ms
    if last_ts == 0:
        since = None
    else:
        since = last_ts + 60_000  # one minute after last bar’s open
        # avoid future
        since = min(since, int(time.time() * 1000) - 60_000)
    candles = exchange.fetch_ohlcv(
        PAIR, timeframe=TIMEFRAME, since=since, limit=200)
    rows = [(ts, PAIR, TIMEFRAME, o, h, l, c, v)
            for ts, o, h, l, c, v in candles]
    cur.executemany(
        "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return len(rows)

# The strategy logic


def calc_emas(df: pd.DataFrame):
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    return df


def bullish_crossover(df: pd.DataFrame) -> bool:
    if len(df) < 51:
        return False
    # last two closes
    last = df.iloc[-1]
    prev = df.iloc[-2]
    return prev["ema20"] <= prev["ema50"] and last["ema20"] > last["ema50"]


def in_open_position() -> bool:
    # check outstanding orders or manual flag; for v0.4 assume flat (no exits yet)
    open_orders = exchange.fetch_open_orders(symbol=PAIR)
    return len(open_orders) > 0


def position_size(price: float) -> float:
    bal = exchange.fetch_balance()["total"].get("USDC", 0)
    stake = bal * RISK_PCT
    qty = stake / price
    return max(round(qty, 6), MIN_QTY)


def place_market_buy(qty: float):
    order = exchange.create_market_buy_order(PAIR, qty)
    cur.execute("INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?)", (
        order["id"], int(time.time()*1000), "buy", order["average"], order["filled"], order["status"]))
    con.commit()
    print("[+] Placed market buy", order["id"], "qty", qty)


def loop(is_live: bool = True):
    """Main bot loop. Set `is_live=False` to paper‑trade."""
    while True:
        try:
            print("[*] Fetching new candles...")
            inserted = fetch_and_store_new_candles()
            if inserted:
                print("[+] Inserted", inserted, "new candles")
                df = pd.read_sql_query("SELECT * FROM candles WHERE pair=? AND timeframe=? ORDER BY ts", con,
                                       params=(PAIR, TIMEFRAME))
                df = calc_emas(df)
                if not in_open_position() and bullish_crossover(df):
                    print("[+] Bullish crossover detected!")
                    price = df.iloc[-1]["close"]
                    qty = position_size(price)
                    if is_live:
                        print("[*] Placing real market buy order for",
                              qty, "BTC at", price)
                        place_market_buy(qty)
                    else:
                        print("[PAPER] Would buy", qty, "BTC at", price)
            time.sleep(10)  # stay under rate limits
        except Exception as e:
            print("[!] Error:", e)
            time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper", action="store_true",
                        help="Run in paper-trade mode (no real orders)")
    args = parser.parse_args()
    is_live = not args.paper
    loop(is_live=is_live)
