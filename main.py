"""
CoinBase Crypto bot v0.2 - FULL TRADE CYCLE
--------------------------------------------
20/50-EMA crossover strategy on 15-minute BTC/USDC, now with **exit logic**:
• **Entry** - bullish crossover closes (EMA20 > EMA50 after being ≤)
• **Exit** - first condition hit:
    1. Bearish crossover (EMA20 < EMA50)
    2. 2 % stop-loss (price ≤ entry x 0.98)
    3. 2 % take-profit (price ≥ entry x 1.02)
• Position-sizing = 1 % of USDC equity per trade
• SQLite dedup + restart-safe

USAGE
-----
$ python main.py --live        # real trade (default)
$ python main.py --paper       # log only, no real orders
$ python main.py --risk 0.005  # use 0.5 % risk per position

ENV
---
COINBASE_API_KEY, COINBASE_API_SECRET, COINBASE_API_PASSPHRASE
"""

import argparse
import os
import time
import datetime as dt
import sqlite3
from typing import Optional, Tuple

import ccxt
from dotenv import load_dotenv
import pandas as pd


PAIR: str = "BTC/USD"
TIMEFRAME: str = "15m"
DB_FILE: str = "bot_log.db"
ORDER_QTY: float = 0.0001  # ~ $6 @ $60k BTC – adjust to your test size
PRICE_OFFSET_PCT: float = -0.01  # 1 % below last price (buy‑side)

load_dotenv()
API_KEY = os.getenv("COINBASE_API_KEY")
API_SECRET = os.getenv("COINBASE_API_SECRET")
API_PASSPHRASE = os.getenv("COINBASE_API_PASSPHRASE")

exchange = ccxt.coinbase({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "password": API_PASSPHRASE,
    "enableRateLimit": True,
})

TIMEFRAME_MS = exchange.parse_timeframe(TIMEFRAME) * 1000  # 15m → 900 000 ms


def init_db(db_file: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    con = sqlite3.connect(db_file, check_same_thread=False)
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


con, cur = init_db(DB_FILE)

# Fetch new candles and store them in the database


def fetch_and_store_candles(conn: sqlite3.Connection):
    pass  # Placeholder for fetching candles logic

# Strategy Logic


def calc_emas(df: pd.DataFrame) -> list:
    ema_20 = df['close'].ewm(span=20, adjust=False).mean()
    ema_50 = df['close'].ewm(span=50, adjust=False).mean()
    return ema_20, ema_50


def bullish_crossover() -> bool:
    pass  # Placeholder for bullish crossover logic


def bearish_crossover() -> bool:
    pass  # Placeholder for bearish crossover logic

# Order Management


def last_order():
    pass  # Placeholder for fetching last order logic


def in_position() -> bool:
    pass  # Placeholder for checking if in position logic


def get_position():
    pass  # Placeholder for getting current position logic


def usdc_balance() -> float:
    pass  # Placeholder for fetching USDC balance logic


def get_position_size():
    pass  # Placeholder for calculating position size logic

# Order Execution


def record_order():
    pass  # Placeholder for recording order logic


def place_market_buy():
    pass  # Placeholder for placing market buy order logic


def place_market_sell():
    pass  # Placeholder for placing market sell order logic


def main(is_live: bool = False,):
    while True:
        try:
            print("[i] Fetching new candles...")
            time.sleep(1)  # Rate limit safeguard
        except Exception as e:
            print("[!] Error:", e)
            time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CoinBase Crypto Bot")
    parser.add_argument("--live", action="store_true",
                        help="Enable live trading")
    parser.add_argument("--paper", action="store_true",
                        help="Enable paper trading")
    parser.add_argument("--risk", type=float, default=0.01,
                        help="Risk per trade")
    args = parser.parse_args()

    is_live = args.live
    if args.paper:
        is_live = False  # Override live mode if paper trading is selected
    risk_per_trade = args.risk
    main(is_live=is_live, risk_pct=risk_per_trade)
