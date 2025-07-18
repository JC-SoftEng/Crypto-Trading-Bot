"""
CoinBase Crypto Bot
===================
This version of the bot is my own

USAGE
-----
$ python main.py --live        # real trade (default)
$ python main.py --paper       # log only, no real orders
$ python main.py --risk 0.5  # use 0.5 % risk per position

ENV
---
COINBASE_API_KEY, COINBASE_API_SECRET, COINBASE_API_PASSPHRASE
"""

import argparse
import os
import time
import datetime as dt
import sqlite3
from typing import Iterable, Optional, Tuple

import ccxt
from dotenv import load_dotenv
import pandas as pd


PAIR: str = "BTC/USD"
TIMEFRAME: str = "15m"
DB_FILE: str = "bot_log.db"
ORDER_QTY: float = 0.00001  # ~ $0.60 @ $60k BTC – adjust to your test size
PRICE_OFFSET_PCT: float = -0.01  # 1 % below last price (buy‑side)
CURRENCY: str = "USDC"
BARS_LOOKBACK: int = 200

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
    cur.execute(
        """CREATE TABLE IF NOT EXISTS logs (
                ts INTEGER,
                state TEXT,
                decision TEXT,
                pnl REAL,
                equity REAL
            )"""
    )
    con.commit()
    return con, cur


con, cur = init_db(DB_FILE)

# Fetch new candles and store them in the database


def fetch_candles(conn: sqlite3.Connection):
    pass  # Placeholder for fetching candles logic


def store_candles():
    pass  # Placeholder for storing candles logic

# Strategy Logic


def calc_emas(df: pd.DataFrame, periods: Iterable[int] = (20, 50), price_col: str = 'close', adjust: bool = False) -> pd.DataFrame:
    """Return a DataFrame of EMAs for each period.

    Args:
        df: Price DataFrame containing *price_col*.
        periods: Iterable of look-back spans (e.g., 20, 50, 200).
        price_col: Column on which to compute EMAs.
        adjust: Passed to `pd.Series.ewm`.

    Returns:
        pd.DataFrame with one column per period, named ``ema_{period}``.

    Raises:
        KeyError: *price_col* not in *df*.
        ValueError: *df* is empty.
    """
    if price_col not in df.columns:
        raise KeyError(f"DataFrame must contain '{price_col}' column.")
    if df.empty:
        raise ValueError("DataFrame is empty, cannot compute EMAs.")
    return pd.concat(
        {f"ema_{p}": df[price_col].ewm(
            span=p, adjust=adjust).mean() for p in periods},
        axis=1
    )


def crosses(fast: pd.Series, slow: pd.Series, direction: str = "up") -> bool:
    """
    Detects latest crossover.

    direction: "up" for bullish, "down" for bearish.
    """
    if len(fast) < 2 or len(slow) < 2:
        return False
    if fast.index[-2:] != slow.index[-2:]:
        raise ValueError("Series misaligned")

    if direction == "up":
        return fast.iloc[-1] > slow.iloc[-1] and fast.iloc[-2] <= slow.iloc[-2]
    if direction == "down":
        return fast.iloc[-1] < slow.iloc[-1] and fast.iloc[-2] >= slow.iloc[-2]
    raise ValueError("direction must be 'up' or 'down'")


# Order Management


def last_order():
    pass  # Placeholder for fetching last order logic


def in_position() -> bool:
    pass  # Placeholder for checking if in position logic


def get_position():
    pass  # Placeholder for getting current position logic


def get_balance() -> float:
    pass  # Placeholder for fetching USDC account balance logic


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
