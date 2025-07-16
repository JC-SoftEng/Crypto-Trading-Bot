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
from typing import Optional, Tuple

import ccxt
from dotenv import load_dotenv
import pandas as pd


PAIR: str = "BTC/USD"
TIMEFRAME: str = "15m"
DB_FILE: str = "bot_log.db"
ORDER_QTY: float = 0.00001  # ~ $0.60 @ $60k BTC – adjust to your test size
PRICE_OFFSET_PCT: float = -0.01  # 1 % below last price (buy‑side)
CURRENCY: str = "USDC"

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


def fetch_and_store_candles(conn: sqlite3.Connection):
    pass  # Placeholder for fetching candles logic

# Strategy Logic


def calc_ema(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    Calculates the 20-period and 50-period EMAs from the 'close' column.
    Args:
        pd.DataFrame: DataFrame containing 'close' prices.
    Returns:
        (Tuple[pd.Series, pd.Series]): A tuple containing two Series: (EMA 20, EMA 50) in this order.
    Raises:
        KeyError: If 'close' column is not present in the DataFrame.
    """
    if 'close' not in df.columns:
        raise KeyError("DataFrame must contain 'close' column.")
    ema_20 = df['close'].ewm(span=20, adjust=False).mean()
    ema_50 = df['close'].ewm(span=50, adjust=False).mean()
    return (ema_20, ema_50)


def bullish_crossover(df: pd.DataFrame) -> bool:
    """
    Checks for a bullish crossover between the 20-period and 50-period EMAs.
    Args:
        pd.DataFrame: DataFrame containing 'close' prices.
    Returns:
        bool: True if a bullish crossover occurred, False otherwise.
    """
    required_cols = ['close']
    if len(df) < 2 or not all(col in df.columns for col in required_cols):
        return False
    if df['close'].isnull().any():
        return False
    ema_20, ema_50 = calc_ema(df)
    if ema_20.empty or ema_50.empty:
        return False
    # Check for NaN values in the last two elements
    if (
        pd.isna(ema_20.iloc[-1]) or pd.isna(ema_50.iloc[-1]) or
        pd.isna(ema_20.iloc[-2]) or pd.isna(ema_50.iloc[-2])
    ):
        return False
    crossed_above = ema_20.iloc[-1] > ema_50.iloc[-1]
    was_below = ema_20.iloc[-2] <= ema_50.iloc[-2]
    return crossed_above and was_below


def bearish_crossover(df: pd.DataFrame) -> bool:
    ema_20, ema_50 = calc_ema(df)
    if ema_20.empty or ema_50.empty:
        return False
    crossed_below = ema_20.iloc[-1] < ema_50.iloc[-1]
    was_above = ema_20.iloc[-2] >= ema_50.iloc[-2]
    return crossed_below and was_above


def bearish_crossover(df: pd.DataFrame) -> bool:
    ema_20, ema_50 = calc_ema(df)
    if ema_20.empty or ema_50.empty:
        return False
    return ema_20.iloc[-1] < ema_50.iloc[-1] and ema_20.iloc[-2] >= ema_50.iloc[-2]

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
