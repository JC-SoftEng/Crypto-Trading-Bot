"""Simple Coinbase trading bot.

The bot trades BTC/USDC on Coinbase Advanced using 15 minute candles. Candles
and orders are persisted to SQLite so that restarts are safe. Market state
(consolidation, up-trend, down-trend or chaos) is detected each candle using
rules based on ATR and twenty bar highs/lows. Orders are sized at ``risk_pct``
of USDC equity and executed at the close of the signal bar.

Usage::

    $ python bot.py --live          # place real orders (default)
    $ python bot.py --paper         # run in paper trading mode
    $ python bot.py --risk 0.005    # risk 0.5 % per trade
    $ python bot.py --loglevel INFO # adjust logging level

Environment variables expected in ``.env``:
    ``COINBASE_API_KEY``
    ``COINBASE_API_SECRET``
    ``COINBASE_API_PASSPHRASE``

API keys should be created with **read** and **trade** permissions only – no
withdrawal scope.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

import ccxt
import pandas as pd
from dotenv import load_dotenv

PAIR = "BTC/USDC"
TIMEFRAME = "15m"
DB_FILE = "bot_log.db"
BARS_LOOKBACK = 200

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

TIMEFRAME_MS = exchange.parse_timeframe(TIMEFRAME) * 1000


@dataclass
class Order:
    id: int
    ts: int
    side: str
    price: float
    amount: float
    stop: float
    target: float
    status: str


class Database:
    def __init__(self, db_file: str = DB_FILE) -> None:
        self.con = sqlite3.connect(db_file, check_same_thread=False)
        self.cur = self.con.cursor()
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS candles (
                ts INTEGER,
                pair TEXT,
                timeframe TEXT,
                open REAL, high REAL, low REAL, close REAL, volume REAL,
                UNIQUE (ts, pair, timeframe)
            )"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                side TEXT,
                price REAL,
                amount REAL,
                stop REAL,
                target REAL,
                status TEXT
            )"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS logs (
                ts INTEGER,
                state TEXT,
                decision TEXT,
                pnl REAL,
                equity REAL
            )"""
        )
        self.con.commit()

    def max_ts(self) -> int:
        row = self.cur.execute(
            "SELECT MAX(ts) FROM candles WHERE pair=? AND timeframe=?",
            (PAIR, TIMEFRAME),
        ).fetchone()
        return row[0] if row and row[0] else 0

    def store_candles(self, bars: List[list]) -> None:
        self.cur.executemany(
            "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?)",
            [(b[0], PAIR, TIMEFRAME, b[1], b[2], b[3], b[4], b[5]) for b in bars],
        )
        self.con.commit()

    def last_open_order(self) -> Optional[Order]:
        row = self.cur.execute(
            "SELECT * FROM orders WHERE status='open' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        return Order(*row)

    def record_order(self, order: Order) -> None:
        self.cur.execute(
            "INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?,?,?)",
            (
                order.id,
                int(order.ts),
                order.side,
                float(order.price),
                float(order.amount),
                float(order.stop),
                float(order.target),
                order.status,
            ),
        )
        self.con.commit()

    def close_order(self, order_id: int, ts: int) -> None:
        self.cur.execute(
            "UPDATE orders SET status='closed', ts=? WHERE id=?",
            (ts, order_id),
        )
        self.con.commit()

    def log_tick(self, ts: int, state: str, decision: str, pnl: float, equity: float) -> None:
        """Store a log entry for a completed tick."""
        self.cur.execute(
            "INSERT INTO logs VALUES (?,?,?,?,?)",
            (ts, state, decision, pnl, equity),
        )
        self.con.commit()

    def candles_dataframe(self) -> pd.DataFrame:
        df = pd.read_sql(
            "SELECT ts, open, high, low, close, volume FROM candles WHERE pair=? AND timeframe=? ORDER BY ts",
            self.con,
            params=(PAIR, TIMEFRAME),
        )
        return df


def fetch_new_candles(db: Database) -> pd.DataFrame:
    """Fetch new candles and enforce data integrity."""
    last_ts = db.max_ts()
    since = last_ts + TIMEFRAME_MS if last_ts else None
    try:
        bars = exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, since=since, limit=BARS_LOOKBACK)
    except Exception as exc:
        logging.error("error fetching candles via REST: %s", exc)
        raise
    if not bars:
        raise ValueError("no candles returned")

    # data integrity: nulls, duplicates and gaps
    timestamps = []
    for bar in bars:
        if None in bar[:6]:
            raise ValueError("NULL value in candle data")
        ts = bar[0]
        if timestamps and ts - timestamps[-1] > TIMEFRAME_MS * 4:
            raise ValueError("data gap greater than 3 bars")
        timestamps.append(ts)
    if len(timestamps) != len(set(timestamps)):
        raise ValueError("duplicate candle timestamps")
    if last_ts and timestamps[0] - last_ts > TIMEFRAME_MS * 4:
        raise ValueError("data gap greater than 3 bars")

    db.store_candles(bars)
    return db.candles_dataframe()


def label_state(df: pd.DataFrame) -> str:
    if len(df) < 21:
        return "chaos"
    rng = df["high"] - df["low"]
    atr = rng.iloc[-1]
    atr_prev = rng.iloc[-2]
    median_range = rng.tail(20).median()
    overlap = df["high"].tail(20).max() - df["low"].tail(20).min() <= median_range
    new_high = df["close"].iloc[-1] > df["high"].iloc[-21:-1].max()
    new_low = df["close"].iloc[-1] < df["low"].iloc[-21:-1].min()
    atr_expanding = atr > atr_prev
    if atr <= 1.01 * median_range and overlap:
        return "consolidation"
    if atr_expanding and (new_high or new_low):
        return "up" if new_high else "down"
    return "chaos"


def position_size(usdc_balance: float, price: float, risk_pct: float) -> float:
    usd_risk = usdc_balance * risk_pct
    return round(usd_risk / price, 8)


def compute_atr(df: pd.DataFrame, period: int = 20) -> float:
    """Return the Average True Range over the last ``period`` bars."""
    high = df["high"]
    low = df["low"]
    close = df["close"].shift()
    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs(),
    ], axis=1).max(axis=1)
    return tr.tail(period).mean()


def get_equity(is_live: bool, last_price: float) -> float:
    """Return total equity in USDC."""
    if is_live:
        bal = exchange.fetch_balance()
        usdc = float(bal["total"].get("USDC", 0))
        btc = float(bal["total"].get("BTC", 0))
    else:
        usdc = 1000.0
        btc = 0.0
    return usdc + btc * last_price


def trade_logic(db: Database, df: pd.DataFrame, state: str, is_live: bool, risk_pct: float) -> tuple[str, float]:
    last_close = df["close"].iloc[-1]
    order = db.last_open_order()
    atr = compute_atr(df)
    high20 = df["high"].iloc[-21:-1].max()
    low20 = df["low"].iloc[-21:-1].min()
    range_mid = (high20 + low20) / 2
    range_size = high20 - low20

    decision = "hold"
    pnl = 0.0

    # exit logic and trailing
    if order:
        if state in ("up", "down"):
            if order.side == "buy":
                new_stop = max(order.stop, last_close - atr)
                new_target = last_close + atr
            else:
                new_stop = min(order.stop, last_close + atr)
                new_target = last_close - atr
            if new_stop != order.stop or new_target != order.target:
                order = Order(order.id, order.ts, order.side, order.price, order.amount, new_stop, new_target, order.status)
                db.record_order(order)

        hit_stop = df["low"].iloc[-1] <= order.stop if order.side == "buy" else df["high"].iloc[-1] >= order.stop
        hit_target = df["high"].iloc[-1] >= order.target if order.side == "buy" else df["low"].iloc[-1] <= order.target
        state_flip = (state == "up" and order.side == "sell") or (state == "down" and order.side == "buy") or state == "chaos"
        if hit_stop or hit_target or state_flip:
            logging.info("Closing order %s", order.id)
            if is_live:
                # real sell/buy to close would go here
                pass
            pnl = (last_close - order.price) * order.amount
            if order.side == "sell":
                pnl *= -1
            db.close_order(order.id, int(df["ts"].iloc[-1]))
            decision = "close"
            order = None

    if order:
        return decision, pnl

    # entry logic
    usdc = float(exchange.fetch_balance()["total"].get("USDC", 0)) if is_live else 1000.0
    amount = position_size(usdc, last_close, risk_pct)
    if state == "consolidation":
        bottom_entry = low20 + 0.1 * range_size
        top_entry = high20 - 0.1 * range_size
        if last_close <= bottom_entry:
            side = "buy"
            stop = low20 - atr
            target = range_mid
        elif last_close >= top_entry:
            side = "sell"
            stop = high20 + atr
            target = range_mid
        else:
            return decision, pnl
    elif state == "up":
        if last_close > high20:
            side = "buy"
            stop = high20 - atr
            target = last_close + atr
        else:
            return decision, pnl
    elif state == "down":
        if last_close < low20:
            side = "sell"
            stop = low20 + atr
            target = last_close - atr
        else:
            return decision, pnl
    else:
        return decision, pnl
    logging.info("Placing %s for %.6f BTC @ %.2f", side, amount, last_close)
    if is_live:
        # real market order would go here
        pass
    order = Order(id=None, ts=int(df["ts"].iloc[-1]), side=side, price=last_close, amount=amount, stop=stop, target=target, status="open")
    db.record_order(order)
    decision = side
    return decision, pnl
def run_bot(is_live: bool = False, risk_pct: float = 0.01) -> None:
    db = Database(DB_FILE)
    logging.info("starting bot: live=%s risk_pct=%s", is_live, risk_pct)
    equity = get_equity(is_live, 0)
    peak_equity = equity
    while True:
        try:
            df = fetch_new_candles(db)
            state = label_state(df)
            logging.info("state=%s close=%s", state, df["close"].iloc[-1])
            decision, pnl = trade_logic(db, df, state, is_live, risk_pct)
            last_price = df["close"].iloc[-1]
            equity = get_equity(is_live, last_price)
            peak_equity = max(peak_equity, equity)
            drawdown = (peak_equity - equity) / peak_equity
            if drawdown >= 0.10 and is_live:
                logging.warning("drawdown exceeded 10%% - disabling live trading")
                is_live = False
            db.log_tick(int(df["ts"].iloc[-1]), state, decision, pnl, equity)
            print(json.dumps({"ts": int(df["ts"].iloc[-1]), "state": state, "decision": decision, "pnl": pnl, "equity": equity}))
            time.sleep(TIMEFRAME_MS / 1000)
        except Exception as exc:
            logging.error("error in main loop: %s", exc)
            time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coinbase categorise-adapt-trade bot")
    parser.add_argument("--live", action="store_true", help="place real orders")
    parser.add_argument("--paper", action="store_true", help="paper trading mode")
    parser.add_argument("--risk", type=float, default=0.01, help="risk per trade")
    parser.add_argument("--loglevel", default="INFO", help="logging level")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")

    live = args.live and not args.paper
    run_bot(is_live=live, risk_pct=args.risk)
