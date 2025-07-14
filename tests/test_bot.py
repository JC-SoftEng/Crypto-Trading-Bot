import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import os
import pandas as pd
from bot import label_state, trade_logic, Database, compute_atr


def make_df(prices):
    bars = []
    ts = 0
    for open_, high, low, close in prices:
        bars.append([ts, open_, high, low, close, 1])
        ts += 60_000
    return pd.DataFrame(bars, columns=["ts", "open", "high", "low", "close", "volume"])


def test_label_consolidation():
    prices = [(100, 101, 99, 100)] * 21
    df = make_df(prices)
    assert label_state(df) == "consolidation"


def test_label_direction_up():
    prices = []
    for i in range(20):
        prices.append((100 + i, 101 + i, 99 + i, 100 + i))
    # last bar expands range and makes new high
    prices.append((120, 125, 118, 124))
    df = make_df(prices)
    assert label_state(df) == "up"


def test_trade_open_and_close(tmp_path):
    db_file = tmp_path / "test.db"
    db = Database(str(db_file))
    prices = []
    for i in range(20):
        prices.append((100 + i, 101 + i, 99 + i, 100 + i))
    prices.append((120, 125, 118, 124))
    df = make_df(prices)
    state = label_state(df)
    trade_logic(db, df, state, is_live=False, risk_pct=0.01)
    order = db.last_open_order()
    assert order is not None and order.side == "buy"
    expected_stop = df["high"].iloc[-21:-1].max() - compute_atr(df)
    assert abs(order.stop - expected_stop) < 1e-6

    # next bar triggers exit at stop
    stop_level = order.stop
    stop_bar = make_df([(99, 101, stop_level - 1, stop_level - 1)])
    df = pd.concat([df, stop_bar])
    state = "chaos"
    trade_logic(db, df, state, is_live=False, risk_pct=0.01)
    assert db.last_open_order() is None
