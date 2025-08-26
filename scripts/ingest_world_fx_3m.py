# scripts/ingest_world_fx_3m.py
# Fetch last ~95 days of world gold (GC=F) and USD/VND (VND=X) via yfinance.
import os, sqlite3, datetime as dt
from pathlib import Path
import pandas as pd
import yfinance as yf

DB_PATH = Path(os.environ.get("GOLD_DB", "data/gold.db"))

def yf_ohlcv(symbol: str, start: str, end: str) -> pd.DataFrame:
    """
    Download OHLCV and normalize to columns:
    date, open, high, low, close, volume (all lowercase; date as YYYY-MM-DD)
    """
    df = yf.download(
        symbol,
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=False,
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","open","high","low","close","volume"])

    # Flatten possible MultiIndex columns to simple strings
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else str(c) for c in df.columns]
    else:
        df.columns = [str(c) for c in df.columns]

    # Reset index -> have a Date column
    df = df.reset_index()

    # Some yfinance versions name it 'Date', ensure we have it
    date_col = "Date" if "Date" in df.columns else ("date" if "date" in df.columns else None)
    if date_col is None:
        # Fallback: try index as datetime
        df["date"] = pd.to_datetime(df.index).date.astype(str)
    else:
        df["date"] = pd.to_datetime(df[date_col]).dt.date.astype(str)

    # Ensure required columns exist, fill missing with NA
    for col in ["Open","High","Low","Close","Volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    out = df[["date","Open","High","Low","Close","Volume"]].rename(
        columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"}
    )
    # Coerce numeric
    for c in ["open","high","low","close","volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

def upsert_world(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty: return 0
    rows = [
        (r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"], "yfinance")
        for _, r in df.iterrows()
    ]
    sql = """
    INSERT INTO world_gold(date, open, high, low, close, volume, source)
    VALUES(?,?,?,?,?,?,?)
    ON CONFLICT(date) DO UPDATE SET
      open=excluded.open, high=excluded.high, low=excluded.low,
      close=excluded.close, volume=excluded.volume, source=excluded.source;
    """
    cur = conn.executemany(sql, rows)
    return cur.rowcount

def upsert_fx(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty: return 0
    rows = [(r["date"], r["close"], "yfinance") for _, r in df.iterrows()]
    sql = """
    INSERT INTO usd_vnd(date, rate, source)
    VALUES(?,?,?)
    ON CONFLICT(date) DO UPDATE SET
      rate=excluded.rate, source=excluded.source;
    """
    cur = conn.executemany(sql, rows)
    return cur.rowcount

def main():
    # Ensure schema exists
    from scripts.create_schema import main as ensure_schema
    ensure_schema()

    today = dt.date.today()
    start = today - dt.timedelta(days=365)  # buffer > 1 years
    end = (today + dt.timedelta(days=1))    # yfinance end is exclusive

    world = yf_ohlcv("GC=F", start.isoformat(), end.isoformat())
    fx    = yf_ohlcv("VND=X", start.isoformat(), end.isoformat())

    with sqlite3.connect(DB_PATH) as conn:
        n1 = upsert_world(conn, world)
        n2 = upsert_fx(conn, fx)
        conn.commit()

    print(f"✅ upsert world={n1}, fx={n2}")

if __name__ == "__main__":
    main()