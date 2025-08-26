# scripts/import_vn_from_json.py
#!/usr/bin/env python3
"""
Import Vietnamese gold quotes from a JSON file into SQLite (schema without 'location').

JSON expected (from your crawler):
[
  {
    "date": "YYYY-MM-DD",
    "time": "HH:MM:SS",
    "timestamp": "YYYY-MM-DDTHH:MM:SS[+TZ]",
    "gold_type": "SJC|PNJ|DOJI|... (free text)",
    "buy_price": 12345678,
    "sell_price": 12399999,
    ... (extra fields ignored)
  },
  ...
]

Upsert key: (brand, ts)
- brand   := normalized from 'gold_type'
- ts      := ISO timestamp; falls back to f"{date}T{time or '00:00:00'}"
- date    := YYYY-MM-DD
- source  := --source (default 'cafef')

Pruning:
  --retention-days N + --prune → delete records older than cutoff date
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

DB_PATH = Path(os.environ.get("GOLD_DB", "data/gold.db"))

# ---------- schema helpers ----------
DDL = [
    """
    CREATE TABLE IF NOT EXISTS vn_gold(
        ts         TEXT,
        date       TEXT,
        brand      TEXT,
        buy_price  REAL,
        sell_price REAL,
        source     TEXT,
        PRIMARY KEY (brand, ts)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_vn_gold_date  ON vn_gold(date);",
    "CREATE INDEX IF NOT EXISTS idx_vn_gold_brand ON vn_gold(brand);",
]

def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()

# ---------- normalization ----------
def norm_brand(gold_type: str | None) -> str:
    if not gold_type:
        return "UNKNOWN"
    s = gold_type.strip().upper()
    if "SJC" in s:
        return "SJC"
    if "PNJ" in s:
        return "PNJ"
    if "DOJI" in s:
        return "DOJI"
    # fallbacks for common Vietnamese labels
    if "NHẪN" in s or "NHAN" in s:
        return "NHAN"   # generic ring gold
    return s[:24]  # cap length to avoid odd strings

def coalesce_ts(item: Dict[str, Any]) -> str:
    # Prefer ISO 'timestamp' if present and parseable; otherwise combine date + time
    ts = item.get("timestamp")
    if isinstance(ts, str) and ts:
        # normalize to no-tz string for stable PK
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    d = (item.get("date") or "").strip()
    t = (item.get("time") or "00:00:00").strip() or "00:00:00"
    if d:
        return f"{d}T{t}"
    # worst-case: now
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

def coalesce_date(item: Dict[str, Any]) -> str:
    d = (item.get("date") or "").strip()
    if d:
        return d
    ts = item.get("timestamp")
    if isinstance(ts, str) and ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return datetime.utcnow().strftime("%Y-%m-%d")

def to_float(x: Any) -> float | None:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

# ---------- IO ----------
def load_json(path: Path) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON root must be a list")
    return data

# ---------- upsert/prune ----------
def upsert(conn: sqlite3.Connection, items: Iterable[Dict[str, Any]], source: str) -> int:
    rows: List[Tuple] = []
    for it in items:
        brand = norm_brand(it.get("gold_type"))
        ts    = coalesce_ts(it)
        date  = coalesce_date(it)
        buy   = to_float(it.get("buy_price"))
        sell  = to_float(it.get("sell_price"))
        rows.append((brand, ts, date, buy, sell, source))

    cur = conn.executemany(
        """
        INSERT INTO vn_gold(brand, ts, date, buy_price, sell_price, source)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(brand, ts) DO UPDATE SET
            date=excluded.date,
            buy_price=excluded.buy_price,
            sell_price=excluded.sell_price,
            source=excluded.source
        """,
        rows,
    )
    conn.commit()
    return cur.rowcount or 0

def prune(conn: sqlite3.Connection, cutoff_date: str) -> None:
    conn.execute("DELETE FROM vn_gold WHERE date < ?", (cutoff_date,))
    conn.commit()

# ---------- main ----------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="Path to VN gold JSON file")
    ap.add_argument("--source", default="cafef", help="Source label to store")
    ap.add_argument("--retention-days", type=int, default=365, help="Keep only N days (default 365)")
    ap.add_argument("--prune", action="store_true", help="Enable pruning older rows")
    args = ap.parse_args()

    json_path = Path(args.json)
    if not json_path.exists():
        raise SystemExit(f"JSON not found: {json_path}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        ensure_schema(conn)
        data = load_json(json_path)
        n = upsert(conn, data, args.source)

        if args.prune:
            cutoff = (datetime.utcnow().date() - timedelta(days=args.retention_days)).strftime("%Y-%m-%d")
            prune(conn, cutoff)

    print(f"✅ imported {n} rows from {json_path} (source={args.source})"
          f"{' with prune' if args.prune else ''}")

if __name__ == "__main__":
    main()