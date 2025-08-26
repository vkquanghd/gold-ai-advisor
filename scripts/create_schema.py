#!/usr/bin/env python3
"""
Create minimal SQLite schema for the project.

Tables
------
- world_gold : daily OHLCV of world gold (GC=F)
- usd_vnd    : daily USD/VND FX rate
- vn_gold    : local VN gold quotes (multiple rows/day by brand + ts)

Notes
-----
- No 'item', 'unit', or 'location' columns in vn_gold.
- Primary keys chosen for safe upsert patterns.
- Indices added for common filters.
"""

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get("GOLD_DB", "data/gold.db"))

DDL_STATEMENTS = [
    # World gold (daily)
    """
    CREATE TABLE IF NOT EXISTS world_gold(
        date   TEXT PRIMARY KEY,   -- YYYY-MM-DD
        open   REAL,
        high   REAL,
        low    REAL,
        close  REAL,
        volume REAL,
        source TEXT
    );
    """,

    # USD/VND (daily)
    """
    CREATE TABLE IF NOT EXISTS usd_vnd(
        date   TEXT PRIMARY KEY,   -- YYYY-MM-DD
        rate   REAL,               -- VND per USD
        source TEXT
    );
    """,

    # Vietnam local prices (potentially multiple rows per day)
    # Keep it minimal and consistent across crawlers/importers.
    """
    CREATE TABLE IF NOT EXISTS vn_gold(
        ts         TEXT,           -- ISO timestamp (e.g., 2025-08-23T16:15:00)
        date       TEXT,           -- YYYY-MM-DD (for fast filtering)
        brand      TEXT,           -- e.g., SJC | PNJ | DOJI
        buy_price  REAL,
        sell_price REAL,
        source     TEXT,
        PRIMARY KEY (brand, ts)
    );
    """,

    # Helpful indices
    "CREATE INDEX IF NOT EXISTS idx_vn_gold_date  ON vn_gold(date);",
    "CREATE INDEX IF NOT EXISTS idx_vn_gold_brand ON vn_gold(brand);",
]


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        cur = conn.cursor()
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
        conn.commit()

    print(f"✅ Schema ensured at {DB_PATH}")
    print("   - Tables: world_gold, usd_vnd, vn_gold")
    print("   - Indices: idx_vn_gold_date, idx_vn_gold_brand")


if __name__ == "__main__":
    main()