# backend/db.py
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Sequence

DB_PATH = Path("data/gold.db")

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # <-- KEY: rows behave like dicts
    return conn

def query(sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None) -> List[sqlite3.Row]:
    with _connect() as conn:
        cur = conn.execute(sql, params or {})
        return cur.fetchall()

def query_dicts(sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None) -> List[dict]:
    rows = query(sql, params)
    return [dict(r) for r in rows]

def get_min_max_rows(table: str, date_col: str = "date") -> tuple[str | None, str | None, int]:
    sql = f"SELECT MIN({date_col}) AS min_d, MAX({date_col}) AS max_d, COUNT(*) AS n FROM {table}"
    rows = query(sql)
    if not rows:
        return None, None, 0
    r = rows[0]
    return (r["min_d"], r["max_d"], int(r["n"] or 0))