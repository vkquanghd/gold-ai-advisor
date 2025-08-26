# backend/dash_app/data.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List
import pandas as pd

from backend.db import get_min_max_rows, query_dicts

@dataclass
class TableStats:
    min_date: Optional[pd.Timestamp]
    max_date: Optional[pd.Timestamp]
    n_rows: int

def get_table_stats(table: str, date_col: str = "date") -> TableStats:
    """
    Read min(date), max(date), count(*) from SQLite for a table.
    """
    min_d, max_d, n = get_min_max_rows(table, date_col)
    md = pd.to_datetime(min_d) if min_d else None
    xd = pd.to_datetime(max_d) if max_d else None
    return TableStats(md, xd, n)

def list_brands() -> List[str]:
    rows = query_dicts("SELECT DISTINCT brand FROM vn_gold ORDER BY brand;")
    return [r["brand"] for r in rows if r.get("brand")]

def df_vn(start: Optional[str], end: Optional[str], brand: Optional[str]) -> pd.DataFrame:
    where = []
    params = {}
    if start:
        where.append("date >= :start")
        params["start"] = start
    if end:
        where.append("date <= :end")
        params["end"] = end
    if brand:
        where.append("brand = :brand")
        params["brand"] = brand
    wsql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
      SELECT date, brand, buy_price, sell_price, ts
      FROM vn_gold
      {wsql}
      ORDER BY date ASC, ts ASC
    """
    return pd.DataFrame(query_dicts(sql, params))

def df_world(start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    where = []
    params = {}
    if start:
        where.append("date >= :start")
        params["start"] = start
    if end:
        where.append("date <= :end")
        params["end"] = end
    wsql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
      SELECT date, open, high, low, close
      FROM world_gold
      {wsql}
      ORDER BY date ASC
    """
    return pd.DataFrame(query_dicts(sql, params))

def df_fx(start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    where = []
    params = {}
    if start:
        where.append("date >= :start")
        params["start"] = start
    if end:
        where.append("date <= :end")
        params["end"] = end
    wsql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
      SELECT date, rate
      FROM usd_vnd
      {wsql}
      ORDER BY date ASC
    """
    return pd.DataFrame(query_dicts(sql, params))