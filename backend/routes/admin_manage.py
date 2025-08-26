#!/usr/bin/env python3
from flask import Blueprint, request, render_template, redirect, url_for
from datetime import datetime, date
from pathlib import Path
import csv
from typing import Any, Dict, List, Tuple

from backend.db import get_conn, query

manage_bp = Blueprint("manage", __name__)

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- helpers ----------
def _today_str() -> str:
    return date.today().isoformat()

def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _export_rows_to_csv(table: str, rows: List[Dict[str, Any]]) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = DATA_DIR / f"deleted_{table}_{ts}.csv"
    if not rows:
        return str(out)

    # write headers from keys of first row
    headers = list(rows[0].keys())
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return str(out)

def _list_brands(limit: int = 30) -> List[str]:
    sql = "SELECT brand, COUNT(*) as n FROM vn_gold GROUP BY brand ORDER BY n DESC LIMIT ?;"
    with get_conn(True) as conn:
        rows = query(conn, sql, [limit])
    return [r["brand"] for r in rows if r.get("brand")]

# ---------- pages ----------
@manage_bp.get("/")
def manage_index():
    # default: show today
    return redirect(url_for("manage.manage_form", start=_today_str(), end=_today_str()))

@manage_bp.get("/form")
def manage_form():
    """
    Main management page: insert VN price and delete data by filters.
    """
    start = request.args.get("start") or _today_str()
    end   = request.args.get("end") or _today_str()
    brand = (request.args.get("brand") or "").upper()
    info  = request.args.get("info", "")
    error = request.args.get("error", "")

    return render_template(
        "admin_manage.html",
        title="Daily Data Management",
        start=start,
        end=end,
        brand=brand,
        brands=_list_brands(),
        info=info,
        error=error
    )

# ---------- actions ----------
@manage_bp.post("/insert_vn")
def insert_vn():
    """
    Insert a single VN gold quote row.
    Required form fields: date, brand, buy_price, sell_price
    Optional: ts (defaults now), source (defaults 'admin')
    """
    form = request.form
    date_str   = form.get("date") or _today_str()
    brand      = (form.get("brand") or "").upper().strip()
    buy_price  = form.get("buy_price")
    sell_price = form.get("sell_price")
    ts         = form.get("ts") or _now_iso()
    source     = form.get("source") or "admin"

    if not brand or not buy_price or not sell_price:
        return redirect(url_for("manage.manage_form", start=date_str, end=date_str,
                                error="Missing required fields (brand, buy_price, sell_price)."))

    try:
        buy = float(buy_price)
        sell = float(sell_price)
    except ValueError:
        return redirect(url_for("manage.manage_form", start=date_str, end=date_str,
                                error="buy_price/sell_price must be numeric."))

    # Insert row
    sql = """
    INSERT INTO vn_gold(ts, date, brand, buy_price, sell_price, source)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    with get_conn(False) as conn:
        conn.execute(sql, [ts, date_str, brand, buy, sell, source])
        conn.commit()

        # fetch back the inserted row to show
        rows = query(conn, """
            SELECT date, ts, brand, buy_price, sell_price, source
            FROM vn_gold WHERE ts = ? AND brand = ? LIMIT 1;
        """, [ts, brand])

    info = f"Inserted 1 row for {brand} on {date_str}."
    return render_template(
        "admin_manage.html",
        title="Daily Data Management",
        start=date_str, end=date_str, brand=brand,
        brands=_list_brands(),
        info=info,
        error="",
        inserted_rows=rows
    )

@manage_bp.post("/delete")
def delete_rows():
    """
    Delete data by filters and export deleted rows to CSV before deletion.
    Form fields:
     - table: one of ['vn_gold','world_gold','usd_vnd'] (required)
     - start, end: date range (required)
     - brand: only for vn_gold (optional)
    """
    form  = request.form
    table = form.get("table") or ""
    start = form.get("start") or _today_str()
    end   = form.get("end") or _today_str()
    brand = (form.get("brand") or "").upper().strip()

    valid_tables = {"vn_gold", "world_gold", "usd_vnd"}
    if table not in valid_tables:
        return redirect(url_for("manage.manage_form", start=start, end=end,
                                error="Invalid table. Choose vn_gold/world_gold/usd_vnd."))

    with get_conn(False) as conn:
        # 1) Select rows to be deleted
        params: List[Any] = [start, end]
        if table == "vn_gold" and brand:
            sql_sel = """
              SELECT ts, date, brand, buy_price, sell_price, source
              FROM vn_gold
              WHERE date BETWEEN ? AND ? AND brand = ?
              ORDER BY date DESC, ts DESC
            """
            params.append(brand)
        elif table == "vn_gold":
            sql_sel = """
              SELECT ts, date, brand, buy_price, sell_price, source
              FROM vn_gold
              WHERE date BETWEEN ? AND ?
              ORDER BY date DESC, ts DESC
            """
        elif table == "world_gold":
            sql_sel = """
              SELECT date, open, high, low, close, volume, source
              FROM world_gold
              WHERE date BETWEEN ? AND ?
              ORDER BY date DESC
            """
        else:  # usd_vnd
            sql_sel = """
              SELECT date, rate, source
              FROM usd_vnd
              WHERE date BETWEEN ? AND ?
              ORDER BY date DESC
            """

        rows = query(conn, sql_sel, params)

        # 2) Export to CSV
        csv_path = _export_rows_to_csv(table, rows)

        # 3) Delete
        if table == "vn_gold" and brand:
            sql_del = "DELETE FROM vn_gold WHERE date BETWEEN ? AND ? AND brand = ?"
            del_params = [start, end, brand]
        elif table == "vn_gold":
            sql_del = "DELETE FROM vn_gold WHERE date BETWEEN ? AND ?"
            del_params = [start, end]
        elif table == "world_gold":
            sql_del = "DELETE FROM world_gold WHERE date BETWEEN ? AND ?"
            del_params = [start, end]
        else:
            sql_del = "DELETE FROM usd_vnd WHERE date BETWEEN ? AND ?"
            del_params = [start, end]

        cur = conn.execute(sql_del, del_params)
        conn.commit()
        deleted = cur.rowcount or 0

    info = f"Exported {len(rows)} rows to {csv_path}. Deleted {deleted} rows from {table}."
    return render_template(
        "admin_manage.html",
        title="Daily Data Management",
        start=start, end=end, brand=brand,
        brands=_list_brands(),
        info=info,
        error="",
        preview_rows=rows,
        table=table
    )