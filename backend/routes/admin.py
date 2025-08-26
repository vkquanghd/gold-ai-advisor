# backend/routes/admin.py
# Clean version — no stray "from __future__" and endpoints are unique

from math import ceil
from typing import Iterable, Sequence, Any
import os, sys, subprocess, time
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    jsonify, flash
)

from backend.db import query, get_min_max_rows

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

PAGE_SIZE = 50


# ----------------------------
# Helpers
# ----------------------------
def _int_arg(name: str, default: int) -> int:
    try:
        return max(1, int(request.args.get(name, default)))
    except Exception:
        return default


def _page_arg(default: int = 1) -> int:
    try:
        p = int(request.args.get("page") or default)
        return max(1, p)
    except Exception:
        return default


def _sort_args(default_col: str, default_dir: str = "desc") -> tuple[str, str]:
    col = (request.args.get("sort") or default_col).strip()
    direction = (request.args.get("dir") or default_dir).lower()
    if direction not in ("asc", "desc"):
        direction = default_dir
    return col, direction


def _like(q: str) -> str:
    # VERY basic escaping for LIKE
    return q.replace("%", r"[%]").replace("_", r"[_]")


def _rows_to_dicts(columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({columns[i]: r[i] if i < len(r) else None for i in range(len(columns))})
    return out


# ----------------------------
# Index
# ----------------------------
@admin_bp.route("/", endpoint="index")
def admin_index():
    # redirect to default VN page
    return redirect(url_for("admin.admin_vn"))


# ----------------------------
# VN GOLD
# ----------------------------
@admin_bp.get("/vn", endpoint="admin_vn")
def admin_vn():
    # filters
    brand = (request.args.get("brand") or "").strip()
    start = (request.args.get("start") or "").strip()
    end = (request.args.get("end") or "").strip()
    search = (request.args.get("search") or "").strip()

    # sorting and paging
    sort_key = (request.args.get("sort") or "date_desc").strip()
    page = _int_arg("page", 1)

    # brands for dropdown
    brands_rows = query("SELECT DISTINCT brand FROM vn_gold ORDER BY brand")
    brands = [r[0] for r in brands_rows if r and r[0]]

    # where
    wh = []
    params: list[Any] = []
    if brand:
        wh.append("brand = ?")
        params.append(brand)
    if start:
        wh.append("date >= ?")
        params.append(start)
    if end:
        wh.append("date <= ?")
        params.append(end)
    if search:
        wh.append("(brand LIKE ? OR source LIKE ?)")
        kw = f"%{_like(search)}%"
        params.extend([kw, kw])

    where_sql = f"WHERE {' AND '.join(wh)}" if wh else ""

    # count
    count_sql = f"SELECT COUNT(*) FROM vn_gold {where_sql}"
    count_rows = query(count_sql, tuple(params))
    total_rows = int(count_rows[0][0]) if count_rows else 0
    total_pages = max(1, ceil(total_rows / PAGE_SIZE))
    if page > total_pages:
        page = total_pages

    # order
    order_map = {
        "date_desc": "date DESC, ts DESC",
        "date_asc": "date ASC, ts ASC",
        "buy_asc": "buy_price ASC",
        "buy_desc": "buy_price DESC",
        "sell_asc": "sell_price ASC",
        "sell_desc": "sell_price DESC",
        "brand_asc": "brand ASC",
        "brand_desc": "brand DESC",
        "source_asc": "source ASC",
        "source_desc": "source DESC",
    }
    order_sql = order_map.get(sort_key, "date DESC, ts DESC")

    # page slice
    offset = (page - 1) * PAGE_SIZE
    data_sql = f"""
        SELECT date, brand, buy_price, sell_price, ts, source
        FROM vn_gold
        {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    data_rows = query(data_sql, tuple(params + [PAGE_SIZE, offset]))
    columns = ["date", "brand", "buy_price", "sell_price", "ts", "source"]
    rows = _rows_to_dicts(columns, data_rows)

    # overall coverage (entire table)
    minmax = query("SELECT MIN(date), MAX(date), COUNT(*) FROM vn_gold")[0]
    data_range = {"table": "vn_gold", "min_date": minmax[0], "max_date": minmax[1], "total_rows": int(minmax[2] or 0)}

    # headers for the template's select
    headers = [
        ("date_desc", "Date ↓"), ("date_asc", "Date ↑"),
        ("buy_desc", "Buy ↓"), ("buy_asc", "Buy ↑"),
        ("sell_desc", "Sell ↓"), ("sell_asc", "Sell ↑"),
        ("brand_asc", "Brand ↑"), ("brand_desc", "Brand ↓"),
        ("source_asc", "Source ↑"), ("source_desc", "Source ↓"),
    ]

    return render_template(
        "admin_table.html",
        title="VN Gold",
        nav="vn",
        rows=rows,
        page=page,
        total_pages=total_pages,
        page_size=PAGE_SIZE,
        total_rows=total_rows,
        # filters
        brands=brands,
        selected_brand=brand,
        date_from=start,
        date_to=end,
        search=search,
        # sorting
        headers=headers,
        sort=sort_key,
        order=("desc" if sort_key.endswith("desc") else "asc"),
        # coverage badge
        data_range=data_range,
        # columns for header rendering (key,label list)
        table="vn_gold",
        columns=[("date", "Date"), ("brand", "Brand"), ("buy_price", "Buy"), ("sell_price", "Sell"), ("ts", "Timestamp"), ("source", "Source")],
        help_text="Vietnam local brands (SJC, PNJ, DOJI…). 50 rows per page; filter by brand, date range, and search.",
    )


# ----------------------------
# WORLD GOLD
# ----------------------------
@admin_bp.get("/world", endpoint="admin_world")
def admin_world():
    page = _page_arg()
    sort_col, sort_dir = _sort_args(default_col="date", default_dir="desc")
    q = (request.args.get("search") or "").strip()

    min_d, max_d, total_rows_all = get_min_max_rows("world_gold", "date")

    where = []
    params: list[Any] = []
    if q:
        where.append("(source LIKE ?)")
        params.append(f"%{_like(q)}%")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT COUNT(*) FROM world_gold {where_sql}"
    total_rows = int(query(count_sql, tuple(params))[0][0])

    total_pages = max(1, ceil(total_rows / PAGE_SIZE))
    page = min(page, total_pages)
    offset = (page - 1) * PAGE_SIZE

    sortable = {"date", "open", "high", "low", "close", "volume", "source"}
    if sort_col not in sortable:
        sort_col = "date"
    order_sql = f"ORDER BY {sort_col} {sort_dir}"

    sql = f"""
        SELECT date, open, high, low, close, volume, source
        FROM world_gold
        {where_sql}
        {order_sql}
        LIMIT ? OFFSET ?
    """
    data_rows = query(sql, tuple(params + [PAGE_SIZE, offset]))
    columns = ["date", "open", "high", "low", "close", "volume", "source"]
    rows = _rows_to_dicts(columns, data_rows)

    headers = [
        ("date_desc", "Date ↓"), ("date_asc", "Date ↑"),
        ("open_desc", "Open ↓"), ("open_asc", "Open ↑"),
        ("high_desc", "High ↓"), ("high_asc", "High ↑"),
        ("low_desc", "Low ↓"), ("low_asc", "Low ↑"),
        ("close_desc", "Close ↓"), ("close_asc", "Close ↑"),
        ("volume_desc", "Volume ↓"), ("volume_asc", "Volume ↑"),
        ("source_asc", "Source ↑"), ("source_desc", "Source ↓"),
    ]
    # Map select value (e.g., "close_desc") to SQL col & dir
    sort_key = request.args.get("sort") or f"{sort_col}_{sort_dir}"
    if "_" in sort_key:
        col, dir_ = sort_key.rsplit("_", 1)
        sort_col, sort_dir = col, ("desc" if dir_ == "desc" else "asc")

    return render_template(
        "admin_table.html",
        title="World Gold",
        nav="world",
        rows=rows,
        page=page,
        total_pages=total_pages,
        page_size=PAGE_SIZE,
        total_rows=total_rows,
        # filters
        brands=None,
        selected_brand="",
        date_from="",
        date_to="",
        search=q,
        # sorting
        headers=headers,
        sort=sort_key,
        order=("desc" if sort_key.endswith("desc") else "asc"),
        # coverage
        data_range={"table": "world_gold", "min_date": min_d, "max_date": max_d, "total_rows": total_rows_all},
        # columns to render in table head/body
        table="world_gold",
        columns=[("date", "Date"), ("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close"), ("volume", "Volume"), ("source", "Source")],
        help_text="World gold futures (GC=F) daily OHLCV.",
    )


# ----------------------------
# USD/VND FX
# ----------------------------
@admin_bp.get("/fx", endpoint="admin_fx")
def admin_fx():
    page = _page_arg()
    sort_col, sort_dir = _sort_args(default_col="date", default_dir="desc")
    q = (request.args.get("search") or "").strip()

    min_d, max_d, total_rows_all = get_min_max_rows("usd_vnd", "date")

    where = []
    params: list[Any] = []
    if q:
        where.append("(source LIKE ?)")
        params.append(f"%{_like(q)}%")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT COUNT(*) FROM usd_vnd {where_sql}"
    total_rows = int(query(count_sql, tuple(params))[0][0])

    total_pages = max(1, ceil(total_rows / PAGE_SIZE))
    page = min(page, total_pages)
    offset = (page - 1) * PAGE_SIZE

    sortable = {"date", "rate", "source"}
    if sort_col not in sortable:
        sort_col = "date"
    order_sql = f"ORDER BY {sort_col} {sort_dir}"

    sql = f"""
        SELECT date, rate, source
        FROM usd_vnd
        {where_sql}
        {order_sql}
        LIMIT ? OFFSET ?
    """
    data_rows = query(sql, tuple(params + [PAGE_SIZE, offset]))
    columns = ["date", "rate", "source"]
    rows = _rows_to_dicts(columns, data_rows)

    headers = [
        ("date_desc", "Date ↓"), ("date_asc", "Date ↑"),
        ("rate_desc", "Rate ↓"), ("rate_asc", "Rate ↑"),
        ("source_asc", "Source ↑"), ("source_desc", "Source ↓"),
    ]
    sort_key = request.args.get("sort") or f"{sort_col}_{sort_dir}"
    if "_" in sort_key:
        col, dir_ = sort_key.rsplit("_", 1)
        sort_col, sort_dir = col, ("desc" if dir_ == "desc" else "asc")

    return render_template(
        "admin_table.html",
        title="USD/VND",
        nav="fx",
        rows=rows,
        page=page,
        total_pages=total_pages,
        page_size=PAGE_SIZE,
        total_rows=total_rows,
        # filters
        brands=None,
        selected_brand="",
        date_from="",
        date_to="",
        search=q,
        # sorting
        headers=headers,
        sort=sort_key,
        order=("desc" if sort_key.endswith("desc") else "asc"),
        # coverage
        data_range={"table": "usd_vnd", "min_date": min_d, "max_date": max_d, "total_rows": total_rows_all},
        # columns to render
        table="usd_vnd",
        columns=[("date", "Date"), ("rate", "Rate"), ("source", "Source")],
        help_text="USD/VND daily rate.",
    )


# ----------------------------
# Daily Update pages
# ----------------------------
@admin_bp.get("/update", endpoint="update_page")
def update_page():
    row = query("SELECT MIN(date), MAX(date), COUNT(*) FROM vn_gold")[0]
    range_info = {"min_date": row[0], "max_date": row[1], "total_rows": int(row[2] or 0)}
    return render_template(
        "update.html",
        title="Daily Update",
        nav="update",
        range_info=range_info,
        data_range={"table": "vn_gold", **range_info},
    )


@admin_bp.post("/update/run", endpoint="update_run")
def update_run():
    """
    Run daily update (world + vn) and return before/after coverage + logs.
    """
    # BEFORE coverage
    b = query("SELECT MIN(date), MAX(date), COUNT(*) FROM vn_gold")[0]
    before = {"min_date": b[0], "max_date": b[1], "total_rows": int(b[2] or 0)}

    # Run your pipeline with the interpreter of this process (venv-safe)
    cmd = [
        sys.executable, "-m", "scripts.daily_update",
        "--world", "--vn", "--retention-days", "365",
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            capture_output=True,
            text=True,
            check=True,
            timeout=600,   # 10 minutes; adjust as you like
        )
        ok = True
        msg = "Daily update finished."
        stdout, stderr = proc.stdout, proc.stderr
    except subprocess.CalledProcessError as e:
        ok = False
        msg = "Daily update failed."
        stdout, stderr = e.stdout, e.stderr
    except subprocess.TimeoutExpired as e:
        ok = False
        msg = "Daily update timed out."
        stdout, stderr = e.stdout or "", e.stderr or ""

    # AFTER coverage
    a = query("SELECT MIN(date), MAX(date), COUNT(*) FROM vn_gold")[0]
    after = {"min_date": a[0], "max_date": a[1], "total_rows": int(a[2] or 0)}

    # Return logs so the front-end can display details if needed
    return jsonify(
        ok=ok,
        message=msg,
        range_before=before,
        range_after=after,
        stdout=stdout,
        stderr=stderr,
        ts=time.time(),                 # version signal for the UI
    )