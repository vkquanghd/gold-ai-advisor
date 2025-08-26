#!/usr/bin/env python3
"""
scripts/daily_update.py

One-shot daily update with strict retention:
- Ingest world gold (GC=F) + USD/VND (yfinance)
- Crawl VN quotes (CafeF) -> data/vn_raw.json
- Import VN into SQLite
- Prune with FIFO to keep only last N DISTINCT dates per table
- Archive deleted rows to CSV under data/archive/<table>/...

Usage examples:
  python -m scripts.daily_update --retention-days 365 --world --vn
  python -m scripts.daily_update --world
  python -m scripts.daily_update --vn
"""

import argparse
import os
import sys
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Iterable, Dict
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
LOG_FILE     = DATA_DIR / "daily_update.log"
ARCHIVE_DIR  = DATA_DIR / "archive"
JSON_PATH    = DATA_DIR / "vn_raw.json"
DB_PATH      = DATA_DIR / "gold.db"


# ---------------- Utilities ----------------
def run_mod(mod: str, args: List[str]) -> Tuple[int, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, "-m", mod] + args
    try:
        out = subprocess.check_output(
            cmd, cwd=str(PROJECT_ROOT), env=env,
            stderr=subprocess.STDOUT, text=True
        )
        return 0, out
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output

def append_log(text: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")

def now_tag() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ts_for_file() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------- Prune with archive ----------------
def ensure_archive_dirs() -> None:
    (ARCHIVE_DIR / "world_gold").mkdir(parents=True, exist_ok=True)
    (ARCHIVE_DIR / "usd_vnd").mkdir(parents=True, exist_ok=True)
    (ARCHIVE_DIR / "vn_gold").mkdir(parents=True, exist_ok=True)

def fetch_all(conn: sqlite3.Connection, sql: str, params: Iterable = ()) -> List[Dict]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def archive_rows(rows: List[Dict], table: str) -> int:
    """Append rows to a new timestamped CSV under data/archive/<table>/"""
    if not rows:
        return 0
    outdir = ARCHIVE_DIR / table
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{table}_deleted_{ts_for_file()}.csv"
    with outpath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    append_log(f"[{now_tag()}] archived {len(rows)} rows -> {outpath}")
    return len(rows)

from datetime import datetime, timedelta

def forward_fill_table(
    conn: sqlite3.Connection,
    table: str,
    num_cols: list,              # các cột số cần copy (ví dụ: ["open","high","low","close","volume"] hoặc ["rate"])
    date_col: str = "date",
    source_col: str = "source",
    keep_days: int = 365
) -> int:
    """
    Chèn các ngày còn thiếu trong cửa sổ keep_days cuối cùng,
    dùng giá trị của ngày gần nhất trước đó. Trả về số hàng đã chèn.
    """
    cur = conn.cursor()
    max_date = cur.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()[0]
    if not max_date:
        return 0

    end = datetime.strptime(max_date, "%Y-%m-%d").date()
    start = end - timedelta(days=keep_days - 1)
    start_str = start.strftime("%Y-%m-%d")

    # Lấy dữ liệu sẵn có trong cửa sổ
    cols_sel = [date_col] + num_cols + [source_col]
    rows = cur.execute(
        f"SELECT {', '.join(cols_sel)} FROM {table} WHERE {date_col} >= ? ORDER BY {date_col} ASC",
        (start_str,)
    ).fetchall()

    # Tập ngày đã có
    existing_by_date = {r[0]: r for r in rows}
    # Seed: bản ghi cuối cùng trước start (để ffill ngay từ đầu cửa sổ nếu thiếu)
    seed = cur.execute(
        f"SELECT {', '.join(num_cols + [source_col])} FROM {table} WHERE {date_col} < ? ORDER BY {date_col} DESC LIMIT 1",
        (start_str,)
    ).fetchone()

    last_vals = None
    if seed:
        # seed = (num_cols..., source)
        last_vals = list(seed)

    inserted = 0
    d = start
    while d <= end:
        ds = d.strftime("%Y-%m-%d")
        if ds in existing_by_date:
            # cập nhật "last_vals" theo bản ghi thực có trong DB
            row = existing_by_date[ds]
            # row = (date, *num_cols, source)
            last_vals = list(row[1:])  # giữ lại num_cols + source
        else:
            # thiếu ngày -> nếu có last_vals thì chèn
            if last_vals:
                prev_source = last_vals[-1] or ""
                new_source = (prev_source + "+ffill").lstrip("+")
                num_values = last_vals[:-1]  # các giá trị số từ ngày trước
                placeholders = ", ".join(["?"] * (1 + len(num_cols) + 1))
                cur.execute(
                    f"INSERT OR IGNORE INTO {table} ({date_col}, {', '.join(num_cols)}, {source_col}) VALUES ({placeholders})",
                    [ds] + num_values + [new_source]
                )
                inserted += cur.rowcount if hasattr(cur, "rowcount") else 1
        d += timedelta(days=1)

    conn.commit()
    return inserted

from datetime import date, timedelta

def forward_fill_vn_gold(conn: sqlite3.Connection, keep_days: int) -> int:
    cur = conn.cursor()
    max_date = cur.execute("SELECT MAX(date) FROM vn_gold").fetchone()[0]
    if not max_date:
        return 0

    end = date.fromisoformat(max_date)
    start = end - timedelta(days=keep_days - 1)
    start_str, end_str = start.isoformat(), end.isoformat()

    brands = [r[0] for r in cur.execute("SELECT DISTINCT brand FROM vn_gold ORDER BY brand")]

    inserted = 0
    for i, b in enumerate(brands):
        # seed: lấy cả bản ghi đúng ngày start nếu có
        seed = cur.execute("""
          SELECT buy_price, sell_price, source
          FROM vn_gold
          WHERE brand=? AND date <= ?
          ORDER BY date DESC, ts DESC
          LIMIT 1
        """, (b, start_str)).fetchone()
        last_vals = list(seed) if seed else None

        # Lấy bản ghi cuối cùng mỗi ngày trong cửa sổ
        rows = cur.execute("""
          SELECT v.date, v.buy_price, v.sell_price, v.source
          FROM vn_gold v
          JOIN (
            SELECT date, MAX(ts) AS ts
            FROM vn_gold
            WHERE brand=? AND date BETWEEN ? AND ?
            GROUP BY date
          ) t ON v.brand=? AND v.date=t.date AND v.ts=t.ts
          ORDER BY v.date
        """, (b, start_str, end_str, b)).fetchall()
        by_date = {r[0]: (r[1], r[2], r[3]) for r in rows}

        d = start
        while d <= end:
            ds = d.isoformat()
            if ds in by_date:
                last_vals = list(by_date[ds])  # cập nhật theo dữ liệu thật của ngày đó
            else:
                if last_vals:
                    prev_src = last_vals[2] or ""
                    new_src = (prev_src + "+ffill").lstrip("+")
                    # ts khác nhau cho mỗi brand để không đụng UNIQUE(ts)
                    ts = f"{ds}T12:00:{i:02d}"
                    cur.execute("""
                      INSERT OR IGNORE INTO vn_gold (ts, date, brand, buy_price, sell_price, source)
                      VALUES (?, ?, ?, ?, ?, ?)
                    """, (ts, ds, b, last_vals[0], last_vals[1], new_src))
                    inserted += cur.rowcount if hasattr(cur, "rowcount") else 1
            d += timedelta(days=1)

    conn.commit()
    return inserted

def prune_table_keep_last_n_days(
    conn: sqlite3.Connection,
    table: str,
    date_col: str,
    keep_days: int
) -> int:
    """
    Keep only last `keep_days` DISTINCT dates. Delete everything older (date < cutoff_keep),
    archiving deleted rows to CSV. Returns number of deleted rows.
    """
    # Get N newest distinct dates (DESC), then find the minimum among them
    # to compute the keep cutoff.
    distinct_dates = fetch_all(
        conn,
        f"SELECT DISTINCT {date_col} AS d FROM {table} WHERE {date_col} IS NOT NULL ORDER BY d DESC LIMIT ?",
        [keep_days]
    )
    if not distinct_dates:
        return 0
    # The cutoff_keep is the smallest date among the kept set -> everything older will be removed.
    cutoff_keep = min([row["d"] for row in distinct_dates if row["d"]])

    # Load rows to be deleted for archive (strictly older)
    to_delete = fetch_all(
        conn,
        f"SELECT * FROM {table} WHERE {date_col} < ?",
        [cutoff_keep]
    )
    if not to_delete:
        return 0

    # Archive then delete
    archived = archive_rows(to_delete, table)
    conn.execute(f"DELETE FROM {table} WHERE {date_col} < ?", [cutoff_keep])
    conn.commit()
    return archived  # number of rows affected (same as archived)


# ---------------- Main Orchestration ----------------
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--retention-days", type=int, default=365, help="Keep last N DISTINCT dates (default 365)")
    p.add_argument("--world", action="store_true", help="Run world+FX ingest (yfinance)")
    p.add_argument("--vn", action="store_true", help="Run VN crawl+import")
    p.add_argument("--crawler-days", type=int, help="Override VN crawler window (default = retention)")
    p.add_argument("--basename", default="vn_raw", help="JSON basename for VN crawl (default vn_raw)")
    p.add_argument("--outdir", default=str(DATA_DIR), help="JSON output dir (default data/)")
    args = p.parse_args()

    if not args.world and not args.vn:
        print("Nothing selected. Use --world and/or --vn.", flush=True)
        return 0

    ensure_archive_dirs()

    append_log("=" * 60)
    append_log(f"[{now_tag()}] daily_update start (retention={args.retention_days})")
    rc = 0
    combined_out: List[str] = []

    # 1) WORLD + FX
    if args.world:
        mod_args = ["--days", str(args.retention_days)]
        code, out = run_mod("scripts.ingest_world_fx_3m", mod_args)
        combined_out.append(f"$ python -m scripts.ingest_world_fx_3m {' '.join(mod_args)}\n{out}")
        append_log(combined_out[-1])
        if code != 0:
            rc = code

    # 2) VN CRAWL + IMPORT
    if args.vn:
        days = args.crawler_days or args.retention_days
        crawl_cmd = ["--outdir", args.outdir, "--basename", args.basename, "--days", str(days)]
        code1, out1 = run_mod("scripts.vendors.gold_price_focused_crawler", crawl_cmd)
        combined_out.append(f"$ python -m scripts.vendors.gold_price_focused_crawler {' '.join(crawl_cmd)}\n{out1}")
        append_log(combined_out[-1])
        if code1 != 0 and rc == 0:
            rc = code1

        # import VN JSON if exists
        json_path = Path(args.outdir) / f"{args.basename}.json"
        if json_path.exists():
            imp_cmd = ["--json", str(json_path), "--source", "cafef", "--retention-days", str(args.retention_days), "--prune"]
            code2, out2 = run_mod("scripts.import_vn_from_json", imp_cmd)
            combined_out.append(f"$ python -m scripts.import_vn_from_json {' '.join(imp_cmd)}\n{out2}")
            append_log(combined_out[-1])
            if code2 != 0 and rc == 0:
                rc = code2
        else:
            miss = f"Missing JSON: {json_path}"
            combined_out.append(miss)
            append_log(miss)
            if rc == 0:
                rc = 2

    # 3) PRUNE WITH FIFO (and archive deletions)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row

            # 3a) Forward-fill trước khi prune
            f1 = forward_fill_table(conn, "world_gold", ["open","high","low","close","volume"], keep_days=args.retention_days)
            f2 = forward_fill_table(conn, "usd_vnd",    ["rate"], keep_days=args.retention_days)
            append_log(f"[{now_tag()}] forward-fill done (world_gold={f1}, usd_vnd={f2})")
            # Forward-fill: thêm ngày trống cho vn_gold
            ff_vn = forward_fill_vn_gold(conn, keep_days=args.retention_days)
            append_log(f"[{now_tag()}] forward-fill vn_gold={ff_vn}")

            # 3b) PRUNE + ARCHIVE
            deleted = 0
            deleted += prune_table_keep_last_n_days(conn, "world_gold", "date", args.retention_days)
            deleted += prune_table_keep_last_n_days(conn, "usd_vnd",    "date", args.retention_days)
            deleted += prune_table_keep_last_n_days(conn, "vn_gold",     "date", args.retention_days)
            append_log(f"[{now_tag()}] prune+archive done (deleted={deleted})")
            combined_out.append(f"Prune+archive done. Deleted rows total: {deleted}")
            
    except Exception as e:
        err = f"Prune/Archive failed: {e}"
        append_log(err)
        combined_out.append(err)
        if rc == 0:
            rc = 3

    append_log(f"[{now_tag()}] daily_update end (exit={rc})")
    print("\n\n".join(combined_out))
    return rc


if __name__ == "__main__":
    sys.exit(main())
