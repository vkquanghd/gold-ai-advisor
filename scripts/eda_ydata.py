#!/usr/bin/env python3
"""
EDA with ydata-profiling (mỗi bảng -> 1 HTML cố định, ghi đè nếu chạy lại)

Usage:
  python -m scripts.eda_ydata --db data/gold.db --outdir reports/eda_ydata --days 365 --tables vn_gold,world_gold,usd_vnd
"""

import argparse
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

def _need_pkg_msg():
    print("[WARN] Missing package 'ydata-profiling'. Cài bằng:")
    print("       pip install ydata-profiling  # hoặc pandas-profiling (bản cũ)")
    print("       pip install setuptools       # nếu thiếu pkg_resources")

def read_table(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    try:
        return pd.read_sql(f"SELECT * FROM {table}", conn)
    except Exception as e:
        print(f"[WARN] Could not read table {table}: {e}")
        return pd.DataFrame()

def normalize_dates(df: pd.DataFrame, table: str) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df
    if table == "vn_gold":
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        elif "ts" in df.columns:
            df["date"] = pd.to_datetime(df["ts"], errors="coerce").dt.normalize()
        else:
            df["date"] = pd.NaT
    else:
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        else:
            df["date"] = pd.NaT
    return df

def filter_last_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df
    cutoff = pd.Timestamp(datetime.today().date()) - pd.Timedelta(days=days)
    return df[df["date"] >= cutoff].copy()

def ensure_outdir(outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)

def save_profile(df: pd.DataFrame, out_html: Path, title: str):
    # import chậm để bắt lỗi thân thiện
    try:
        from ydata_profiling import ProfileReport  # type: ignore
    except Exception:
        _need_pkg_msg()
        return False

    if df.empty:
        print(f"[INFO] Skip ydata for {title}: empty dataframe.")
        return False

    # ydata có thể lỗi với cột quá dài/chuỗi… có thể subset nếu cần
    profile = ProfileReport(df, title=title, explorative=True)
    profile.to_file(out_html)
    print(f"[OK] ydata report saved: {out_html}")
    return True

def main():
    p = argparse.ArgumentParser(description="EDA with ydata-profiling (overwrite latest html)")
    p.add_argument("--db", type=str, default="data/gold.db", help="SQLite DB path")
    p.add_argument("--outdir", type=str, default="reports/eda_ydata", help="Output directory")
    p.add_argument("--days", type=int, default=365, help="Lookback window (days)")
    p.add_argument("--tables", type=str, default="vn_gold,world_gold,usd_vnd", help="Comma-separated table names")
    args = p.parse_args()

    outdir = Path(args.outdir)
    ensure_outdir(outdir)

    if not Path(args.db).exists():
        print(f"[ERROR] DB not found: {args.db}")
        return

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    with sqlite3.connect(args.db) as conn:
        for t in tables:
            df = read_table(conn, t)
            df = normalize_dates(df, t)
            df = filter_last_days(df, args.days)

            # file HTML cố định (ghi đè nếu chạy lại)
            out_html = outdir / f"{t}_ydata.html"
            try:
                save_profile(df, out_html, title=f"{t} (last {args.days} days)")
            except Exception as e:
                print(f"[WARN] Failed ydata for {t}: {e}")

    print(f"[DONE] All ydata reports saved under: {outdir.resolve()}")

if __name__ == "__main__":
    main()