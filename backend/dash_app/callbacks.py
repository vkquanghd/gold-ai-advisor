# backend/dash_app/callbacks.py
from __future__ import annotations

import time
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output  # bound to the Dash instance in register_callbacks

DB_PATH = Path("data/gold.db")

# =========================
# DB helpers
# =========================
def _conn() -> sqlite3.Connection:
    # Fresh connection each time so we always see latest commits
    return sqlite3.connect(DB_PATH, isolation_level=None)

def _table_min_max(table: str, col: str = "date") -> tuple[str | None, str | None]:
    with _conn() as c:
        row = c.execute(f"SELECT MIN({col}), MAX({col}) FROM {table}").fetchone()
    return (row[0], row[1]) if row else (None, None)

def list_vn_brands() -> List[str]:
    with _conn() as c:
        df = pd.read_sql("SELECT DISTINCT brand FROM vn_gold ORDER BY brand", c)
    return [b for b in df.get("brand", pd.Series([], dtype=object)).dropna().astype(str).tolist()]

# =========================
# VN data & transforms
# =========================
def load_vn(start: Optional[str], end: Optional[str], brands: Optional[Iterable[str]]) -> pd.DataFrame:
    where, params = [], []
    if start:
        where.append("date >= ?"); params.append(start)
    if end:
        where.append("date <= ?"); params.append(end)
    if brands:
        brands = [str(b) for b in brands]
        where.append("brand IN (%s)" % ",".join(["?"] * len(brands)))
        params.extend(brands)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT date, brand, buy_price, sell_price
        FROM vn_gold
        {where_sql}
        ORDER BY date ASC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c, params=params)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ("buy_price", "sell_price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["avg_price"] = df[["buy_price", "sell_price"]].mean(axis=1)
    return df.dropna(subset=["date", "avg_price"])

def iqr_filter(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df

    def _f(g: pd.DataFrame) -> pd.DataFrame:
        g = g.dropna(subset=[col])
        if g.empty:
            return g
        q1, q3 = g[col].quantile(0.25), g[col].quantile(0.75)
        iqr = (q3 - q1) if pd.notna(q3) and pd.notna(q1) else 0.0
        if iqr == 0:
            return g
        lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        return g[(g[col] >= lo) & (g[col] <= hi)]

    return df.groupby("brand", group_keys=False).apply(_f) if "brand" in df.columns else _f(df)

def smooth_brand(df: pd.DataFrame, window: int) -> pd.DataFrame:
    if df.empty or window <= 1:
        return df

    def _s(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        g["avg_price"] = g["avg_price"].rolling(int(window), min_periods=1).mean()
        return g

    return df.groupby("brand", group_keys=False).apply(_s) if "brand" in df.columns else _s(df)

def rebase100_brand(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    def _rb(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        base = pd.to_numeric(g["avg_price"], errors="coerce").dropna()
        if base.empty:
            return g
        base_val = base.iloc[0]
        if base_val:
            g["avg_price"] = g["avg_price"] / base_val * 100.0
        return g

    return df.groupby("brand", group_keys=False).apply(_rb) if "brand" in df.columns else _rb(df)

# =========================
# World data & transforms
# =========================
def load_world(start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    where, params = [], []
    if start:
        where.append("date >= ?"); params.append(start)
    if end:
        where.append("date <= ?"); params.append(end)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT date, open, high, low, close, volume
        FROM world_gold
        {where_sql}
        ORDER BY date ASC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c, params=params)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["date", "close"])

def add_ma(df: pd.DataFrame, n: int) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    if df.empty or n <= 1 or "close" not in df.columns:
        df["ma"] = np.nan
        return df
    df["ma"] = df["close"].rolling(int(n), min_periods=1).mean()
    return df

def rebase100_series(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    df = df.sort_values("date").copy()
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return df
    base = s.iloc[0]
    if base:
        df[col] = df[col] / base * 100.0
    return df

# =========================
# FX data & transforms
# =========================
def load_fx(start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    where, params = [], []
    if start:
        where.append("date >= ?"); params.append(start)
    if end:
        where.append("date <= ?"); params.append(end)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT date, rate
        FROM usd_vnd
        {where_sql}
        ORDER BY date ASC
    """
    with _conn() as c:
        df = pd.read_sql(sql, c, params=params)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    return df.dropna(subset=["date", "rate"])

def smooth_series(df: pd.DataFrame, col: str, window: int) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    if df.empty or col not in df.columns or window <= 1:
        df[f"{col}_sm"] = df.get(col)
        return df
    df[f"{col}_sm"] = df[col].rolling(int(window), min_periods=1).mean()
    return df

# =========================
# Plot helpers (robust)
# =========================
SAFE_TEMPLATE = "plotly_white"

def _empty_fig(title: str = "No data", height: int = 330) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(title=title, template=SAFE_TEMPLATE, height=height,
                      margin=dict(l=40, r=20, t=50, b=40))
    fig.update_layout(template=None)
    return fig

def _safe_histogram(df: pd.DataFrame, x: str, nbins: int = 40, title: str = "", height: int = 330) -> go.Figure:
    if df.empty or x not in df.columns:
        return _empty_fig(title=title or "No data", height=height)
    s = pd.to_numeric(df[x], errors="coerce").dropna()
    if s.empty:
        return _empty_fig(title=title or "No data", height=height)
    fig_px = px.histogram(s.to_frame(name=x), x=x, nbins=int(nbins), title=title, template=SAFE_TEMPLATE)
    fig = go.Figure(fig_px)
    fig.update_layout(template=None, height=height, margin=dict(l=40, r=20, t=50, b=40), bargap=0.05)
    return fig

def _safe_line(df: pd.DataFrame, x: str, y: str, color: Optional[str] = None,
               title: str = "", log_y: bool = False, height: int = 340) -> go.Figure:
    if df.empty or x not in df.columns or y not in df.columns:
        return _empty_fig(title=title or "No data", height=height)
    dfx = df.copy()
    dfx[y] = pd.to_numeric(dfx[y], errors="coerce")
    dfx = dfx.dropna(subset=[x, y]).sort_values(x)
    if dfx.empty:
        return _empty_fig(title=title or "No data", height=height)

    if color and color in dfx.columns:
        fig_px = px.line(dfx, x=x, y=y, color=color, title=title, template=SAFE_TEMPLATE)
        fig = go.Figure(fig_px)
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dfx[x], y=dfx[y], mode="lines", name=y))
        fig.update_layout(title=title, template=SAFE_TEMPLATE)
    if log_y:
        fig.update_yaxes(type="log")
    fig.update_layout(template=None, height=height, margin=dict(l=40, r=20, t=50, b=40), hovermode="x unified")
    return fig

def _safe_box(df: pd.DataFrame, x: str, y: str, title: str = "", log_y: bool = False, height: int = 340) -> go.Figure:
    if df.empty or x not in df.columns or y not in df.columns:
        return _empty_fig(title=title or "No data", height=height)
    dfx = df.copy()
    dfx[y] = pd.to_numeric(dfx[y], errors="coerce")
    dfx = dfx.dropna(subset=[x, y])
    if dfx.empty:
        return _empty_fig(title=title or "No data", height=height)
    fig_px = px.box(dfx, x=x, y=y, points=False, title=title, template=SAFE_TEMPLATE)
    fig = go.Figure(fig_px)
    if log_y:
        fig.update_yaxes(type="log")
    fig.update_layout(template=None, height=height, margin=dict(l=40, r=20, t=50, b=40), xaxis_tickangle=-35)
    return fig

# =====================================================================
# Public entry: register all callbacks onto the provided dash_app
# =====================================================================
def register_callbacks(dash_app):
    # ----------------- Version bumper (forces re-queries) -----------------
    @dash_app.callback(
        Output("data-version", "data"),
        Input("refresh-btn", "n_clicks"),
        Input("data-tick", "n_intervals"),
        prevent_initial_call=False,
    )
    def bump_version(n_clicks, n_intervals):
        # changing scalar; all callbacks depending on this will rerun
        return time.time()

    # ----------------- Sync DatePickerRange with DB coverage -------------
    @dash_app.callback(
        Output("vn-range", "min_date_allowed"),
        Output("vn-range", "max_date_allowed"),
        Output("vn-range", "start_date"),
        Output("vn-range", "end_date"),
        Input("data-version", "data"),
        prevent_initial_call=False,
    )
    def sync_vn_range(_ver):
        mn, mx = _table_min_max("vn_gold", "date")
        return mn, mx, mn, mx

    @dash_app.callback(
        Output("world-range", "min_date_allowed"),
        Output("world-range", "max_date_allowed"),
        Output("world-range", "start_date"),
        Output("world-range", "end_date"),
        Input("data-version", "data"),
        prevent_initial_call=False,
    )
    def sync_world_range(_ver):
        mn, mx = _table_min_max("world_gold", "date")
        return mn, mx, mn, mx

    @dash_app.callback(
        Output("fx-range", "min_date_allowed"),
        Output("fx-range", "max_date_allowed"),
        Output("fx-range", "start_date"),
        Output("fx-range", "end_date"),
        Input("data-version", "data"),
        prevent_initial_call=False,
    )
    def sync_fx_range(_ver):
        mn, mx = _table_min_max("usd_vnd", "date")
        return mn, mx, mn, mx

    # ---------- Initialize VN brand options ----------
    @dash_app.callback(
        Output("vn-brands", "options"),
        Output("vn-brands", "value"),
        Input("vn-brands", "id"),
    )
    def init_vn_brands(_):
        brands = list_vn_brands()
        return [{"label": b, "value": b} for b in brands], brands[:5]

    # ---------- VN charts ----------
    @dash_app.callback(
        Output("vn-line", "figure"),
        Output("vn-box", "figure"),
        Input("vn-range", "start_date"),
        Input("vn-range", "end_date"),
        Input("vn-brands", "value"),
        Input("vn-scale", "value"),
        Input("vn-normalize", "value"),
        Input("vn-smooth", "value"),
        Input("vn-outlier", "value"),
        Input("data-version", "data"),  # 👈 force fresh query
    )
    def cb_vn(start_date, end_date, brands, scale, normalize_vals, smooth_days, outlier_vals, _ver):
        brands = brands or []
        df = load_vn(start_date, end_date, brands if brands else None)
        if df.empty:
            return _empty_fig("No data"), _empty_fig("No data")

        if "iqr" in (outlier_vals or []):
            df = iqr_filter(df, "avg_price")
        df = smooth_brand(df, int(smooth_days or 1))

        y_title = "Average price (₫)"
        if "rb" in (normalize_vals or []):
            df = rebase100_brand(df)
            y_title = "Rebased (100 = first day)"

        log_y = (scale == "log")
        fig_line = _safe_line(
            df, x="date", y="avg_price", color="brand",
            title=f"VN gold · avg(buy,sell) by brand<br><sub>{y_title}</sub>",
            log_y=log_y
        )
        fig_box = _safe_box(
            df, x="brand", y="avg_price",
            title=f"Distribution of VN gold by brand<br><sub>{y_title}</sub>",
            log_y=log_y
        )
        return fig_line, fig_box

    # ---------- World charts ----------
    @dash_app.callback(
        Output("world-main", "figure"),
        Output("world-dist", "figure"),
        Input("world-range", "start_date"),
        Input("world-range", "end_date"),
        Input("world-scale", "value"),
        Input("world-normalize", "value"),
        Input("world-ma", "value"),
        Input("world-chart-type", "value"),
        Input("data-version", "data"),  # 👈 force fresh query
    )
    def cb_world(start_date, end_date, scale, normalize_vals, ma_n, chart_type, _ver):
        df = load_world(start_date, end_date)
        if df.empty:
            return _empty_fig("No data"), _empty_fig("No data")

        df = df.sort_values("date")
        y_title = "Close"
        if "rb" in (normalize_vals or []):
            df = rebase100_series(df, "close")
            y_title = "Rebased close (100 = first day)"

        ma_n = int(ma_n or 7)
        df_ma = add_ma(df, ma_n)

        if chart_type == "candle" and {"open", "high", "low", "close"}.issubset(df.columns):
            fig_main = go.Figure(data=[
                go.Candlestick(x=df["date"], open=df["open"], high=df["high"],
                               low=df["low"], close=df["close"], name="OHLC")
            ])
            if "ma" in df_ma.columns and df_ma["ma"].notna().any():
                fig_main.add_trace(go.Scatter(x=df_ma["date"], y=df_ma["ma"], mode="lines", name=f"MA {ma_n}"))
            fig_main.update_layout(
                title=f"World gold – candlestick<br><sub>{y_title}</sub>",
                template=SAFE_TEMPLATE, height=420, margin=dict(l=40, r=20, t=60, b=40)
            )
        else:
            fig_main = go.Figure()
            fig_main.add_trace(go.Scatter(x=df["date"], y=df["close"], mode="lines", name="close"))
            if "ma" in df_ma.columns and df_ma["ma"].notna().any():
                fig_main.add_trace(go.Scatter(x=df_ma["date"], y=df_ma["ma"], mode="lines", name=f"MA {ma_n}"))
            fig_main.update_layout(
                title=f"World gold – close & MA<br><sub>{y_title}</sub>",
                template=SAFE_TEMPLATE, height=420, margin=dict(l=40, r=20, t=60, b=40),
                hovermode="x unified",
            )
        if scale == "log":
            fig_main.update_yaxes(type="log")

        df_ret = df.copy()
        df_ret["ret"] = pd.to_numeric(df_ret["close"], errors="coerce").pct_change() * 100.0
        fig_dist = _safe_histogram(df_ret.dropna(subset=["ret"]), x="ret", nbins=40,
                                   title="Distribution of daily returns (%)", height=330)
        fig_dist.update_layout(xaxis_title="Return (%)")

        fig_main.update_layout(template=None)
        fig_dist.update_layout(template=None)
        return fig_main, fig_dist

    # ---------- FX charts ----------
    @dash_app.callback(
        Output("fx-line", "figure"),
        Output("fx-hist", "figure"),
        Input("fx-range", "start_date"),
        Input("fx-range", "end_date"),
        Input("fx-scale", "value"),
        Input("fx-normalize", "value"),
        Input("fx-smooth", "value"),
        Input("data-version", "data"),  # 👈 force fresh query
    )
    def cb_fx(start_date, end_date, scale, normalize_vals, smooth_days, _ver):
        df = load_fx(start_date, end_date)
        if df.empty:
            return _empty_fig("No data"), _empty_fig("No data")

        df = df.sort_values("date")
        df_sm = smooth_series(df, "rate", int(smooth_days or 1))
        y_col = "rate_sm"
        y_title = "USD/VND"

        if "rb" in (normalize_vals or []):
            base = pd.to_numeric(df_sm[y_col], errors="coerce").dropna()
            if not base.empty and base.iloc[0]:
                df_sm[y_col] = df_sm[y_col] / base.iloc[0] * 100.0
                y_title = "Rebased (100 = first day)"

        fig_line = _safe_line(df_sm, x="date", y=y_col,
                              title=f"USD/VND time series<br><sub>{y_title}</sub>",
                              log_y=(scale == "log"), height=380)

        df_ret = df.copy()
        df_ret["ret"] = pd.to_numeric(df_ret["rate"], errors="coerce").pct_change() * 100.0
        fig_hist = _safe_histogram(df_ret.dropna(subset=["ret"]), x="ret", nbins=40,
                                   title="Distribution of daily returns (%)", height=330)
        fig_hist.update_layout(xaxis_title="Return (%)")
        return fig_line, fig_hist