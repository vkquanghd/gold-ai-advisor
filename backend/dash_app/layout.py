from __future__ import annotations
from dash import html, dcc
import datetime as dt

def build_layout() -> html.Div:
    today = dt.date.today()
    start_default = today - dt.timedelta(days=90)

    # Reusable ranges
    def date_range(id_prefix: str):
        return dcc.DatePickerRange(
            id=f"{id_prefix}-range",
            min_date_allowed=dt.date(2000, 1, 1),
            max_date_allowed=today,
            start_date=start_default,
            end_date=today,
            display_format="YYYY-MM-DD",
            number_of_months_shown=2,
        )

    def scale_picker(id_prefix: str, default="linear"):
        return dcc.RadioItems(
            id=f"{id_prefix}-scale",
            options=[{"label": "Linear", "value": "linear"},
                     {"label": "Log", "value": "log"}],
            value=default,
            inline=True,
        )

    def normalize_check(id_prefix: str):
        return dcc.Checklist(
            id=f"{id_prefix}-normalize",
            options=[{"label": "Rebase to 100", "value": "rb"}],
            value=[],
            inline=True,
        )

    def smooth_slider(id_prefix: str):
        return dcc.Slider(
            id=f"{id_prefix}-smooth",
            min=1, max=14, step=1, value=1,
            tooltip={"placement": "bottom", "always_visible": False}
        )

    return html.Div([
        # ======== Global refresh controls (hidden store + interval) ========
            html.Div(
                [
                    html.Button("Refresh data", id="refresh-btn", n_clicks=0, className="btn"),
                    dcc.Interval(id="data-tick", interval=30 * 1000, n_intervals=0),  # every 30s
                    dcc.Store(id="data-version"),  # holds a timestamp so callbacks rerun
                ],
                style={"display": "flex", "gap": "10px", "alignItems": "center", "margin": "8px 0"},
            ),
        html.H2("EDA Dashboard"),

        dcc.Tabs(id="tabs", value="tab-vn", children=[
            # ---------------- VN TAB ----------------
            dcc.Tab(label="VN gold", value="tab-vn", children=[
                html.Div(className="controls", children=[
                    html.Div(className="ctrl", children=[html.Label("Date range"), date_range("vn")]),
                    html.Div(className="ctrl", children=[
                        html.Label("Brands"),
                        dcc.Dropdown(id="vn-brands", multi=True, placeholder="All brands"),
                    ]),
                    html.Div(className="ctrl", children=[html.Label("Scale"), scale_picker("vn")]),
                    html.Div(className="ctrl", children=[html.Label("Normalize"), normalize_check("vn")]),
                    html.Div(className="ctrl", children=[html.Label("Smoothing (days)"), smooth_slider("vn")]),
                    html.Div(className="ctrl", children=[
                        html.Label("Outliers"),
                        dcc.Checklist(id="vn-outlier",
                                      options=[{"label": "Remove by IQR", "value": "iqr"}],
                                      value=[], inline=True),
                    ]),
                ]),
                html.Div(className="plots", children=[
                    dcc.Graph(id="vn-line"),
                    dcc.Graph(id="vn-box"),
                ]),
            ]),

            # ---------------- WORLD TAB ----------------
            dcc.Tab(label="World gold", value="tab-world", children=[
                html.Div(className="controls", children=[
                    html.Div(className="ctrl", children=[html.Label("Date range"), date_range("world")]),
                    html.Div(className="ctrl", children=[html.Label("Scale"), scale_picker("world")]),
                    html.Div(className="ctrl", children=[html.Label("Normalize"), normalize_check("world")]),
                    html.Div(className="ctrl", children=[
                        html.Label("Moving average (days)"),
                        dcc.Input(id="world-ma", type="number", min=1, max=60, step=1, value=7, style={"width": "90px"}),
                    ]),
                    html.Div(className="ctrl", children=[
                        html.Label("Chart type"),
                        dcc.RadioItems(
                            id="world-chart-type",
                            options=[{"label": "Line (close)", "value": "line"},
                                     {"label": "Candlestick (OHLC)", "value": "candle"}],
                            value="line", inline=True),
                    ]),
                ]),
                html.Div(className="plots", children=[
                    dcc.Graph(id="world-main"),
                    dcc.Graph(id="world-dist"),
                ]),
            ]),

            # ---------------- FX TAB ----------------
            dcc.Tab(label="USD/VND", value="tab-fx", children=[
                html.Div(className="controls", children=[
                    html.Div(className="ctrl", children=[html.Label("Date range"), date_range("fx")]),
                    html.Div(className="ctrl", children=[html.Label("Scale"), scale_picker("fx")]),
                    html.Div(className="ctrl", children=[html.Label("Normalize"), normalize_check("fx")]),
                    html.Div(className="ctrl", children=[html.Label("Smoothing (days)"), smooth_slider("fx")]),
                ]),
                html.Div(className="plots", children=[
                    dcc.Graph(id="fx-line"),
                    dcc.Graph(id="fx-hist"),
                ]),
            ]),
        ]),
    ], className="container")