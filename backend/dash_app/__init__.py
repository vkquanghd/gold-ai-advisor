from __future__ import annotations
from dash import Dash
from .layout import build_layout

def register_dash(flask_app):
    """
    Mount a Dash app at /dash/ on top of the given Flask app.
    """
    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname="/dash/",
        suppress_callback_exceptions=True,  # allow callbacks for components created later
    )
    dash_app.layout = build_layout()

    # Register all callbacks (this imports and executes registration)
    from .callbacks import register_callbacks
    register_callbacks(dash_app)

    return dash_app