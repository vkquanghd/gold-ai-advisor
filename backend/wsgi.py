# backend/wsgi.py
from flask import Flask, redirect, url_for
from pathlib import Path

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # register admin blueprint
    from backend.routes.admin import admin_bp
    app.register_blueprint(admin_bp)

    # register dash app
    from backend.dash_app import register_dash
    register_dash(app)

    @app.route("/")
    def index():
        # chuyển về trang bảng VN Gold
        return redirect(url_for("admin.admin_vn"))

    return app

app = create_app()