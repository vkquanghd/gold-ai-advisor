# backend/app.py
from flask import Flask, redirect, url_for
from backend.routes.admin import admin_bp

def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False
    app.register_blueprint(admin_bp, url_prefix="/admin")

    @app.get("/")
    def root():
        # Jump straight to VN admin
        return redirect(url_for("admin.admin_vn"))

    return app

# exported for FLASK_APP=backend.wsgi:app
app = create_app()