# backend/routes/api.py
from flask import Blueprint, jsonify
import subprocess, sys

api_bp = Blueprint("api", __name__, url_prefix="/api")

@api_bp.post("/trigger_update")
def trigger_update():
    # Ví dụ chạy lệnh “make daily” trong shell (đảm bảo Makefile có target daily)
    try:
        # comment 2 dòng dưới nếu bạn tự viết logic update bằng Python
        subprocess.run(["make", "daily"], check=True)
        return jsonify({"status": "ok", "message": "Daily update triggered"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500