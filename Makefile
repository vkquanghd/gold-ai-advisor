# =========================
# Variables
# =========================
# Virtualenv (Python 3.12)
VENV      ?= .venv
PY        := $(VENV)/bin/python
PIP       := $(VENV)/bin/pip

# Data / Config
DB        ?= data/gold.db
DAYS      ?= 365

# VN crawl/import defaults
VN_JSON   ?= data/vn_raw.json
OUTDIR    ?= data
BASENAME  ?= vn_raw

# =========================
# Phony
# =========================
.PHONY: help venv init world vn-crawl vn-import vn-all daily reset clean-data \
        backend dash dash-bg dash-stop dash-log \
        eda eda-ydata eda-sweetviz eda-autoviz

# =========================
# Help
# =========================
help:
	@echo "Targets:"
	@echo "  venv        - Create venv & install requirements"
	@echo "  init        - Create SQLite schema at $(DB)"
	@echo "  world       - Ingest world gold (GC=F) + USD/VND (window: $(DAYS)d)"
	@echo "  vn-crawl    - Crawl VN gold via CafeF → $(VN_JSON)"
	@echo "  vn-import   - Import $(VN_JSON) into SQLite (prune $(DAYS)d)"
	@echo "  vn-all      - Crawl + Import"
	@echo "  daily       - One-shot daily update (world + vn) with retention $(DAYS)d"
	@echo "  reset       - Fresh DB: init → world → vn-all"
	@echo "  backend     - Run Flask backend on :8000"
	@echo "  dash        - Run Dash EDA on :8050"
	@echo "  dash-bg     - Run Dash in background"
	@echo "  dash-stop   - Stop background Dash"
	@echo "  dash-log    - Tail Dash background log"
	@echo "  eda*        - Generate EDA reports (matplotlib / ydata / sweetviz / AutoViz)"
	@echo "  clean-data  - Remove generated JSON/CSV/LOG (keep DB)"

# =========================
# Environment
# =========================
venv:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@mkdir -p data reports

# =========================
# Schema / World
# =========================
init:
	$(PY) scripts/create_schema.py

world:
	$(PY) -m scripts.ingest_world_fx_3m --days $(DAYS)

# =========================
# VN: Crawl + Import
# =========================
vn-crawl:
	$(PY) -m scripts.vendors.gold_price_focused_crawler \
		--outdir $(OUTDIR) \
		--basename $(BASENAME) \
		--days $(DAYS)

vn-import:
	@test -f $(VN_JSON) || (echo "ERROR: $(VN_JSON) not found. Run 'make vn-crawl' first or set VN_JSON=path"; exit 1)
	$(PY) -m scripts.import_vn_from_json \
		--json $(VN_JSON) \
		--source cafef \
		--retention-days $(DAYS) \
		--prune

vn-all: vn-crawl vn-import

# =========================
# Daily update (world + vn + prune/archive)
# =========================
daily:
	$(PY) -m scripts.daily_update --retention-days $(DAYS) --world --vn

# =========================
# Reset (fresh DB + pipeline)
# =========================
reset:
	rm -f $(DB)
	$(MAKE) init
	$(MAKE) world
	$(MAKE) vn-all

# =========================
# EDA (reports)
# =========================
eda:
	$(PY) -m scripts.eda --db $(DB) --outdir reports/eda --days $(DAYS)

eda-ydata:
	$(PY) -m scripts.eda_ydata --db $(DB) --outdir reports/eda_ydata --days $(DAYS)

eda-sweetviz:
	$(PY) -m scripts.eda_sweetviz --db $(DB) --outdir reports/eda_sweetviz --days $(DAYS)

eda-autoviz:
	$(PY) -m scripts.eda_autoviz --db $(DB) --outdir reports/eda_autoviz --days $(DAYS)

# =========================
# Backend (Flask)
# =========================
backend:
	FLASK_APP=backend.wsgi:app FLASK_ENV=development $(PY) -m flask run --host=0.0.0.0 -p 8000
# =========================
# Dash (Plotly) EDA
# =========================
dash:
	$(PY) -m backend.dash_app.run --db $(DB) --days $(DAYS) --host 0.0.0.0 --port 8050

dash-bg:
	@mkdir -p data
	nohup $(PY) -m backend.dash_app.run --db $(DB) --days $(DAYS) --host 0.0.0.0 --port 8050 > data/dash.out 2>&1 & \
	echo $$! > data/dash.pid; \
	echo "✅ Dash started on :8050 (PID=$$(cat data/dash.pid))"

dash-stop:
	@if [ -f data/dash.pid ]; then \
		kill $$(cat data/dash.pid) || true; \
		rm -f data/dash.pid; \
		echo "🛑 Dash stopped."; \
	else \
		echo "No dash.pid found."; \
	fi

dash-log:
	@tail -n 100 -f data/dash.out || true

# =========================
# Utilities
# =========================
clean-data:
	rm -f data/*.json data/*.csv data/*.out data/*.log