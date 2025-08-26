# scripts/trim_keep_last_3m.py
# Keep only last ~95 days for all tables.

import os, sqlite3, datetime as dt
from pathlib import Path

DB_PATH = Path(os.environ.get("GOLD_DB", "data/gold.db"))

def main():
    cutoff = (dt.date.today() - dt.timedelta(days=365)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        w = conn.execute("DELETE FROM world_gold WHERE date < ?", (cutoff,)).rowcount
        f = conn.execute("DELETE FROM usd_vnd   WHERE date < ?", (cutoff,)).rowcount
        v = conn.execute("DELETE FROM vn_gold   WHERE date < ?", (cutoff,)).rowcount
        conn.commit()
    print(f"""🧹 trimmed rows older than {cutoff}
- world_gold: {w}
- usd_vnd:    {f}
- vn_gold:    {v}
""")

if __name__ == "__main__":
    main()
