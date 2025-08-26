from dataclasses import dataclass

@dataclass
class DashConfig:
    db_path: str = "data/gold.db"
    lookback_days: int = 365
    page_size: int = 50  # not used by graphs, but handy if add tables