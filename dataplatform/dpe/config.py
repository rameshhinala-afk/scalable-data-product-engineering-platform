"""Central configuration. Override with environment variables if needed."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DPE_DATA_DIR", BASE_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_PATH = DATA_DIR / "warehouse.db"

WORKERS = int(os.environ.get("DPE_WORKERS", os.cpu_count() or 2))
MAX_REJECT_RATE = float(os.environ.get("DPE_MAX_REJECT_RATE", 0.05))  # 5%
API_HOST = os.environ.get("DPE_API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("DPE_API_PORT", 8000))
