import logging
from pathlib import Path
from datetime import datetime
from config import LOGS_DIR
from storage import _clean

def setup_logger(level=logging.INFO) -> Path:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / f"log_{datetime.now():%Y_%m_%d__%H_%M_%S}.log"

    _clean(LOGS_DIR, "*.log")

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return log_path