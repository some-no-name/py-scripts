import logging
from pathlib import Path
from datetime import datetime

def setup_logger(logs_dir: str, level=logging.INFO) -> Path:
    log_path = logs_dir / f"log_{datetime.now():%Y_%m_%d__%H_%M_%S}.log"

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return log_path
