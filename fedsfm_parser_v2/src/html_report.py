from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime
from pathlib import Path
import pandas as pd
from config import REPORTS_DIR

# ----------------------------------------------------------------------
env = Environment(
    loader=FileSystemLoader(Path(__file__).resolve().parent / "templates"),
    autoescape=select_autoescape(['html', 'xml'])
)

def build_report(
    added_rfm: list[dict],
    removed_rfm: list[dict],
    added_db: list[dict],
    removed_db: list[dict],
    matches: list[dict],
) -> Path:
    """Создаёт HTML-файл и возвращает его путь."""
    tmpl = env.get_template("report.html")

    html = tmpl.render(
        ts=datetime.now().strftime("%d.%m.%Y %H:%M"),
        added_rfm=added_rfm,
        removed_rfm=removed_rfm,
        added_db=added_db,
        removed_db=removed_db,
        matches=matches,
    )

    path = REPORTS_DIR / f"report_{datetime.now():%Y%m%d_%H%M%S}.html"
    path.write_text(html, encoding="utf-8")
    return path
