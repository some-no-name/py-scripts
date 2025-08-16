"""
Главный модуль: запускает парсер, сравнение, отчёт и уведомления.
Запускается либо:
    $ python -m src.main                # обычный режим
    $ python -m src.main --all          # сравнить весь список RFM ↔ Airtable
    $ python -m src.main --test         # тестовый режим (как прежде)
Вызывается и из bot_server.py.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd

from notify_tools import notify
from logger_utils import setup_logger
from rfm_parser import fetch_rfm_list
from storage import save_dataset, load_latest, save_report, update_master
from compare import diff
from airtable_client import AirtableClient
from compare_airtable import compare_all_mapped, find_best_match, MatchType, match_quality, missed_regions
from html_report import build_report
from telegram_notify import send_message, send_document
from config import (
    AIRTABLE_CACHE_DIR,
    AIRTABLE_TABLE_PERSECUTED,
    AIRTABLE_VIEW_MONITORING,
    AIRTABLE_FIELDS_MAIN,
    DATA_DIR,
    REPORTS_DIR,
)
from utils import get_file_name

# ──────────────────────────────────────────────────────────────────────

def run(*, test_mode: bool = False, compare_all: bool = False) -> None:
    """
    Основной workflow.
    :param test_mode:    эхо-режим, отключает часть уведомлений (как раньше)
    :param compare_all:  сравнить RFM с каждым человеком в Airtable (вопрос «1»)
    """
    try:
        ts = datetime.now()
        log_path: Path = setup_logger()
        logging.info("=== RFM Parser started ===")
        logging.info("Log path: %s", log_path)

        # 1. ── Парсим RFM ────────────────────────────────────────────────────
        df_new = fetch_rfm_list()
        file_new = save_dataset(df_new, ts)
        logging.info("Dataset saved: %s", file_new)

        # 2. ── Diff «новый vs предыдущий RFM» ───────────────────────────────
        df_old, file_old = load_latest(exclude=file_new)
        if df_old is None:
            logging.info("First launch – diff step skipped")
            added_rfm: List[Dict] = []
            removed_rfm: List[Dict] = []
            changed_rfm = []
            df_old = df_new.copy()
        else:
            added_rfm, removed_rfm, changed_rfm = diff(df_old, df_new)
            logging.info("RFM diff: +%d | -%d | ~%d", len(added_rfm), len(removed_rfm), len(changed_rfm))

        # TODO:
        update_master(df_new, ts)

        # 2.5 -- RFM отчет  ─────────────────────────────────────────────────────
        report = []

        report.append(f"New file: {get_file_name(file_new)}; {len(df_new)} записей")
        report.append(f"Compare file: {get_file_name(file_old)}; {len(df_old)} записей")
        report.append(f"RFM diff: +{len(added_rfm)} | -{len(removed_rfm)} | ~{len(changed_rfm)}")

        for added_row in added_rfm:
            msg = "#добавлен"
            msg += f"\n{added_row.get('Изначальный текст') or added_row.get('Имя')}"
            report.append(msg)

        for removed_row in removed_rfm:
            msg = "#удален"
            msg += f"\n{removed_row.get('Изначальный текст') or removed_row.get('Имя')}"
            report.append(msg)

        for old_row, new_row in changed_rfm:
            msg = "#обновлен (было/стало)"
            msg += f"\n{old_row.get('Изначальный текст') or old_row.get('Имя')}"
            msg += f"\n\n{new_row.get('Изначальный текст') or new_row.get('Имя')}"
            report.append(msg)

        # 3. ── Airtable ─────────────────────────────────────────────────────
        client = AirtableClient()
        airtable_df, airtable_file = client.fetch_df(
            AIRTABLE_TABLE_PERSECUTED,
            view=AIRTABLE_VIEW_MONITORING,
            fields=AIRTABLE_FIELDS_MAIN,
            cache_name=f"airtable_{ts:%Y%m%d_%H%M%S}.csv",
        )
        logging.info("Airtable rows: %d", len(airtable_df))

        airtable_df = pd.read_csv(airtable_file, dtype=str)

        report.append(f"Airtable file: {get_file_name(airtable_file)}; {len(airtable_df)} записей")

        # 4. ── Сравнение added/removed с Airtable (уведомления) ─────────────        
        if not compare_all:
            def _find_match(row, mode: str, index: int, count: int):
                mtype, mrow, score = find_best_match(row, airtable_df)
                logging.info(f"Best match {mode} ({index}/{count}): {mtype}({score})\nRFM: {row.get("Изначальный текст")}\nAIR: {mrow}")
                if mtype != "none":
                    notify(mode, row, mtype, score, mrow, report)

            index = 0
            for row in added_rfm:
                index += 1
                _find_match(row, "add", index, len(added_rfm))

            index = 0
            for row in removed_rfm:
                index += 1
                _find_match(row, "del", index, len(removed_rfm))

            index = 0
            for old_row, row in changed_rfm:
                _find_match(row, "changed", index, len(changed_rfm))


        # 5. ── Полное сравнение (если compare_all=True) ──────────────────────
        index = 0
        # just matches list
        matches_all: List[Dict] = []
        if compare_all:
            logging.info("Running FULL compare RFM ↔ Airtable …")

            matches_all = compare_all_mapped(df_new, airtable_df, report)

            logging.info("Full compare: %d matches", len(matches_all))

        if len(missed_regions) > 0:
            missed_regions_list = ', '.join(list(missed_regions))
            logging.warning(f"Missed regions list: [{missed_regions_list}]")
            send_message(f"! В словаре регионов есть пропущенные ключи: [{missed_regions_list}]")

        # 6. ── HTML-отчёт ────────────────────────────────────────────────────
        rfm_report_save = "\n----------\n".join(report)
        report_path = save_report(rfm_report_save, ts)
        logging.info("TXT report: %s", report_path)

        # report_path = build_report(
        #     added_rfm=added_rfm,
        #     removed_rfm=removed_rfm,
        #     added_db=[],          # можно заполнить при желании
        #     removed_db=[],        # …
        #     matches=matches_all,
        # )
        # logging.info("HTML report: %s", report_path)

        # 7. ── Итоговая сводка + файлы ───────────────────────────────────────
        summary = "#отчет\n"
        summary += f"RFM: {len(df_new)} записей\n"
        summary += f"+{len(added_rfm)} | -{len(removed_rfm)} | ~{len(changed_rfm)}"

        logging.info(f"Summary: {summary}")
        send_message(summary)
        send_document(str(file_new))
        send_document(str(report_path))

        logging.info("=== Finished ===")

    except Exception as e:
        logging.error(e)

# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="RFM parser main entry")
    parser.add_argument("--test", action="store_true", help="Test mode (quiet)")
    parser.add_argument("--all",  action="store_true",
                        help="Compare EVERY RFM row with Airtable")
    args = parser.parse_args()
    run(test_mode=args.test, compare_all=args.all)
