import os
import re
import hashlib
import argparse
import requests
import pandas as pd
import logging
import glob

from bs4 import BeautifulSoup
from datetime import datetime
from tg_bot import send_telegram_message, send_telegram_file

from consts import LOGS_FOLDER, DATA_FOLDER, PARSE_URL, LOGS_LEVEL, KEEP_FILES_COUNT


def clean_field(value: str) -> str:
    if not isinstance(value, str):
        return value  # skip cleaning non-string values

    # Remove leading/trailing spaces and unwanted punctuation
    return re.sub(r'^[\s,;.\'\"“”«»]+|[\s,;.\'\"“”«»]+$', '', value)

def setup_logger() -> str:
    log_dir = LOGS_FOLDER
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("log_%Y_%m_%d__%H_%M_%S.log")
    log_path = os.path.join(log_dir, log_filename)

    logging.basicConfig(
        level=LOGS_LEVEL,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return log_path


def find_previous_data_file(current_filename):
    print('os.listdir(DATA_FOLDER)', os.listdir(DATA_FOLDER))

    files = [
        f for f in os.listdir(DATA_FOLDER)
        if f.startswith("data_") and f.endswith(".csv") and f != current_filename
    ]

    if not files:
        return None

    # Option 1: Sort by timestamp in filename
    def extract_datetime(f):
        try:
            return datetime.strptime(f, "data_%Y_%m_%d__%H_%M_%S.csv")
        except ValueError:
            return datetime.min  # fallback for unexpected files

    files.sort(key=extract_datetime, reverse=True)
    return os.path.join(DATA_FOLDER, files[0])


def generate_record_id(rec: str) -> str:
    unique_string = rec.strip().lower()
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

def parse_person_text(text: str):
    try:
        logging.debug(f"Raw text: {text}")
        terrorist = '*' in text

        # Extract number at the start
        number_match = re.match(r'(\d+)\.', text)
        number = number_match.group(1) if number_match else ""

        # Remove number part
        text_no_number = text[number_match.end():].strip() if number_match else text.strip()

        # Extract birth date
        birth_date_match = re.search(r'(\d{2}\.\d{2}\.\d{4}) г\.р\.', text_no_number)
        birth_date = birth_date_match.group(1) if birth_date_match else ""

        # Extract alias in parentheses if present
        alias_match = re.search(r'\(([^()]*)\)', text_no_number)
        second_name_in_braces = clean_field(alias_match.group(1)) if alias_match else ""

        # Remove alias and birth date for name extraction
        cleaned_text = text_no_number
        if alias_match:
            cleaned_text = cleaned_text.replace(alias_match.group(0), '')
        if birth_date_match:
            cleaned_text = cleaned_text.replace(birth_date_match.group(0), '')

        # Extract name (everything before first comma or double comma)
        parts = [p.strip() for p in cleaned_text.split(',') if p.strip()]
        name = parts[0].replace('*', '') if parts else ""
        name = clean_field(name)

        # Other data = everything remaining after birth date
        other_data = ""
        if birth_date_match:
            other_data = text_no_number[birth_date_match.end():].strip()
        elif len(parts) > 1:
            other_data = ', '.join(parts[1:]).strip()
        other_data = clean_field(other_data)

        birth_date = clean_field(birth_date)
        rec_id = generate_record_id(text)

        return [rec_id, number, name, second_name_in_braces, birth_date, other_data, terrorist]

    except Exception as e:
        logging.warning(f"Ошибка при обработке строки: {e}")
        return ["Ошибка", "", text, "", "", "", ""]



def clean_old_files(folder_path, keep_latest=10):
    files = sorted(glob.glob(os.path.join(folder_path, "*")), key=os.path.getmtime, reverse=True)
    for old_file in files[keep_latest:]:
        try:
            os.remove(old_file)
        except Exception as e:
            logging.warning(f"Не удалось удалить {old_file}: {e}")

def parse_data():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(LOGS_FOLDER, exist_ok=True)  # Assuming you have a LOG_FOLDER constant

    # Clean up old files before starting
    clean_old_files(DATA_FOLDER, KEEP_FILES_COUNT)
    clean_old_files(LOGS_FOLDER, KEEP_FILES_COUNT)

    data_filename = datetime.now().strftime("data_%Y_%m_%d__%H_%M_%S.csv")
    data_filepath = os.path.join(DATA_FOLDER, data_filename)
    prev_filename = find_previous_data_file(data_filename)

    try:
        send_telegram_message("🚀 Начат парсинг данных...")

        url = PARSE_URL
        logging.info(f"Загрузка HTML страницы: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        section = soup.select_one("#russianFL")

        if not section:
            raise ValueError("Секция #russianFL не найдена в HTML. Страница могла измениться.")

        persons_list = section.find_all("li")
        data = []

        persons_list_count = len(persons_list)
        person_parse_errors = 0
        person_parse_ok = 0

        for person in persons_list:
            text = person.get_text(strip=True)

            parsed = parse_person_text(text)
            data.append(parsed)

            if parsed[0] != "Ошибка":
                person_parse_ok += 1
                logging.debug(f"Parsed: {','.join(map(str, parsed))}")
            else:
                person_parse_errors += 1

        if not data:
            raise ValueError("Данные не найдены на странице.")

        logging.info(f"Результаты парсинга count: {persons_list_count}, ok: {person_parse_ok}, err: {person_parse_errors}")

        df = pd.DataFrame(data, columns=["ID", "Порядковый номер", "Имя", "Доп Имя", "Дата рождения", "Остальные данные", "Террорист"])
        df.to_csv(data_filepath, index=False, encoding='utf-8')
        logging.info(f"Новая выгрузка сохранена в файл {data_filename}")

        summary = f"✅ Парсинг завершён.\nВсего: {persons_list_count}\nУспешно: {person_parse_ok}\nОшибки: {person_parse_errors}"
        report_file = None

        if prev_filename and os.path.exists(prev_filename):
            old_df = pd.read_csv(prev_filename, dtype=str)
            new_df = pd.read_csv(data_filepath, dtype=str)

            report, added_ids, removed_ids = compare_with_previous(old_df, new_df)
            if report.strip():
                logging.info("Изменения по сравнению с предыдущим запуском:")
                logging.info(report)

                # Save report
                report_filename = os.path.splitext(data_filename)[0] + "_report.txt"
                report_file = os.path.join(DATA_FOLDER, report_filename)
                with open(report_file, "w", encoding="utf-8") as f:
                    f.write(report)

                summary += f"\n\n📌 Изменения:"
                summary += f"\nДобавлено ({len(added_ids)}): {', '.join(map(str, added_ids))}"[:1000]
                summary += f"\nУдалено ({len(removed_ids)}): {', '.join(map(str, removed_ids))}"[:1000]
                
            else:
                logging.info("Изменений нет.")
                summary += "\n\nИзменений нет."
        else:
            logging.info("Это первый запуск. Предыдущих данных для сравнения нет.")
            summary += "\n\n📂 Первый запуск. Нет данных для сравнения."

        logging.debug(summary)
        
        # Send final message and files
        send_telegram_message(summary)
        send_telegram_file(data_filepath)

        if report_file:
            send_telegram_file(report_file)

    except Exception as e:
        logging.error("Произошла ошибка:", e)
        send_telegram_message(f"❌ Ошибка при парсинге данных: {str(e)}")


def compare_with_previous(old_df: pd.DataFrame, new_df: pd.DataFrame) -> str:
    old_data = {row["ID"]: row for _, row in old_df.iterrows()}
    new_data = {row["ID"]: row for _, row in new_df.iterrows()}

    old_ids = set(old_data.keys())
    new_ids = set(new_data.keys())

    logging.debug(f"old_ids: {len(old_ids)}")
    logging.debug(f"new_ids: {len(new_ids)}")

    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids
    common_ids = new_ids & old_ids

    logging.debug(f"added_ids: {len(added_ids)}")
    logging.debug(f"removed_ids: {len(removed_ids)}")
    logging.debug(f"common_ids: {len(common_ids)}")

    lines = []

    if added_ids:
        lines.append("Новые записи:")
        for i in added_ids:
            lines.append(str(new_data[i]))

    if removed_ids:
        lines.append("\nУдалённые записи:")
        for i in removed_ids:
            lines.append(str(old_data[i]))

    # for i in common_ids:
    #     # if not new_data[i].equals(old_data[i]):
    #     if not all(str(a).strip() == str(b).strip() for a, b in zip(new_data[i], old_data[i])):
    #         lines.append(f"\nИзменения в записи ID={i}:\n")
    #         lines.append(f"Было:\n{old_data[i]}")
    #         lines.append(f"Стало:\n{new_data[i]}")

    return "\n".join(lines), added_ids, removed_ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер реестра РФМ (fedsfm.ru)")
    args = parser.parse_args()

    log_path = setup_logger()
    logging.info("=== Запуск парсера данных РФМ ===")
    logging.info(f"Лог сохраняется в: {log_path}")
    parse_data()
    logging.info("=== Завершение работы ===")
