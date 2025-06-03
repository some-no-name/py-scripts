import os
import re
import hashlib
import argparse
import requests
import pandas as pd
import logging
import glob
import difflib

from bs4 import BeautifulSoup
from datetime import datetime
from tg_bot import send_telegram_message, send_telegram_file

from consts import LOGS_FOLDER, DATA_FOLDER, PARSE_URL, LOGS_LEVEL, KEEP_FILES_COUNT, AIRTABLE_FOLDER

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
        handlers=[logging.FileHandler(log_path, encoding='utf-8'), logging.StreamHandler()]
    )
    return log_path


def find_previous_data_file(current_filename):
    files = [
        f for f in os.listdir(DATA_FOLDER)
        if f.startswith("data_") and f.endswith(".csv") and f != current_filename
    ]

    if not files:
        return None
    def extract_datetime(f):
        try:
            return datetime.strptime(f, "data_%Y_%m_%d__%H_%M_%S.csv")
        except ValueError:
            return datetime.min  # fallback for unexpected files

    files.sort(key=extract_datetime, reverse=True)
    return os.path.join(DATA_FOLDER, files[0])

def generate_record_id(name: str, birth_date: str, counter: int = 0) -> str:
    base = f"{name.strip().lower()}|{birth_date.strip()}"
    if counter > 0:
        base += f"|{counter}"
    return hashlib.md5(base.encode('utf-8')).hexdigest()

def parse_person_text(text: str):
    try:
        logging.debug(f"Raw text: {text}")
        terrorist = '*' in text

        # Extract number at the start
        number_match = re.match(r'(\d+)\.', text)
        number = number_match.group(1) if number_match else "-"

        # Remove number part
        text_no_number = text[number_match.end():].strip() if number_match else text.strip()

        # Extract birth date
        birth_date_match = re.search(r'(\d{2}\.\d{2}\.\d{4}) г\.р\.', text_no_number)
        birth_date = birth_date_match.group(1) if birth_date_match else "-"

        # Extract alias in parentheses if present
        alias_match = re.search(r'\(([^()]*)\)', text_no_number)
        second_name_in_braces = clean_field(alias_match.group(1)) if alias_match else "-"

        # Remove alias and birth date for name extraction
        cleaned_text = text_no_number
        if alias_match:
            cleaned_text = cleaned_text.replace(alias_match.group(0), '')
        if birth_date_match:
            cleaned_text = cleaned_text.replace(birth_date_match.group(0), '')

        # Extract name (everything before first comma or double comma)
        parts = [p.strip() for p in cleaned_text.split(',') if p.strip()]
        name = parts[0].replace('*', '') if parts else "-"
        name = clean_field(name)

        # Other data = everything remaining after birth date
        other_data = ""
        if birth_date_match:
            other_data = text_no_number[birth_date_match.end():].strip()
        elif len(parts) > 1:
            other_data = ', '.join(parts[1:]).strip()
        other_data = clean_field(other_data)
        other_data = other_data if other_data else "-"

        birth_date = clean_field(birth_date)

        return [number, name, second_name_in_braces, birth_date, other_data, terrorist, text]

    except Exception as e:
        logging.warning(f"Ошибка при обработке строки: {e}")
        return ["Ошибка", "", "", "", "", ""]

def clean_old_files(folder_path, keep_latest=10):
    files = sorted(glob.glob(os.path.join(folder_path, "*")), key=os.path.getmtime, reverse=True)
    for old_file in files[keep_latest:]:
        try:
            os.remove(old_file)
        except Exception as e:
            logging.warning(f"Не удалось удалить {old_file}: {e}")

def compare_with_previous(old_df, new_df):
    old_ids = set(old_df['ID'])
    new_ids = set(new_df['ID'])

    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids
    common_ids = old_ids & new_ids
    changed_ids = set()

    logging.info(f"Последний запуск записей: {len(old_df)}")
    logging.info(f"Новый запуск записей: {len(new_df)}")
    logging.info(f"Добавлено: {len(added_ids)}")
    logging.info(f"Удалено: {len(removed_ids)}")
    logging.info(f"Общих: {len(common_ids)}")

    # Convert DataFrames to dicts: ID -> row (as dict)
    old_dict = old_df.set_index('ID').to_dict(orient='index')
    new_dict = new_df.set_index('ID').to_dict(orient='index')

    added_list = []
    removed_list = []
    changed_list = []

    # added
    for id_ in added_ids:
        added_list.append(new_dict.get(id_))

    # removed
    for id_ in removed_ids:
        removed_list.append(old_dict.get(id_))

    # changed
    for id_ in common_ids:
        old_row = old_dict.get(id_)
        new_row = new_dict.get(id_)

        # Compare only selected fields
        if (
            old_row.get('Остальные данные') != new_row.get('Остальные данные') or
            old_row.get('Доп Имя') != new_row.get('Доп Имя') or
            old_row.get('Террорист') != new_row.get('Террорист')
        ):
            changed_ids.add(id_)
            changed_list.append((old_row, new_row))

    logging.info(f"Обновлено общих: {len(changed_ids)}")

    return added_list, removed_list, changed_list

    # report = ""
    # if added_ids:
    #     report += f"Добавлены: {len(added_ids)}\n"
    # if removed_ids:
    #     report += f"Удалены: {len(removed_ids)}\n"
    # if changed_ids:
    #     report += f"Изменены данные: {len(changed_ids)}\n"

    # return report, added_ids, removed_ids, changed_ids

def parse_data():
    log_path = setup_logger()
    logging.info("=== Запуск парсера данных РФМ ===")
    logging.info(f"Лог сохраняется в: {log_path}")

    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(LOGS_FOLDER, exist_ok=True)
    os.makedirs(AIRTABLE_FOLDER, exist_ok=True)

    clean_old_files(DATA_FOLDER, KEEP_FILES_COUNT)
    clean_old_files(LOGS_FOLDER, KEEP_FILES_COUNT)
    clean_old_files(AIRTABLE_FOLDER, KEEP_FILES_COUNT)

    data_filename = datetime.now().strftime("data_%Y_%m_%d__%H_%M_%S.csv")
    data_filepath = os.path.join(DATA_FOLDER, data_filename)
    prev_filename = find_previous_data_file(data_filename)

    try:
        send_telegram_message("🚀 Начат парсинг данных...")

        response = requests.get(PARSE_URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        section = soup.select_one("#russianFL")

        if not section:
            raise ValueError("Секция #russianFL не найдена в HTML.")

        persons_list = section.find_all("li")
        data = []
        id_counter = {}

        for person in persons_list:
            text = person.get_text(strip=True)
            parsed = parse_person_text(text)
            if parsed[0] == "Ошибка":
                continue

            number, name, alias, birth_date, other_data, terrorist, raw_text = parsed
            key = (name.lower(), birth_date)
            id_counter.setdefault(key, 0)
            record_id = generate_record_id(name, birth_date, id_counter[key])
            id_counter[key] += 1

            data.append([record_id, number, name, alias, birth_date, other_data, terrorist, raw_text])

        if not data:
            raise ValueError("Нет данных для сохранения.")

        df = pd.DataFrame(data, columns=["ID", "Порядковый номер", "Имя", "Доп Имя", "Дата рождения", "Остальные данные", "Террорист", "Изначальный текст"])
        df.to_csv(data_filepath, index=False, encoding='utf-8')
        logging.info(f"Данные сохранены в {data_filepath}")

        summary = f"#отчет"
        summary += f"\n✅ Парсинг завершён. Записей найдено: {len(data)}"
        summary += f"\nСохранено: {data_filepath}"
        report_file = None

        if prev_filename and os.path.exists(prev_filename):
            logging.info(f"Последний сохраненый файл {prev_filename}")

            old_df = pd.read_csv(prev_filename, dtype=str)
            new_df = pd.read_csv(data_filepath, dtype=str)

            added_list, removed_list, changed_list = compare_with_previous(old_df, new_df)

            summary += f"\n\nСравнение с: {prev_filename}; Было записей: {len(old_df)}"
            summary += f"\n- Добавлено: {len(added_list)};{'Слишком много, отдельных постов не будет!' if len(added_list) >= 100 else ''}"
            summary += f"\n- Удалено: {len(removed_list)};{'Слишком много, отдельных постов не будет!' if len(removed_list) >= 100 else ''}"
            summary += f"\n- Обновлено: {len(changed_list)};{'Слишком много, отдельных постов не будет!' if len(changed_list) >= 100 else ''}"

            send_telegram_message(summary)
            send_telegram_file(data_filepath)

            report = []

            index = 0
            for added_row in added_list:
                index += 1
                msg = "#добавлен"
                msg += f"\n{added_row.get('Изначальный текст') or added_row.get('Имя')}"

                report.append(msg)

                if index < 100:
                    send_telegram_message(msg)

            index = 0
            for removed_row in removed_list:
                index += 1

                msg = "#удален"
                msg += f"\n{removed_row.get('Изначальный текст') or removed_list.get('Имя')}"

                report.append(msg)
            
                if index < 100:
                    send_telegram_message(msg)

            index = 0
            for old_row, new_row in changed_list:
                index += 1

                msg = "#обновлен (было/стало)"
                msg += f"\n\n{old_row.get('Изначальный текст') or old_row.get('Имя')}"
                msg += f"\n\n{new_row.get('Изначальный текст') or new_row.get('Имя')}"
    
                report.append(msg)

                if index < 100:
                    send_telegram_message(msg)

            if report:
                report_filename = os.path.splitext(data_filename)[0] + "_report.txt"
                report_file = os.path.join(DATA_FOLDER, report_filename)
                with open(report_file, "w", encoding="utf-8") as f:
                    report = "\n----------\n".join(report)
                    f.write(report)

                if report_file:
                    send_telegram_file(report_file)

        else:
            logging.info("Первый запуск. Нет данных для сравнения.")
            summary += "\n\n📂 Первый запуск. Нет данных для сравнения."

            send_telegram_message(summary)
            send_telegram_file(data_filepath)


    except Exception as e:
        logging.error("Произошла ошибка:", exc_info=True)
        send_telegram_message(f"❌ Ошибка при парсинге данных: {str(e)}")

    logging.info("=== Завершение работы ===")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер реестра РФМ (fedsfm.ru)")
    args = parser.parse_args()

    parse_data()
