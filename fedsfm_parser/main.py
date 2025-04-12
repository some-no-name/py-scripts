import os
import re
import hashlib
import argparse
import traceback
import requests
import pandas as pd
import logging

from bs4 import BeautifulSoup
from datetime import datetime

from consts import LOGS_FOLDER, DATA_FOLDER, PARSE_URL, LOGS_LEVEL


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


def generate_person_id(name: str, birth_date: str) -> str:
    """
    Создаёт стабильный ID на основе ФИО и даты рождения.
    Пример: hash MD5(ФИО+Дата). Берём первые 8 символов, чтобы было короче.
    Если для одного человека не меняются ФИО и дата рождения,
    то ID всегда будет одним и тем же.
    """
    unique_string = (name + birth_date).strip().lower()
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()[:8]


def parse_data():
    os.makedirs(DATA_FOLDER, exist_ok=True)

    data_filename = datetime.now().strftime("data_%Y_%m_%d__%H_%M_%S.csv")
    data_diff_filename = datetime.now().strftime("diff_%Y_%m_%d__%H_%M_%S.txt")
    prev_filename = find_previous_data_file(data_filename)

    try:
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
            try:
                logging.debug(f"Person text: {text}")

                terrorist = '*' in text
                number_match = re.match(r'(\d+)\.', text)
                number = number_match.group(1) if number_match else ""
                birth_date_match = re.search(r'(\d{2}\.\d{2}\.\d{4}) г.р.', text)
                birth_date = birth_date_match.group(1) if birth_date_match else ""
                birth_date = clean_field(birth_date)
                name = text[number_match.end():birth_date_match.start()].strip() if number_match and birth_date_match else ""
                name = name.replace('*', '')
                name = clean_field(name)
                other_data = text[birth_date_match.end():].strip() if birth_date_match else ""
                other_data = clean_field(other_data)
                person_id = generate_person_id(name, birth_date)

                data.append([person_id, number, name, birth_date, other_data, terrorist])

                logging.debug(f"Person record: {','.join([person_id, number, name, birth_date, other_data, 'True' if terrorist else 'False'])}")

                person_parse_ok += 1

            except Exception as e:
                logging.warning(f"Ошибка при обработке строки: {e}")
                data.append(["Ошибка", "", text, "", "", ""])
                person_parse_errors += 1

        if not data:
            raise ValueError("Данные не найдены на странице.")

        logging.info(f"Результаты парсинга count: {persons_list_count}, ok: {person_parse_ok}, err: {person_parse_errors}")


        df = pd.DataFrame(data, columns=["ID", "Порядковый номер", "Имя", "Дата рождения", "Остальные данные", "Террорист"])
        df.to_csv(f"{DATA_FOLDER}/{data_filename}", index=False, encoding='utf-8')
        logging.info(f"Новая выгрузка сохранена в файл {data_filename}")


        if prev_filename and os.path.exists(prev_filename):
            old_df = pd.read_csv(prev_filename, dtype=str)
            new_df = pd.read_csv(f"{DATA_FOLDER}/{data_filename}", dtype=str)
            report = compare_with_previous(old_df, new_df)
            if report.strip():
                logging.info("Изменения по сравнению с предыдущим запуском:")
                logging.info(report)



                # data_diff_filename

            else:
                logging.info("Изменений нет.")
        else:
            logging.info("Это первый запуск. Предыдущих данных для сравнения нет.")

        # df.to_csv(PREVIOUS_CSV, index=False, encoding='utf-8')

    except Exception:
        logging.error("Произошла ошибка:", exc_info=True)


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

    for i in common_ids:
        # if not new_data[i].equals(old_data[i]):
        if not all(str(a).strip() == str(b).strip() for a, b in zip(new_data[i], old_data[i])):
            lines.append(f"\nИзменения в записи ID={i}:\n")
            lines.append(f"Было:\n{old_data[i]}")
            lines.append(f"Стало:\n{new_data[i]}")

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Парсер реестра РФМ (fedsfm.ru)")
    args = parser.parse_args()

    log_path = setup_logger()
    logging.info("=== Запуск парсера данных РФМ ===")
    logging.info(f"Лог сохраняется в: {log_path}")
    parse_data()
    logging.info("=== Завершение работы ===")
