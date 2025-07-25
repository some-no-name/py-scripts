import logging, re, requests, pandas as pd
from bs4 import BeautifulSoup
from config import PARSE_URL
from utils import clean_field, generate_record_id

# ─────────────────────────────────────────────────────────────────────────────
def _parse_person_text(text: str):
    """
    Разбор одной строки РФМ-списка → кортеж значений.
    """
    terrorist = "*" in text

    text = re.sub(r'[\r\n]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    num_match = re.match(r"(\d+)\.", text)
    number = num_match.group(1) if num_match else "-"

    body = text[num_match.end():].strip() if num_match else text.strip()

    dob_match = re.search(r"(\d{2}\.\d{2}\.\d{4}) г\.р\.", body)
    birth = dob_match.group(1) if dob_match else "-"

    alias_match = re.search(r"\(([^()]*)\)", body)
    alias = clean_field(alias_match.group(1)) if alias_match else "-"

    cleaned = body
    if alias_match:
        cleaned = cleaned.replace(alias_match.group(0), "")
    if dob_match:
        cleaned = cleaned.replace(dob_match.group(0), "")

    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    name = clean_field(parts[0].replace("*", "")) if parts else "-"

    other = ""
    if dob_match:
        other = body[dob_match.end():].strip()
    elif len(parts) > 1:
        other = ", ".join(parts[1:]).strip()
    other = clean_field(other) or "-"

    birth = clean_field(birth)
    return number, name, alias, birth, other, terrorist, text

# ─────────────────────────────────────────────────────────────────────────────
def fetch_rfm_list() -> pd.DataFrame:
    logging.info("Fetching RFM list…")
    resp = requests.get(
        PARSE_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=60,
        verify=False,        # сайт с самоподписанным сертификатом
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    section = soup.select_one("#russianFL")
    if not section:
        raise ValueError("Секция #russianFL не найдена")

    persons = section.find_all("li")
    data, id_counter = [], {}
    for idx, li in enumerate(persons, 1):
        parsed = _parse_person_text(li.get_text(strip=True))
        n, name, alias, dob, other, terr, raw = parsed

        key = (name.lower(), dob)
        id_counter[key] = id_counter.get(key, 0) + 1
        rec_id = generate_record_id(name, dob, id_counter[key] - 1)
        data.append([rec_id, n, name, alias, dob, other, terr, raw])

        if idx % 100 == 0:
            logging.info("RFM-парсер: обработано %d человек…", idx)

    df = pd.DataFrame(
        data,
        columns=[
            "ID",
            "Порядковый номер",
            "Имя",
            "Доп Имя",
            "Дата рождения",
            "Остальные данные",
            "Террорист",
            "Изначальный текст",
        ],
    )
    logging.info("RFM-парсер: всего %d записей", len(df))
    return df
