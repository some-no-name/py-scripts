import ast
from collections import defaultdict
from datetime import datetime
import logging, re, unicodedata, pandas as pd
from rapidfuzz import fuzz, process
from typing import Literal, Tuple, List, Dict

from notify_tools import is_terrorist, notify
from storage import get_region_dict

MatchType = Literal["full", "partial", "none"]
region_dict = get_region_dict()

missed_regions = set()

# ─────────────────────────────────────────────────────────────────────────────
def _parse_id_list(id_string: str) -> List[str]:
    try:
        if not isinstance(id_string, str):
            return []
        result = ast.literal_eval(id_string)
        if isinstance(result, list):
            return [str(item).strip() for item in result]
    except Exception as e:
        print('id_string', id_string)
        print(f"Error parsing id list: {e}")
    return []

def _norm(text: str) -> str:
    """
    Приводим строку к верхнему регистру + убираем диакритику и лишние пробелы.
    """
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\s]", " ", text, flags=re.U)
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def _fio_tokens(row: dict) -> List[str]:
    """
    Возвращает токены ФИО (без порядок значения).
    Для Airtable по умолчанию поля начинаются с «✦ ».
    """
    fio = []
    for fld in ["Фамилия", "✦ Фамилия", "Имя", "✦Имя", "Отчество", "✦Отчество", "✦Второе имя/имя на иностранном языке", "Доп Имя"]:
        val = row.get(fld, "")
        if isinstance(val, str):
            fio.append(val)

    fio = " ".join(fio)

    return _norm(fio).strip()

def _get_possible_nan_val(obj, field):
    if field in obj:
        val = obj.get(field)
        if isinstance(val, str):
            return str(val).strip()

    return ""

def _region(row: dict) -> str:
    """
    Приводим регион к нормализованной строке для сопоставления.
    """

    res = []

    if "✦Регион, где возбудили УД/задержали" in row:
        reg = row.get("✦Регион, где возбудили УД/задержали")
        for reg_id in _parse_id_list(reg):
            if reg_id in region_dict:
                res.append(_norm(region_dict[reg_id][0]))
                for reg_option in region_dict[reg_id][1]:
                    res.append(_norm(reg_option))
            else:
                if reg_id not in missed_regions:
                    logging.warning(f"REGION IS MISSED IN THE DICTIONATY: {reg_id}")
                    missed_regions.add(reg_id)

    else:
        reg = row.get("Остальные данные", "")
        if reg:
            res.append(_norm(reg))

    return res


def match_quality(rfm_row: dict, air_row: dict) -> Tuple[MatchType, int]:
    """
    Возвращает тип совпадения и числовой «вес» (для выбора лучшего).
    full      – 3 из 3: ДР, ФИО, регион
    partial   – 2 из 3
    none      – 1 / 0 из 3
    """
    # FIO
    score_fio = fuzz.token_set_ratio(" ".join(_fio_tokens(air_row)),
                                     " ".join(_fio_tokens(rfm_row) ))
    ok_fio  = score_fio >= 85

    # DOB
    air_dob = _get_possible_nan_val(air_row, "✦Дата рождения")
    rfm_dob = _get_possible_nan_val(rfm_row, "Дата рождения")
    ok_dob  = normalize_and_compare_dobs(air_dob, rfm_dob)

    # REGION
    air_region = _region(air_row)
    rfm_region = _region(rfm_row)

    score_region = 0
    region_best_tuple = (None, None)

    if len(air_region) > 0 and len(rfm_region) > 0:
        for check_region in air_region:
            ratio = fuzz.token_set_ratio(rfm_region[0], check_region)
            if ratio > score_region:
                score_region = ratio
                region_best_tuple = (rfm_region[0], check_region)

    ok_region  = score_region >= 85

    # name is more important than region so multiply on 2
    total_score = score_fio * 2 + score_region + (100 if ok_dob else 0)

    total_ok = sum((ok_fio, ok_dob, ok_region))
    if total_ok == 3:
        return "full", total_score
    if total_ok == 2:
        return "partial", total_score
    return "none", total_score


def normalize_and_compare_dobs(date1: str, date2: str) -> bool:
    formats = ["%Y-%m-%d", "%d.%m.%Y"]

    def parse_date(date_str):
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Unsupported date format: {date_str}")

    try:
        if date1 and date2 and len(date1) > 2 and len(date2) > 2:
            d1 = parse_date(date1)
            d2 = parse_date(date2)
            return d1 == d2
        
        return False
    except ValueError as e:
        print(e)
        return False

# ─────────────────────────────────────────────────────────────────────────────
def find_best_match(
    target_row: dict,
    airtable_df: pd.DataFrame,
) -> Tuple[MatchType, Dict | None, int]:
    """
    Для одной строки RFM ищем лучший вариант в Airtable.
    """
    best_type, best_score, best_row = "none", 0, None

    for _, air_row in airtable_df.iterrows():
        mtype, fscore = match_quality(target_row, air_row.to_dict())
        if mtype == "none":
            continue
        if (mtype == "full" and best_type != "full") or (
            mtype == best_type and fscore > best_score
        ):
            best_type, best_score, best_row = mtype, fscore, air_row.to_dict()
            # best_row["✦Регион, где возбудили УД/задержали"] = _region(best_row)

            # «full» нельзя перебить — можно прервать цикл
            if best_type == "full":
                break
    return best_type, best_row, best_score

def compare_all_mapped(df_new, airtable_df, report):
    index = 0
    matches_all: List[Dict] = []

    def name_keys(row):
        # Take first N chars of the normalized FIO tokens as a crude filter
        tokens = _fio_tokens(row)
        tokens = tokens.split(" ")
        return tokens

    # Build index for airtable_df
    airtable_index = defaultdict(list)
    for _, row in airtable_df.iterrows():
        row_dict = row.to_dict()
        keys = name_keys(row_dict)
        for key in keys:
            airtable_index[key].append(row_dict)

    # For each row in df_new, only compare to similar name keys
    for _, rfm_row in df_new.iterrows():
        index += 1
        rfm_row_dict = rfm_row.to_dict()
        candidates = []
        keys = name_keys(rfm_row_dict)
        for key in keys:
            candidates += airtable_index.get(key, [])
        # candidates = list(set(candidates))

        best_type, best_score, best_row = "none", 0, None
        for air_row in candidates:
            mtype, fscore = match_quality(rfm_row_dict, air_row)
            if mtype == "none":
                continue
            if (mtype == "full" and best_type != "full") or (
                mtype == best_type and fscore > best_score
            ):
                best_type, best_score, best_row = mtype, fscore, air_row
                if best_type == "full":
                    break

        terr_match = ""
        if best_type != "none" and best_row:
            notify("match_all", rfm_row, best_type, best_score, best_row, report)
        
        logging.info(f"Best match all ({index}/{len(df_new)}) {terr_match}: {best_type}({best_score})\nRFM: {rfm_row.get("Изначальный текст")}\nAIR: {best_row}")
        

    return matches_all

def compare_all_slow(df_new, airtable_df, report):
    matches_all = []

    for _, rfm_row in df_new.iterrows():
        index += 1
        rfm_row = rfm_row.to_dict()
        mtype, mrow, score = find_best_match(rfm_row, airtable_df)
        logging.info(f"Best match all ({index}/{len(df_new)}): {mtype}({score})\nRFM: {rfm_row.get("Изначальный текст")}\nAIR: {mrow}")
        if mtype != "none" and mrow:
            rfm_terr = is_terrorist(rfm_row)
            air_terr = is_terrorist(mrow)

            msg = f"#мэтч (сравнение всех) [{"T" if rfm_terr else "F"}{"T" if air_terr else "F"}]"
            msg += f"\n{rfm_row.get('Изначальный текст') or rfm_row.get('Имя')}"
            msg += f"\n\n{mrow}"
            report.append(msg)

            matches_all.append({**rfm_row, **mrow})

    return matches_all
