
import logging
from typing import Dict
from modules.telegram_queue import TelegramQueue

# mtype: MatchType

def notify(
        action: str,
        rfm_row: Dict,
        mtype: str,
        score: int,
        air_row: dict,
        report,
        tq: TelegramQueue,
        test_mode = False,
        ):
    logging.info(f"[Notify]: {action}, {mtype}, {score}, {rfm_row}")
    if test_mode:
        return  # не шумим
    sym = {"add": "🟢", "del": "🔴", "changed": "🟡", "match_all": "🔁"}[action]
    fio = rfm_row.get("Имя") or rfm_row.get("Name", "?")
    dob = rfm_row.get("Дата рождения") or rfm_row.get("✦Дата рождения", "?")
    rfm_terrorist = is_terrorist(rfm_row)
    air_terrorist = is_terrorist(air_row)

    msg = f"{sym} {fio} ({dob}) — {action} [{mtype}, {score}];"
    msg += f"\nAIR: {'✅' if air_terrorist else '❌'}; "
    msg += f"RFM: {'✅' if rfm_terrorist else '❌'}"
    msg += f"\nRFM:\n{str(rfm_row.get('Изначальный текст'))}"
    msg += f"\n\nAIR:\n{_dict_to_string(air_row)}"

    report.append(msg)

    tq.send_message(msg)

    # send_message(f"{sym} {fio} ({dob}) — {action} [{mtype}, {score}];\nRFM: {str(rfm_row.get("Изначальный текст"))}\n\nAIR:\n{_dict_to_string(mrow)}")

def _dict_to_string(obj: dict):
    result = []
    skip_keys = set(["Manual sort", "_air_id"])

    for key, value in obj.items():
        if key not in skip_keys:
            result.append(f"{key}: {value}")
    return "\n".join(result)

def is_terrorist(row: dict) -> bool:
    for fld in ["Террорист", "✦Росфинмониторинг"]:
        val = row.get(fld, None)
        if val != None and isinstance(val, str):
            return val == 'True'
        if val != None and isinstance(val, bool):
            return val

    return False
