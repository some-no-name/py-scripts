import logging, time, re
from telegram import Bot
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

FLOOD_WAIT = 1
MAX_TRIES = 7

def extract_wait_seconds(exc) -> int | None:
    m = re.search(r"retry after (\d+)", str(exc).lower())
    return int(m.group(1))+1 if m else None

def _send(callable_):
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    tries, delay = 0, FLOOD_WAIT
    while tries < MAX_TRIES:
        try:
            callable_(bot)
            return
        except Exception as e:
            tries += 1
            if "retry after" in str(e).lower():
                wait = extract_wait_seconds(e) or delay
                logging.warning("Flood control. Waiting %s s", wait)
                time.sleep(wait)
                delay *= 2
            else:
                logging.error("Telegram error: %s", e)
                break

def send_message(text: str):
    logging.info("SEND MESSAGE: %s", text)
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logging.warning("Telegram credentials missing")
        return
    _send(lambda b: b.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=text))

def send_document(path: str):
    logging.info("SEND FILE: %s", path)
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logging.warning("Telegram credentials missing")
        return
    _send(lambda b: b.send_document(chat_id=TELEGRAM_CHANNEL_ID, document=open(path, 'rb')))