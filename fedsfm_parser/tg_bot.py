import logging
import time

from telegram import Bot
from consts import TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

FLOOD_WAIT_SECONDS = 1  # base delay between messages
MAX_TRIES = 7

def send_telegram_message(message: str):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
            logging.debug("Creating tg bot...")
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            success = False
            try_number = 1
            delay = FLOOD_WAIT_SECONDS
            while not success:
                try:
                    try_number += 1
                    logging.debug("Entering: send_message")
                    bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message)
                    logging.debug("Exiting: send_message")
                    success = True
                except Exception as e:
                    if ("retry after" in str(e).lower() or "flood" in str(e).lower()) and try_number < MAX_TRIES:
                        wait = extract_retry_seconds(e) or delay
                        logging.warning(f"Flood control triggered. Waiting {wait} seconds...")
                        time.sleep(wait)
                        delay *= 2  # exponential backoff
                    else:
                        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")
                        break
        else:
            logging.debug("No creds for tg bot")
    except Exception as e:
        logging.error(f"Ошибка при работе с Telegram API: {e}")


def send_telegram_file(file_path):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
            logging.debug("Creating tg bot...")
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            success = False
            try_number = 1
            delay = FLOOD_WAIT_SECONDS
            while not success:
                try:
                    try_number += 1
                    with open(file_path, 'rb') as file:
                        bot.send_document(chat_id=TELEGRAM_CHANNEL_ID, document=file)
                    success = True
                except Exception as e:
                    if ("retry after" in str(e).lower() or "flood" in str(e).lower()) and try_number < MAX_TRIES:
                        wait = extract_retry_seconds(e) or delay
                        logging.warning(f"Flood control (file) triggered. Waiting {wait} seconds...")
                        time.sleep(wait)
                        delay *= 2
                    else:
                        logging.error(f"Не удалось отправить файл {file_path} в Telegram: {e}")
                        break
        else:
            logging.debug("No creds for tg bot")
    except Exception as e:
        logging.error(f"Ошибка при работе с Telegram API (file): {e}")


def extract_retry_seconds(exception) -> int:
    # Try to extract number of seconds from Telegram exception text
    import re
    match = re.search(r"retry in (\d+(\.\d+)?)", str(exception))
    if match:
        return int(float(match.group(1))) + 1
    return None
