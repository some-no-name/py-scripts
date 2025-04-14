import logging

from telegram import Bot
from consts import TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

def send_telegram_message(message: str):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
            logging.debug("Creating tg bot...")
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message)
        else:
            logging.debug("No creds for tg bot")
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")

def send_telegram_file(file_path):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID:
            logging.debug("Creating tg bot...")
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            with open(file_path, 'rb') as file:
                bot.send_document(chat_id=TELEGRAM_CHANNEL_ID, document=file)
        else:
            logging.debug("No creds for tg bot")
    except Exception as e:
        logging.error(f"Не удалось отправить файл {file_path} в Telegram: {e}")
