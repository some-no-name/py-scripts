import logging, threading, csv
from pathlib import Path
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

from config import TELEGRAM_BOT_TOKEN, ALLOWED_USERS_FILE
from main import run as run_parser   # импортируем функцию

# -------- utils -------------------------------------------------------
import re

def load_allowed_ids() -> set[int]:
    path = ALLOWED_USERS_FILE
    ids: set[int] = set()
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = re.sub(r"[^\d-]", "", line)   # оставляем только цифры/-
                if not line:
                    continue
                try:
                    ids.add(int(line))
                except ValueError:
                    logging.warning("Bad ID in allowed_users.csv: %r", line)
    except FileNotFoundError:
        logging.warning("File %s not found — никто не авторизован", path)
    logging.info("Allowed IDs loaded (%s): %s", path, ids)
    return ids

ALLOWED_IDS = load_allowed_ids()

def is_allowed(user_id: int) -> bool:
    return user_id in ALLOWED_IDS

# ---------- handlers ---------------------------------------------------
def start(update: Update, ctx: CallbackContext):
    uid = update.effective_user.id
    if not is_allowed(uid):
        update.message.reply_text("Нет доступа.")
        return
    kb = ReplyKeyboardMarkup([["1","0"]], one_time_keyboard=True, resize_keyboard=True)
    update.message.reply_text("Сравнить весь список? 1 — да, 0 — нет", reply_markup=kb)
    ctx.user_data["await_choice"] = True

def handle_choice(update: Update, ctx: CallbackContext):
    if not ctx.user_data.get("await_choice"):
        return
    choice = update.message.text.strip()
    compare_all = choice == "1"
    update.message.reply_text("Запускаю скрипт… это займёт пару минут.")
    ctx.user_data["await_choice"] = False

    # запускаем в отдельном потоке, чтобы бот не завис
    threading.Thread(target=run_parser, kwargs={"compare_all": compare_all}, daemon=True).start()

# ---------- main loop --------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO)
    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("rfm", start))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_choice))
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
