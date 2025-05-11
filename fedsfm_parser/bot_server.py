import logging
import os
import ssl
import telegram
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters

from main import parse_data  # <-- импортируем твою старую функцию
from consts import TELEGRAM_BOT_TOKEN  # <-- токен бота

# Настроим логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# /start команда
def start(update: Update, context: CallbackContext):
    keyboard = [
        ["🚀 Запустить парсинг"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    update.message.reply_text('Привет! Нажми кнопку для запуска парсинга.', reply_markup=reply_markup)


# Обработчик сообщений
def handle_message(update: Update, context: CallbackContext):
    text = update.message.text
    chat_id = update.effective_chat.id

    if text == "🚀 Запустить парсинг":
        context.bot.send_message(chat_id=chat_id, text="🔄 Парсинг запущен...")
        try:
            parse_data()
        except Exception as e:
            context.bot.send_message(chat_id=chat_id, text=f"❌ Ошибка при парсинге: {e}")
    else:
        update.message.reply_text('Неизвестная команда. Нажмите на кнопку.')


def main():
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dispatcher = updater.dispatcher

    # Регистрируем команды и обработчики
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    # Запускаем
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
