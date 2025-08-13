import logging

from telegram_notify import send_message

logging.basicConfig(level=logging.INFO)

send_message(f"TESTTTTT")

logging.info("info")
logging.debug("debug")
logging.error("error")
logging.warning("warning")


