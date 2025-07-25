import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"
LOGS_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"
AIRTABLE_CACHE_DIR = BASE_DIR / "airtable_cache"

for p in (LOGS_DIR, DATA_DIR, AIRTABLE_CACHE_DIR):
    p.mkdir(parents=True, exist_ok=True)

PARSE_URL = "https://www.fedsfm.ru/documents/terrorists-catalog-portal-act"
KEEP_FILES_COUNT = int(os.getenv("KEEP_FILES_COUNT", 10))

# Telegram
TELEGRAM_BOT_TOKEN = None
# TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "5829942614:AAHoSXYs_calefF_EcI6snB7R6dkR6KegHw")
TELEGRAM_CHANNEL_ID = None
# TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "-1002254148040")

# Airtable
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "patQscz4OyuslD2jz.97137b63ccfef76c0d36c5878db025d38d1924493dda02c39ee71d9ecbde08ef")  # <= замените своим ключом
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "app42KQc45WUgqx7A")
AIRTABLE_TABLE_PERSECUTED = "tbl2Y2SQxCQJnH5TJ"
AIRTABLE_VIEW_MONITORING = "viw07mkkLXN17mZzm"

AIRTABLE_FIELDS_MAIN = [
    "Name", "✦ Фамилия", "✦Имя", "✦Отчество",
    "✦Второе имя/имя на иностранном языке", "✦пол",
    "✦Дата рождения", "✦Город",
    "✦Регион, где возбудили УД/задержали",
    "✦Росфинмониторинг", "✦Дата включения в список РФМ",
    "✦Дата исключения из списка РФМ", "Manual sort",
]

AIRTABLE_TABLE_REGIONS = "tblFgR19058Blt28i"
AIRTABLE_TABLE_CITIES = "tblCBynEJ1blhGSOm"

# Master dataset
MASTER_FILE = DATA_DIR / "rfm_master.csv"

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

ALLOWED_USERS_FILE = BASE_DIR / "src" / "allowed_users.csv"     # <-- список ID
HTML_TEMPLATE = BASE_DIR / "templates" / "report.html" # хранится внутри пакета