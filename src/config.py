import logging
import os
from dotenv import load_dotenv

load_dotenv()

CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "data/raw/steam_games.csv")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "steam_games")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TABLE_NAME = os.getenv("TABLE_NAME", "games")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/etl.log")


def setup_logging():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode='w', encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def validate_config():
    required = {
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }

    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Не заданы переменные окружения! Пересмотри .env")


def get_db_url():
    validate_config()
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

