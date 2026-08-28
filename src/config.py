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
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SteamGamesETL")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")


def setup_logging():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
        raise ValueError("Не заданы переменные окружения!")


def get_db_url():
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"