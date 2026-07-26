import logging
import pandas as pd
from src.exceptions import ExtractionError

logger = logging.getLogger(__name__)


def extract(file_path):
    logger.info("Чтение CSV-файла: %s", file_path)

    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError as e:
        logger.error("CSV-файл не найден: %s", file_path)
        raise ExtractionError(f"CSV-файл не найден: {file_path}") from e
    except pd.errors.EmptyDataError as e:
        logger.error("CSV-файл полностью пуст: %s", file_path)
        raise ExtractionError(f"CSV-файл полностью пуст: {file_path}") from e
    except Exception as e:
        logger.exception("Ошибка при чтении CSV-файла: %s", file_path)
        raise ExtractionError(f"Ошибка при чтении CSV-файла: {file_path}") from e

    if df.empty:
        logger.warning("CSV-файл не содержит данных: %s", file_path)
        raise ExtractionError(f"CSV-файл не содержит данных: {file_path}")

    logger.info("Успешно извлечено %d строк.", len(df))
    return df