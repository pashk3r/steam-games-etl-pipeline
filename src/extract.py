import logging

from pyspark.sql import SparkSession, DataFrame

from src.config import CSV_FILE_PATH
from src.exceptions import ExtractException

logger = logging.getLogger(__name__)


def fix_csv_header(file_path: str):
    logger.info("Начинается добавление запятой")

    try:
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            text = f.read()

        fixed_text = text.replace("DiscountDLC count", "Discount,DLC count", 1)

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            f.write(fixed_text)

        logger.info("Запятая успешно добавлена")
    except Exception as e:
        logger.exception("Ошибка при добавлении запятой")
        raise ExtractException("Не удалось добавить запятую") from e

    
def extract(spark: SparkSession) -> DataFrame:
    logger.info(f"Чтение CSV-файла: {CSV_FILE_PATH}")

    try:
        fix_csv_header(CSV_FILE_PATH)

        df = spark \
                .read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(CSV_FILE_PATH)
    except FileNotFoundError as e:
        logger.error(f"CSV-файл не найден: {CSV_FILE_PATH}")
        raise ExtractException(f"CSV-файл не найден: {CSV_FILE_PATH}") from e
    except Exception as e:
        logger.exception(f"Ошибка при чтении CSV-файла: {CSV_FILE_PATH}")
        raise ExtractException(f"Ошибка при чтении CSV-файла: {CSV_FILE_PATH}") from e

    if df.count() == 0:
        logger.warning(f"CSV-файл не содержит данных: {CSV_FILE_PATH}")
        raise ExtractException(f"CSV-файл не содержит данных: {CSV_FILE_PATH}")

    logger.info(f"Успешно извлечено {df.count()} строк")
    return df
