import logging

from src.config import CSV_FILE_PATH, setup_logging
from src.extract import extract
from src.transform import transform
from src.load import load
from src.exceptions import ExtractionError, TransformationError, LoadError

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    logger.info("Запуск ETL-пайплайна")

    try:
        df = extract(CSV_FILE_PATH)
        df = transform(df)
        load(df)
    except (ExtractionError, TransformationError, LoadError):
        logger.exception("ETL-пайплайн завершился с ошибкой.")
        raise

    logger.info("ETL-пайплайн успешно завершен")


if __name__ == "__main__":
    main()