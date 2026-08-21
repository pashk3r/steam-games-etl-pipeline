import logging

from pyspark.sql import SparkSession

from src.config import setup_logging, validate_config, SPARK_APP_NAME, SPARK_MASTER
from src.extract import extract
from src.transform import transform
from src.load import load
from src.exceptions import ExtractException, TransformException, LoadException

logger = logging.getLogger(__name__)


def main():
    setup_logging()
    validate_config()

    logger.info("Запуск пайплайна")

    spark = SparkSession \
                    .builder \
                    .appName(SPARK_APP_NAME) \
                    .master(SPARK_MASTER) \
                    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                    .getOrCreate()

    try:
        df = extract(spark)
        df = transform(df)
        load(df)
    except (ExtractException, TransformException, LoadException):
        logger.exception("Пайплайн завершился с ошибкой")
        raise
    finally:
        spark.stop()

    logger.info("Пайплайн успешно завершен")


if __name__ == "__main__":
    main()