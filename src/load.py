import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import ArrayType

from src.config import DB_PASSWORD, DB_USER, TABLE_NAME, get_db_url
from src.exceptions import LoadException

logger = logging.getLogger(__name__)


def load(df: DataFrame):
    if df.count() == 0:
        raise LoadException("Получен пустой DataFrame")

    logger.info("Подготовка данных к загрузке в PostgreSQL")

    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, concat_ws(",", col(field.name)))

    logger.info("Загрузка данных в PostgreSQL через JDBC")

    try:
        df \
            .write \
            .format("jdbc") \
            .option("url", get_db_url()) \
            .option("dbtable", TABLE_NAME) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
    except Exception as e:
        logger.exception("Не удалось загрузить данные в PostgreSQL")
        raise LoadException("Не удалось загрузить данные в PostgreSQL") from e

    logger.info(f"Успешно загружено {df.count()} строк в таблицу {TABLE_NAME}")