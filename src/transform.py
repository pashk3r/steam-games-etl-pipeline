import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array_remove,
    col,
    dayofmonth,
    month,
    split,
    to_date,
    trim,
    year,
    translate,
    transform as Transform,
)

from src.exceptions import TransformException

logger = logging.getLogger(__name__)


def drop_missing_name(df: DataFrame) -> DataFrame:
    logger.info("Удаление строк у которых Name пустое")

    try:
        line_counter_before = df.count()
        df = df.filter((col("Name").isNotNull()) & (trim(col("Name")) != ""))
        line_counter_after = df.count()
        line_count_deleted = line_counter_before - line_counter_after

        logger.info(f"Удалено строк с пустым Name: {line_count_deleted}")
        return df
    except Exception as e:
        logger.error(f"Ошибка при удалении строк с пустым Name: {e}")
        raise TransformException(f"Не удалось удалить строки с пустым Name: {e}") from e


def split_release_date(df: DataFrame) -> DataFrame:
    logger.info('Разбиение "Release date" на 3 колонки')

    try:
        df = df.withColumn("Release date", to_date(col("Release date"), "MMM d, yyyy"))
        df = df.withColumn("Release day", dayofmonth(col("Release date")))
        df = df.withColumn("Release month", month(col("Release date")))
        df = df.withColumn("Release year", year(col("Release date")))
        df = df.drop("Release date")

        logger.info('Разбиение "Release date" произошло успешно')
        return df
    except Exception as e:
        logger.error(f'Ошибка при разбиении "Release date": {e}')
        raise TransformException(f'Не удалось разбить "Release date": {e}') from e


def split_estimated_owners(df: DataFrame) -> DataFrame:
    logger.info('Разбиение "Estimated owners" на 2 колонки')

    try:
        owners = split(col("Estimated owners"), " - ")

        df = df.withColumn("Estimated owners min", trim(owners.getItem(0)).cast("int"))
        df = df.withColumn("Estimated owners max", trim(owners.getItem(1)).cast("int"))
        df = df.drop("Estimated owners")

        logger.info('Разбиение "Estimated owners" произошло успешно')
        return df
    except Exception as e:
        logger.error(f'Ошибка при разбиении "Estimated owners": {e}')
        raise TransformException(f'Не удалось разбить "Estimated owners": {e}') from e


def string_to_array(df: DataFrame, column_name: str) -> DataFrame:
    logger.info(f'Преобразование колонки "{column_name}" из строки в массив')

    try:
        df = df.withColumn(column_name, array_remove(Transform(split(col(column_name), ","), trim), ""))

        logger.info(f'Успешное преобразование колонки "{column_name}" из строки в массив')
        return df
    except Exception as e:
        logger.error(f'Ошибка при преобразовании колонки "{column_name}" в массив: {e}')
        raise TransformException(f'Не удалось преобразовать колонку "{column_name}" в массив: {e}') from e


def list_string_to_array(df: DataFrame, column_name: str) -> DataFrame:
    logger.info(f'Преобразование колонки "{column_name}" в массив')

    try:
        df = df.withColumn(column_name, translate(col(column_name), "[]'", ""))
        df = df.withColumn(column_name, array_remove(Transform(split(col(column_name), ","), trim), ""))

        logger.info(f'Колонка "{column_name}" успешно преобразована в массив')
        return df
    except Exception as e:
        logger.error(f'Ошибка при преобразовании "{column_name}" в массив: {e}')
        raise TransformException(f'Не удалось преобразовать "{column_name}" в массив: {e}') from e


def drop_column(df: DataFrame, column_name: str) -> DataFrame:
    logger.info(f'Удаление "{column_name}"')

    try:
        df = df.drop(column_name)

        logger.info(f'Колонка "{column_name}" успешно удалена')
        return df
    except Exception as e:
        logger.error(f'Ошибка при удалении "{column_name}": {e}')
        raise TransformException(f'Не удалось удалить "{column_name}": {e}') from e


def transform(df: DataFrame) -> DataFrame:
    STRING_TO_ARRAY_COLUMNS = ["Screenshots", "Tags", "Genres", "Categories", "Developers", "Publishers"]
    LIST_STRING_TO_ARRAY_COLUMNS = ["Supported languages", "Full audio languages"]

    rows_count = df.count()
    if rows_count == 0:
        raise TransformException("Получен пустой датафрейм")

    logger.info(f"Начинаю трансформацию: {rows_count} строк")

    df = drop_missing_name(df)
    df = split_release_date(df)
    df = split_estimated_owners(df)
    df = drop_column(df, "Movies")

    for column in STRING_TO_ARRAY_COLUMNS:
        df = string_to_array(df, column)

    for column in LIST_STRING_TO_ARRAY_COLUMNS:
        df = list_string_to_array(df, column)
    
    logger.info(f"Трансформация завершена: {rows_count} строк")
    return df
