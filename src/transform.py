import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    to_date,
    lit,
    regexp_extract,
    regexp_replace,
    split,
    transform as spark_transform,
    array_remove,
    lower,
    trim,
)

from src.exceptions import TransformException

logger = logging.getLogger(__name__)

FREE_LABELS = ["free", "free to play", "play for free!"]
LIST_COLUMNS = ["popular_tags", "game_details", "languages", "genre"]
PRICE_COLUMNS = ["original_price", "discount_price"]
REVIEW_COLUMNS = [("recent_reviews", "recent"), ("all_reviews", "all")]


def _clean_prices(df: DataFrame) -> DataFrame:
    for price_col in PRICE_COLUMNS:
        clean = trim(col(price_col))
        df = df.withColumn(
            price_col,
            when(lower(clean).isin(FREE_LABELS), lit(0.0))
            .when(
                clean.startswith("$"), regexp_replace(clean, r"^\$", "").cast("double")
            )
            .otherwise(lit(None).cast("double")),
        )
    return df.withColumn("is_free", col("original_price") == 0.0)


def _parse_review_column(df: DataFrame, src_col: str, prefix: str) -> DataFrame:
    src = col(src_col)
    need_more = src.like("%Need more%")

    summary = when(need_more, lit("Insufficient reviews")).otherwise(
        regexp_extract(src, r"^([^,]+)", 1)
    )
    count_expr = when(need_more, regexp_extract(src, r"^(\d+)", 1)).otherwise(
        regexp_extract(src, r"\(([\d,]+)\)", 1)
    )

    df = df.withColumn(
        f"{prefix}_review_summary", when(summary == "", lit(None)).otherwise(summary)
    )
    df = df.withColumn(
        f"{prefix}_review_count", regexp_replace(count_expr, ",", "").cast("int")
    )
    df = df.withColumn(
        f"{prefix}_review_positive_pct",
        when(need_more, lit(None).cast("int")).otherwise(
            regexp_extract(src, r"(\d+)%", 1).cast("int")
        ),
    )
    return df.drop(src_col)


def _parse_reviews(df: DataFrame) -> DataFrame:
    for src_col, prefix in REVIEW_COLUMNS:
        df = _parse_review_column(df, src_col, prefix)
    return df


def _parse_release_date(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "release_date", to_date(trim(col("release_date")), "MMM d, yyyy")
    )


def _clean_list_columns(df: DataFrame) -> DataFrame:
    for column in LIST_COLUMNS:
        df = df.withColumn(
            column,
            array_remove(
                spark_transform(split(col(column), ","), lambda x: trim(x)), ""
            ),
        )
    return df


def _clean_achievements(df: DataFrame) -> DataFrame:
    return df.withColumn("achievements", trim(col("achievements")).cast("int"))


def _clean_mature_content(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "mature_content",
        when(col("mature_content").isNull(), lit("")).otherwise(col("mature_content")),
    )


def transform(df: DataFrame) -> DataFrame:
    df = df.dropna(subset=["types"])

    row_count = df.count()
    if row_count == 0:
        raise TransformException("Получен пустой датафрейм")

    logger.info(f"Начинаю трансформацию: {row_count} строк")

    df = _clean_prices(df)
    df = _parse_reviews(df)
    df = _parse_release_date(df)
    df = _clean_list_columns(df)
    df = _clean_achievements(df)
    df = _clean_mature_content(df)

    logger.info(f"Трансформация завершена: {row_count} строк")
    return df
