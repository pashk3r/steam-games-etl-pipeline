import logging
import re
import pandas as pd

from src.exceptions import TransformationError

logger = logging.getLogger(__name__)

REVIEW_PATTERN = re.compile(
    r"^(?P<summary>[^,]+),\((?P<count>[\d,]+)\),(?:\*,)?-\s\*(?P<positive_pct>\d+)%"
)

INSUFFICIENT_REVIEWS_PATTERN = re.compile(
    r"^(?P<count>\d+) user reviews?,-\s\*Need more"
)

FREE_LABELS = {"free", "free to play", "play for free!"}

LIST_COLUMNS = ["popular_tags", "game_details", "languages", "genre"]


def drop_invalid_types(df):
    removed = df["types"].isna().sum()

    if removed:
        logger.info("Удалено %d строк с пустым types", removed)

    return df.dropna(subset=["types"])


def parse_price(value):
    if pd.isna(value):
        return None

    text = str(value).strip()

    if text.lower() in FREE_LABELS:
        return 0.0

    if text.startswith("$"):
        try:
            return float(text[1:].replace(",", ""))
        except ValueError:
            logger.warning("Не удалось распарсить цену: %r", value)

    return None


def parse_reviews(value):
    if pd.isna(value):
        return {"summary": None, "count": None, "positive_pct": None}

    text = str(value).strip()

    match = REVIEW_PATTERN.match(text)
    if match:
        return {
            "summary": match.group("summary").strip(),
            "count": int(match.group("count").replace(",", "")),
            "positive_pct": int(match.group("positive_pct")),
        }

    match = INSUFFICIENT_REVIEWS_PATTERN.match(text)
    if match:
        return {
            "summary": "Insufficient reviews",
            "count": int(match.group("count")),
            "positive_pct": None,
        }
    logger.warning("Не удалось распарсить reviews: %r", value)
    return {"summary": None, "count": None, "positive_pct": None}


def parse_list_column(value):
    if pd.isna(value):
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def transform_prices(df):
    df["original_price"] = df["original_price"].apply(parse_price)
    df["discount_price"] = df["discount_price"].apply(parse_price)
    df["is_free"] = df["original_price"] == 0.0
    return df


def transform_reviews(df):
    for column, prefix in (("recent_reviews", "recent"), ("all_reviews", "all")):
        parsed = df[column].apply(parse_reviews).apply(pd.Series)
        df[f"{prefix}_review_summary"] = parsed["summary"]
        df[f"{prefix}_review_count"] = parsed["count"]
        df[f"{prefix}_review_positive_pct"] = parsed["positive_pct"]

    return df.drop(columns=["recent_reviews", "all_reviews"])


def transform_release_date(df):
    parsed = pd.to_datetime(
        df["release_date"],
        format="%b %d, %Y",
        errors="coerce"
    )

    invalid = parsed.isna() & df["release_date"].notna()

    if invalid.any():
        logger.warning("Не удалось распарсить release_date у %d строк", invalid.sum())

    df["release_date"] = parsed
    return df


def transform_list_columns(df):
    for column in LIST_COLUMNS:
        df[column] = df[column].apply(parse_list_column)
    return df


def transform_achievements(df):
    df["achievements"] = df["achievements"].astype("Int64")
    return df


def transform_mature_content(df):
    df["mature_content"] = df["mature_content"].fillna("")
    return df


def transform(df):
    if df.empty:
        raise TransformationError("Получен пустой DataFrame")

    logger.info("Начинаю трансформацию: %d строк", len(df))

    df = df.copy()

    df = drop_invalid_types(df)
    df = transform_prices(df)
    df = transform_reviews(df)
    df = transform_release_date(df)
    df = transform_list_columns(df)
    df = transform_achievements(df)
    df = transform_mature_content(df)

    logger.info("Трансформация завершена: %d строк", len(df))
    return df
