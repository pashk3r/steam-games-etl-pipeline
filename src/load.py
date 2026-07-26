import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from src.config import TABLE_NAME, get_db_url
from src.exceptions import LoadError

logger = logging.getLogger(__name__)


def load(df):
    if df.empty:
        raise LoadError("Получен пустой DataFrame")

    logger.info("Подключение к PostgreSQL")

    try:
        engine = create_engine(get_db_url())
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace",
            index=False
        )
    except SQLAlchemyError as e:
        logger.exception("Не удалось загрузить данные в PostgreSQL")
        raise LoadError("Не удалось загрузить данные в PostgreSQL") from e

    logger.info("Успешно загружено %d строк в таблицу %s",len(df), TABLE_NAME)