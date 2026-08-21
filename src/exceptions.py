class ETLException(Exception):
    """Базовое исключение ETL"""


class ExtractException(ETLException):
    """Ошибка извлечения"""


class TransformException(ETLException):
    """Ошибка трансформации"""


class LoadException(ETLException):
    """Ошибка загрузки"""
