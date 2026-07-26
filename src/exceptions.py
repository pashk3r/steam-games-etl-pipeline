class ETLError(Exception):
    """Базовое исключение ETL"""


class ExtractionError(ETLError):
    """Ошибка извлечения"""


class TransformationError(ETLError):
    """Ошибка трансформации"""


class LoadError(ETLError):
    """Ошибка загрузки"""
