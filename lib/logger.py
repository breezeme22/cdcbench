
import datetime
import logging
import os

from sqlalchemy import event
from sqlalchemy.engine import Engine
from typing import Any

from lib.globals import *

LOG_DIRECTORY = "logs"


class LoggerManager:

    log_level: str
    sql_logging: str
    _sql_log_level: int

    @classmethod
    def get_logger(cls, module_name: str) -> logging.Logger:

        _log_file_name = "cdcbench.log"

        if not os.path.isdir(LOG_DIRECTORY):
            os.mkdir(LOG_DIRECTORY)

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(LOG_DIRECTORY, _log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger = logging.getLogger(module_name)

        logger.setLevel(cls.log_level)

        if not logger.hasHandlers():
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def get_sql_logger(cls, module_name: str) -> logging.Logger:

        _sql_log_file_name = "sql.log"

        # formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        formatter = _SQLFormatter()

        file_handler = logging.FileHandler(os.path.join(LOG_DIRECTORY, _sql_log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        sql_logger = logging.getLogger(module_name)

        sql_logging_to_log_level = {"NONE": logging.NOTSET, "SQL": logging.INFO, "ALL": logging.DEBUG}
        cls._sql_log_level = sql_logging_to_log_level[cls.sql_logging]
        sql_logger.setLevel(cls._sql_log_level)

        if not sql_logger.hasHandlers():
            sql_logger.addHandler(file_handler)

        return sql_logger


# https://stackoverflow.com/questions/14844970/modifying-logging-message-format-based-on-message-logging-level-in-python3
class _SQLFormatter(logging.Formatter):
    info_fmt = "%(asctime)s [SQL] %(message)s"
    dbg_fmt = "%(asctime)s [DATA] %(message)s"

    def __init__(self):
        super().__init__(fmt="%(levelno)d: %(msg)s", datefmt=None, style='%')

    def format(self, record):

        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.DEBUG:
            self._style._fmt = _SQLFormatter.dbg_fmt

        elif record.levelno == logging.INFO:
            self._style._fmt = _SQLFormatter.info_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._style._fmt = format_orig

        return result


@event.listens_for(Engine, "before_cursor_execute")
def _sql_logging(conn, cursor, statement, parameters, context, executemany):
    logger = LoggerManager.get_logger(__name__)
    sql_logger = LoggerManager.get_sql_logger("sql")

    def data_formatting(data: Any) -> Any:
        formatted_data: str
        dialect_name_upper = conn.dialect.name.upper()

        # print(f"[{type(data)}] {data}")
        if isinstance(data, (datetime.datetime, datetime.date, datetime.time)):
            if dialect_name_upper == ORACLE:
                if isinstance(data, datetime.date):
                    formatted_data = f"TO_DATE('{str(data)}', 'YYYY-MM-DD')"
                elif isinstance(data, datetime.time):
                    formatted_data = f"TO_TIMESTAMP('{str(data)}', 'HH24:MI:SS.FF9')"
                else:   # datetime.datetime
                    if data.microsecond == 0:
                        formatted_data = f"TO_DATE('{str(data)}', 'YYYY-MM-DD HH24:MI.SS')"
                    else:
                        formatted_data = f"TO_TIMESTAMP('{str(data)}', 'YYYY-MM-DD HH24:MI.SS.FF9')"
            else:
                formatted_data = f"'{str(data)}'"
        elif isinstance(data, str):
            formatted_data = f"'{data}'"
        elif isinstance(data, int):
            formatted_data = str(data)
        elif isinstance(data, float):
            split_data = str(data).split(".")
            if split_data[1] == "0":
                formatted_data = split_data[0]
            else:
                formatted_data = str(data)
        elif data is None:
            formatted_data = "null"
        else:
            formatted_data = data
        return formatted_data

    def make_row_data_format(row_data) -> str:

        result = "( "
        if isinstance(row_data, tuple):
            result += ", ".join((data_formatting(col_data) for col_data in row_data))
        else:
            result += ", ".join((data_formatting(row_data[col_data]) for col_data in row_data))
        result += " )"
        return result

    if "SELECT" not in statement:

        sql_logger.info(statement)

        # Multi DML check
        if any(dml in statement for dml in ["INSERT", "UPDATE", "DELETE"]):
            logger.info(f"Multi operation: {executemany}")

            if LoggerManager.sql_logging == "ALL":
                if isinstance(parameters, list):
                    for row in parameters:
                        sql_logger.debug(make_row_data_format(row))
                else:   # Single row
                    sql_logger.debug(make_row_data_format(parameters))
