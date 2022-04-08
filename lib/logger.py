
import datetime
import logging
import logging.handlers
import multiprocessing
import os
import threading

from sqlalchemy import event
from sqlalchemy.engine import Engine
from typing import Any, NoReturn, List, Tuple, Dict, Union

from lib.globals import *

LOG_DIRECTORY = "logs"

_log_file_name = "cdcbench.log"
_sql_log_file_name = "sql.log"

sql_logging_to_log_level = {"NONE": logging.NOTSET, "SQL": logging.INFO, "ALL": logging.DEBUG}


# https://stackoverflow.com/questions/14844970/modifying-logging-message-format-based-on-message-logging-level-in-python3
class _SQLFormatter(logging.Formatter):
    info_fmt = "%(asctime)s.%(msecs)d [%(processName)s][SQL] %(message)s"
    dbg_fmt = "%(asctime)s.%(msecs)d [%(processName)s][DATA] %(message)s"

    def __init__(self):
        super().__init__(fmt="%(levelno)d: %(msg)s", datefmt="%Y-%m-%d %H:%M:%S", style='%')

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


class LogManager:

    log_level: str
    sql_logging: str

    def configure_logger(self, log_level: str, sql_logging: str) -> NoReturn:

        self.log_level = log_level
        self.sql_logging = sql_logging

        cb_logger = logging.getLogger(CDCBENCH)
        cb_logger.setLevel(self.log_level)

        if not cb_logger.hasHandlers():
            handler = logging.FileHandler(os.path.join(LOG_DIRECTORY, _log_file_name), encoding="utf-8")
            fmt = logging.Formatter("%(asctime)s.%(msecs)d [%(processName)s][%(levelname)s] %(message)s",
                                    "%Y-%m-%d %H:%M:%S")
            handler.setFormatter(fmt)
            cb_logger.addHandler(handler)

        sql_logger = logging.getLogger(SQL)
        sql_logger.setLevel(sql_logging_to_log_level[self.sql_logging])

        if not sql_logger.hasHandlers():

            sql_handler = logging.FileHandler(os.path.join(LOG_DIRECTORY, _sql_log_file_name), encoding="utf-8")
            sql_handler.setFormatter(_SQLFormatter())
            sql_logger.addHandler(sql_handler)


@event.listens_for(Engine, "before_cursor_execute")
def _sql_logging(conn, cursor, statement, parameters, context, executemany):

    sql_logger = logging.getLogger(SQL)
    # logger = logging.getLogger(CDCBENCH)

    def data_formatting(data: Any) -> Any:
        formatted_data: str
        dialect_name_upper = conn.dialect.name.upper()

        # logger.debug(f"[{type(data)}] {data}")
        if isinstance(data, (datetime.datetime, datetime.date, datetime.time)):
            if dialect_name_upper == ORACLE:
                if isinstance(data, datetime.datetime):
                    if data.microsecond == 0:
                        formatted_data = f"TO_DATE('{str(data)}', 'YYYY-MM-DD HH24:MI.SS')"
                    else:
                        formatted_data = f"TO_TIMESTAMP('{str(data)}', 'YYYY-MM-DD HH24:MI.SS.FF9')"
                elif isinstance(data, datetime.date):
                    formatted_data = f"TO_DATE('{str(data)}', 'YYYY-MM-DD')"
                else:  # isinstance(data, datetime.time)
                    formatted_data = f"TO_TIMESTAMP('{str(data)}', 'HH24:MI:SS.FF9')"
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
        elif isinstance(data, bytes):
            formatted_data = f"'{data.hex()}'"
        elif data is None:
            formatted_data = "null"
        else:
            formatted_data = data
        return formatted_data

    def make_row_data_format(row_data) -> str:

        result = "( "
        if isinstance(row_data, (tuple, list)):
            result += ", ".join((data_formatting(col_data) for col_data in row_data))
        else:  # dict
            result += ", ".join((data_formatting(row_data[column_name]) for column_name in row_data))
        result += " )"
        return result

    if "SELECT" not in statement.upper():

        sql_logger.info(statement)

        # Multi DML check
        if any(dml in statement for dml in ["INSERT", "UPDATE", "DELETE"]):
            # logger.debug(f"[{type(parameters)}] {parameters}")
            if isinstance(parameters, (list, tuple)):
                for row in parameters:
                    sql_logger.debug(make_row_data_format(row))
            else:   # Single row
                sql_logger.debug(make_row_data_format(parameters))
