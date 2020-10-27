import logging
import os


class LoggerManager:

    _logs_dir = "logs"
    _log_level = None
    _sql_log_level = None
    _pool_log_level = None

    @classmethod
    def set_log_level(cls, log_level):
        cls._log_level = log_level

    @classmethod
    def get_log_level(cls):
        return cls._log_level

    @classmethod
    def set_sql_log_level(cls, log_level):
        cls._sql_log_level = log_level

    @classmethod
    def get_sql_log_level(cls):
        return cls._sql_log_level

    @classmethod
    def set_pool_log_level(cls, log_level):
        cls._pool_log_level = log_level

    @classmethod
    def get_pool_log_level(cls):
        return cls._pool_log_level

    @classmethod
    def get_logger(cls, module_name):

        _log_file_name = "cdcbench.log"

        if not os.path.isdir(cls._logs_dir):
            os.mkdir(cls._logs_dir)

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(cls._logs_dir, _log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger = logging.getLogger(module_name)

        logger.setLevel(cls._log_level)

        if logger.hasHandlers() is False:
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def get_sql_logger(cls):

        if cls._sql_log_level != logging.WARNING:

            _log_file_name = "sql.log"

            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

            file_handler = logging.FileHandler(os.path.join(cls._logs_dir, _log_file_name), encoding="utf-8")
            file_handler.setFormatter(formatter)

            sql_logger = logging.getLogger('sqlalchemy.engine')

            sql_logger.setLevel(cls._sql_log_level)

            if sql_logger.hasHandlers() is False:
                sql_logger.addHandler(file_handler)

            return sql_logger

    @classmethod
    def get_sa_unsupported_dbms_sql_logger(cls, module_name):

        _log_file_name = "sql.log"

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(cls._logs_dir, _log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        sa_unsupported_dbms_sql_logger = logging.getLogger(module_name)

        sa_unsupported_dbms_sql_logger.setLevel(cls._sql_log_level)

        if sa_unsupported_dbms_sql_logger.hasHandlers() is False:
            sa_unsupported_dbms_sql_logger.addHandler(file_handler)

        return sa_unsupported_dbms_sql_logger

    @classmethod
    def get_pool_logger(cls):

        _log_file_name = "pool.log"

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(_log_file_name)
        file_handler.setFormatter(formatter)

        pool_logger = logging.getLogger('sqlalchemy.pool')

        pool_logger.setLevel(cls._pool_log_level)

        if pool_logger.hasHandlers() is False:
            pool_logger.addHandler(file_handler)

        return pool_logger
