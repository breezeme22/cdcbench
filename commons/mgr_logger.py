import logging
import os


class LoggerManager:

    __logs_dir = "logs"
    __log_level = None
    __sql_log_level = None
    __pool_log_level = None

    @classmethod
    def set_log_level(cls, log_level):
        cls.__log_level = log_level

    @classmethod
    def get_log_level(cls):
        return cls.__log_level

    @classmethod
    def set_sql_log_level(cls, log_level):
        cls.__sql_log_level = log_level

    @classmethod
    def get_sql_log_level(cls):
        return cls.__sql_log_level

    @classmethod
    def set_pool_log_level(cls, log_level):
        cls.__pool_log_level = log_level

    @classmethod
    def get_pool_log_level(cls):
        return cls.__pool_log_level

    @classmethod
    def get_logger(cls, module_name):

        __log_file_name = "cdcbench.log"

        if not os.path.isdir(cls.__logs_dir):
            os.mkdir(cls.__logs_dir)

        formatter = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(cls.__logs_dir, __log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger = logging.getLogger(module_name)

        logger.setLevel(cls.__log_level)

        if logger.hasHandlers() is False:
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def get_sql_logger(cls):

        if cls.__sql_log_level != logging.WARNING:

            __log_file_name = "sql.log"

            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

            file_handler = logging.FileHandler(os.path.join(cls.__logs_dir, __log_file_name), encoding="utf-8")
            file_handler.setFormatter(formatter)

            sql_logger = logging.getLogger('sqlalchemy.engine')

            sql_logger.setLevel(cls.__sql_log_level)

            if sql_logger.hasHandlers() is False:
                sql_logger.addHandler(file_handler)

            return sql_logger

    @classmethod
    def get_pool_logger(cls):

        __log_file_name = "pool.log"

        formatter = logging.Formatter("%(asctime)s [%(module)s][%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(__log_file_name)
        file_handler.setFormatter(formatter)

        pool_logger = logging.getLogger('sqlalchemy.pool')

        pool_logger.setLevel(cls.__pool_log_level)

        if pool_logger.hasHandlers() is False:
            pool_logger.addHandler(file_handler)

        return pool_logger
