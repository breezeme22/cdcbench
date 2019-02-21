import logging
import os


class LoggerManager:

    __logs_dir = "logs"

    @classmethod
    def get_logger(cls, module_name, log_level):

        __log_file_name = "cdcbench.log"

        if not os.path.isdir(cls.__logs_dir):
            os.mkdir(cls.__logs_dir)

        formatter = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(cls.__logs_dir, __log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger = logging.getLogger(module_name)

        logger.setLevel(log_level)

        logger.addHandler(file_handler)

        return logger

    @classmethod
    def get_sql_logger(cls, log_level):

        __log_file_name = "sql.log"

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(cls.__logs_dir, __log_file_name), encoding="utf-8")
        file_handler.setFormatter(formatter)

        sql_logger = logging.getLogger('sqlalchemy.engine')

        sql_logger.setLevel(log_level)

        sql_logger.addHandler(file_handler)

        return sql_logger

    @classmethod
    def get_pool_logger(cls, log_level):

        __log_file_name = "pool.log"

        formatter = logging.Formatter("%(asctime)s [%(module)s][%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(__log_file_name)
        file_handler.setFormatter(formatter)

        pool_logger = logging.getLogger('sqlalchemy.pool')

        pool_logger.setLevel(log_level)

        pool_logger.addHandler(file_handler)

        return pool_logger
