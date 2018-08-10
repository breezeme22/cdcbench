import logging


class LoggerManager:

    @staticmethod
    def get_logger(module_name, log_level):

        formatter = logging.Formatter("%(asctime)s [%(module)s][%(levelname)s] %(message)s")

        file_handler = logging.FileHandler("cdcbench.log")
        file_handler.setFormatter(formatter)

        logger = logging.getLogger(module_name)

        logger.setLevel(log_level)

        logger.addHandler(file_handler)

        return logger
