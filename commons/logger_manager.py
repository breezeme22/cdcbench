import logging
import os


class LoggerManager:
    def __init__(self, module_name, log_level=logging.INFO):

        self.level = log_level
        self.module_name = module_name

    def get_logger(self):

        formatter = logging.Formatter("%(asctime)s [%(module)s][%(levelname)s] %(message)s")

        file_handler = logging.FileHandler("./cdcbench.log")

        file_handler.setFormatter(formatter)
        logger = logging.getLogger(self.module_name)
        logger.setLevel(self.level)
        logger.addHandler(file_handler)

        return logger
