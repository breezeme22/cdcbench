import configparser
import logging


class LoggerManager:
    def __init__(self, level=logging.INFO):
        self.level = level

    def set_level(self, level):
        if isinstance(level, (int, float, complex)):
            self.level = level
        # CRITICAL < ERROR < WARNING < INFO < DEBUG
        elif str(level) == level:
            if level.upper() == 'CRITICAL':
                self.level = logging.CRITICAL
            elif level.upper() == 'ERROR':
                self.level = logging.ERROR
            elif level.upper() == 'WARNING':
                self.level = logging.WARNING
            elif level.upper() == 'INFO':
                self.level = logging.INFO
            elif level.upper() == 'DEBUG':
                self.level = logging.DEBUG
            else:
                raise ValueError("Log Level not a valid : %r" % level)
        else:
            raise ValueError("Log Level type not a valid string: %r" % level)

        return self.level

    def get_logger(self):

        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

        file_handler = logging.FileHandler('./cdcbench.log')

        file_handler.setFormatter(formatter)

        logger = logging.getLogger()
        logger.setLevel(self.level)
        logger.addHandler(file_handler)

        return logger


class ConfigLoad:

    def __init__(self, config_name='conf/default.ini'):

        self.config = configparser.ConfigParser()
        self.config_name = config_name

        self.logger = LoggerManager()

        self.rdbms = None
        self.user_id = None
        self.user_password = None
        self.host_name = None
        self.port = None
        self.instance_name = None

        self.update_total_data = None
        self.update_commit_unit = None
        self.delete_total_data = None
        self.delete_commit_unit = None

        # config file read 하는 함수가 set_config_load에서 수행됨으로 호출
        self.set_config_load(self.config_name)

    def set_config_load(self, config_name):

        self.config.clear()
        self.config.read(config_name)

        self.logger.set_level(str(self.config['cdcbench_setting']['log_level']).upper())

        self.rdbms = self.config['db_connection']['rdbms']
        self.user_id = self.config['db_connection']['user_id']
        self.user_password = self.config['db_connection']['user_password']
        self.host_name = self.config['db_connection']['host_name']
        self.port = self.config['db_connection']['port']
        self.instance_name = self.config['db_connection']['instance_name']

        self.update_total_data = self.config['init_data']['update_total_data']
        self.update_commit_unit = self.config['init_data']['update_commit_unit']
        self.delete_total_data = self.config['init_data']['delete_total_data']
        self.delete_commit_unit = self.config['init_data']['delete_commit_unit']

    def get_config_load(self):
        return self

    def view_cdcbench_setting_config(self):
        return "[CONFIG CDCBENCH SETTING INFORMATION] \n" \
               "  LOG_LEVEL: " + logging.getLevelName(self.logger.level) + "\n"

    def get_connection_info(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        return self.rdbms + '://' + self.user_id + ':' + self.user_password + '@' + \
               self.host_name + ':' + self.port + '/' + self.instance_name

    def view_connection_config(self):
        return "[CONFIG CONNECTION INFORMATION] \n" \
               "  HOST_NAME: " + self.host_name + "\n" \
               "  PORT: " + self.port + "\n" \
               "  RDBMS: " + self.rdbms + "\n" \
               "  INSTANCE_NAME: " + self.instance_name + "\n" \
               "  USER_ID: " + self.user_id + "\n" \
               "  USER_PASSWORD: " + self.user_password + "\n"

    def get_init_data_info(self):
        return {"update_total_data": self.update_total_data,
                "update_commit_unit": self.update_commit_unit,
                "delete_total_data": self.delete_total_data,
                "delete_commit_unit": self.delete_commit_unit}

    def view_init_data_config(self):
        return "[CONFIG INITIALIZE INFORMATION] \n" \
               "  UPDATE_TOTAL_DATA: " + self.update_total_data + "\n" \
               "  UPDATE_COMMIT_UNIT: " + self.update_commit_unit + "\n" \
               "  DELETE_TOTAL_DATA: " + self.delete_total_data + "\n" \
               "  DELETE_COMMIT_UNIT: " + self.delete_commit_unit
