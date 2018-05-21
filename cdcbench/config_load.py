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

        self.host_name = None
        self.port = None
        self.db_type = None
        self.db_name = None
        self.user_id = None
        self.user_password = None

        self.update_total_data = None
        self.update_commit_unit = None
        self.delete_total_data = None
        self.delete_commit_unit = None

        # config file read 하는 함수가 set_config_load에서 수행됨으로 호출
        self.set_config_load(self.config_name)

    def set_config_load(self, config_name):

        self.config.clear()
        self.config.read(config_name)

        self.logger.set_level(str(self.config['setting']['log_level']).upper())

        self.host_name = self.config['database']['host_name']
        self.port = self.config['database']['port']
        self.db_type = self.config['database']['db_type']
        self.db_name = self.config['database']['db_name']
        self.user_id = self.config['database']['user_id']
        self.user_password = self.config['database']['user_password']

        self.update_total_data = self.config['initial_update_test_data']['total_num_of_data']
        self.update_commit_unit = self.config['initial_update_test_data']['commit_unit']

        self.delete_total_data = self.config['initial_delete_test_data']['total_num_of_data']
        self.delete_commit_unit = self.config['initial_delete_test_data']['commit_unit']

    # def get_config_load(self):
    #     return self

    def view_setting_config(self):
        return " [CONFIG SETTING INFORMATION] \n" \
               "  LOG LEVEL: " + logging.getLevelName(self.logger.level) + "\n"

    def get_connection_info(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        return self.db_type + '://' + self.user_id + ':' + self.user_password + '@' + \
               self.host_name + ':' + self.port + '/' + self.db_name

    def view_connection_config(self):
        return " [CONFIG DATABASE INFORMATION] \n" \
               "  HOST NAME: " + self.host_name + "\n" \
               "  PORT: " + self.port + "\n" \
               "  DB TYPE: " + self.db_type + "\n" \
               "  DB NAME: " + self.db_name + "\n" \
               "  USER ID: " + self.user_id + "\n" \
               "  USER PASSWORD: " + self.user_password + "\n"

    def get_init_data_info(self):
        return {"update_total_data": self.update_total_data,
                "update_commit_unit": self.update_commit_unit,
                "delete_total_data": self.delete_total_data,
                "delete_commit_unit": self.delete_commit_unit}

    def view_init_data_config(self):
        return " [CONFIG INITIALIZE DATA INFORMATION] \n" \
               "  UPDATE_TEST TOTAL DATA: " + self.update_total_data + "\n" \
               "  UPDATE_TEST COMMIT UNIT: " + self.update_commit_unit + "\n" \
               "  DELETE_TEST TOTAL DATA: " + self.delete_total_data + "\n" \
               "  DELETE_TEST COMMIT UNIT: " + self.delete_commit_unit

