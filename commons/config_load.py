import configparser
import logging
import os
import sys


class ConfigLoad:

    def __init__(self, config_name="default.ini"):

        self.config = configparser.ConfigParser()
        self.config_name = None

        self.log_level = None

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
        self.set_config_load(config_name)

    # config 존재 유무 검사
    def set_config_name(self, config_name):
        if os.path.isfile(os.path.join(os.getcwd(), config_name)):
            self.config_name = config_name
        else:
            raise ValueError("The specified config file (%r) does not exist." % config_name)

        return self.config_name

    # log level 유효성 검사
    def set_log_level(self, log_level):
        if isinstance(log_level, (int, float, complex)):
            self.log_level = log_level
        # CRITICAL < ERROR < WARNING < INFO < DEBUG
        elif str(log_level) == log_level:
            if log_level.upper() == "CRITICAL":
                self.log_level = logging.CRITICAL
            elif log_level.upper() == "ERROR":
                self.log_level = logging.ERROR
            elif log_level.upper() == "WARNING":
                self.log_level = logging.WARNING
            elif log_level.upper() == "INFO":
                self.log_level = logging.INFO
            elif log_level.upper() == "DEBUG":
                self.log_level = logging.DEBUG
            else:
                raise ValueError("Log Level not a valid : %r" % log_level)
        else:
            raise ValueError("Log Level type not a valid string: %r" % log_level)

        return self.log_level

    def set_config_load(self, config_name):

        # 현재 경로가 ~ ~/cdcbench/conf Directory로 Working Directory 변경
        os.chdir(os.path.join(os.getcwd(), "conf"))

        self.config.clear()
        self.config.read(self.set_config_name(config_name))

        self.log_level = self.set_log_level(self.config["setting"]["log_level"])

        self.host_name = self.config["source_database"]["host_name"]
        self.port = self.config["source_database"]["port"]
        self.db_type = self.config["source_database"]["db_type"]
        self.db_name = self.config["source_database"]["db_name"]
        self.user_id = self.config["source_database"]["user_id"]
        self.user_password = self.config["source_database"]["user_password"]

        self.update_total_data = self.config["initial_update_test_data"]["total_num_of_data"]
        self.update_commit_unit = self.config["initial_update_test_data"]["commit_unit"]

        self.delete_total_data = self.config["initial_delete_test_data"]["total_num_of_data"]
        self.delete_commit_unit = self.config["initial_delete_test_data"]["commit_unit"]

        # curdir이 ~/cdcbench/conf일 경우 ~/cdcbench로 Working Directory 변경
        if os.path.basename(os.getcwd()) == "conf":
            os.chdir(os.path.pardir)

    # def get_config_load(self):
    #     return self

    def view_setting_config(self):
        return " [CONFIG SETTING INFORMATION] \n" \
               "  LOG LEVEL: " + logging.getLevelName(self.log_level) + "\n"

    def get_connection_info(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        return self.db_type + "://" + self.user_id + ":" + self.user_password + "@" + \
               self.host_name + ":" + self.port + "/" + self.db_name

    def view_connection_config(self):
        return " [CONFIG SOURCE DATABASE INFORMATION] \n" \
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
