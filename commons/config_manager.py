import configparser
import logging
import os

# Config Module Variables
CONFIG = None


class ConfigManager(object):

    def __init__(self, config_name="default.ini"):

        # 현재 경로가 ~ ~/cdcbench/conf Directory로 Working Directory 변경
        os.chdir(os.path.join(os.getcwd(), "conf"))

        self.config = configparser.ConfigParser()
        self.config.clear()

        self.config_name = config_name
        self.config.read(self.config_name, encoding="utf-8")

        self.log_level = self.config["setting"]["log_level"]
        self.nls_lang = self.config["setting"]["nls_lang"]

        self.source_host_name = self.config["source_database"]["host_name"]
        self.source_port = self.config["source_database"]["port"]
        self.source_db_type = self.config["source_database"]["db_type"]
        self.source_db_name = self.config["source_database"]["db_name"]
        self.source_user_id = self.config["source_database"]["user_id"]
        self.source_user_password = self.config["source_database"]["user_password"]

        self.target_host_name = self.config["target_database"]["host_name"]
        self.target_port = self.config["target_database"]["port"]
        self.target_db_type = self.config["target_database"]["db_type"]
        self.target_db_name = self.config["target_database"]["db_name"]
        self.target_user_id = self.config["target_database"]["user_id"]
        self.target_user_password = self.config["target_database"]["user_password"]

        self.update_total_num_of_data = self.config["initial_update_test_data"]["total_num_of_data"]
        self.update_commit_unit = self.config["initial_update_test_data"]["commit_unit"]
        self.delete_total_num_of_data = self.config["initial_delete_test_data"]["total_num_of_data"]
        self.delete_commit_unit = self.config["initial_delete_test_data"]["commit_unit"]

        global CONFIG
        CONFIG = self

        # curdir이 ~/cdcbench/conf일 경우 ~/cdcbench로 Working Directory 변경
        if os.path.basename(os.getcwd()) == "conf":
            os.chdir(os.path.pardir)

    def __repr__(self):
        return str({"setting": {"log_level": logging.getLevelName(self.log_level), "nls_lang": self.nls_lang},
                    "source_database": {"host_name": self.source_host_name, "port": self.source_port,
                                        "db_type": self.source_db_type, "db_name": self.source_db_name,
                                        "user_id": self.source_user_id, "user_password": self.source_user_password},
                    "target_database": {"host_name": self.target_host_name, "port": self.target_port,
                                        "db_type": self.target_db_type, "db_name": self.target_db_name,
                                        "user_id": self.target_user_id, "user_password": self.target_user_password},
                    "initial_update_test_data": {"total_num_of_data": self.update_total_num_of_data,
                                                 "commit_unit": self.update_commit_unit},
                    "initial_delete_test_data": {"total_num_of_data": self.delete_total_num_of_data,
                                                 "commit_unit": self.delete_commit_unit}
                    })

    @property
    def config_name(self):
        return self.__config_name

    # config 존재 유무 검사
    @config_name.setter
    def config_name(self, config_name):
        if os.path.isfile(os.path.join(os.getcwd(), config_name)):
            self.__config_name = config_name
        else:
            raise FileNotFoundError("The specified configuration file ({}) does not exist.".format(config_name))

    @property
    def log_level(self):
        return self.__log_level

    # log_level 유효성 검사
    @log_level.setter
    def log_level(self, log_level):
        if isinstance(log_level, (int, float)):
            self.__log_level = log_level
        # CRITICAL < ERROR < WARNING < INFO < DEBUG
        elif str(log_level) == log_level:
            if log_level.upper() == "CRITICAL":
                self.__log_level = logging.CRITICAL
            elif log_level.upper() == "ERROR":
                self.__log_level = logging.ERROR
            elif log_level.upper() == "WARNING":
                self.__log_level = logging.WARNING
            elif log_level.upper() == "INFO":
                self.__log_level = logging.INFO
            elif log_level.upper() == "DEBUG":
                self.__log_level = logging.DEBUG
            else:
                raise ValueError("Configuration value 'Log Level' not a valid : {}".format(log_level))
        else:
            raise ValueError("Configuration value 'Log Level' type not a valid string: {}".format(log_level))

    @property
    def nls_lang(self):
        return self.__nls_lang

    @nls_lang.setter
    def nls_lang(self, nls_lang):
        self.__nls_lang = nls_lang
        os.putenv("NLS_LANG", nls_lang)

    @property
    def source_host_name(self):
        return self.__source_host_name

    @source_host_name.setter
    def source_host_name(self, host_name):
        self.__source_host_name = host_name

    @property
    def source_port(self):
        return self.__source_port

    @source_port.setter
    def source_port(self, port):
        self.__source_port = port

    @property
    def source_db_type(self):
        return self.__source_db_type

    @source_db_type.setter
    def source_db_type(self, db_type):
        self.__source_db_type = db_type

    @property
    def source_db_name(self):
        return self.__source_db_name

    @source_db_name.setter
    def source_db_name(self, db_name):
        self.__source_db_name = db_name

    @property
    def source_user_id(self):
        return self.__source_user_id

    @source_user_id.setter
    def source_user_id(self, user_id):
        self.__source_user_id = user_id

    @property
    def source_user_password(self):
        return self.__source_user_password

    @source_user_password.setter
    def source_user_password(self, user_password):
        self.__source_user_password = user_password

    @property
    def target_host_name(self):
        return self.__target_host_name

    @target_host_name.setter
    def target_host_name(self, host_name):
        self.__target_host_name = host_name

    @property
    def target_port(self):
        return self.__target_port

    @target_port.setter
    def target_port(self, port):
        self.__target_port = port

    @property
    def target_db_type(self):
        return self.__target_db_type

    @target_db_type.setter
    def target_db_type(self, db_type):
        self.__target_db_type = db_type

    @property
    def target_db_name(self):
        return self.__target_db_name

    @target_db_name.setter
    def target_db_name(self, db_name):
        self.__target_db_name = db_name

    @property
    def target_user_id(self):
        return self.__target_user_id

    @target_user_id.setter
    def target_user_id(self, user_id):
        self.__target_user_id = user_id

    @property
    def target_user_password(self):
        return self.__target_user_password

    @target_user_password.setter
    def target_user_password(self, user_password):
        self.__target_user_password = user_password

    @property
    def update_total_num_of_data(self):
        return self.__update_total_num_of_data

    @update_total_num_of_data.setter
    def update_total_num_of_data(self, update_total_num_of_data):
        if update_total_num_of_data.isnumeric():
            self.__update_total_num_of_data = int(update_total_num_of_data)
        else:
            raise ValueError(
                "Configuration value 'total_num_of_data' is not a numeric value: {}".format(update_total_num_of_data))

    @property
    def update_commit_unit(self):
        return self.__update_commit_unit

    @update_commit_unit.setter
    def update_commit_unit(self, update_commit_unit):
        if update_commit_unit.isnumeric():
            self.__update_commit_unit = int(update_commit_unit)
        else:
            raise ValueError("Configuration value 'total_num_of_data' is not a numeric value: {}".format(update_commit_unit))

    @property
    def delete_total_num_of_data(self):
        return self.__delete_total_num_of_data

    @delete_total_num_of_data.setter
    def delete_total_num_of_data(self, delete_total_num_of_data):
        if delete_total_num_of_data.isnumeric():
            self.__delete_total_num_of_data = int(delete_total_num_of_data)
        else:
            raise ValueError(
                "Configuration value 'total_num_of_data' is not a numeric value: {}".format(delete_total_num_of_data))

    @property
    def delete_commit_unit(self):
        return self.__delete_commit_unit

    @delete_commit_unit.setter
    def delete_commit_unit(self, delete_commit_unit):
        if delete_commit_unit.isnumeric():
            self.__delete_commit_unit = int(delete_commit_unit)
        else:
            raise ValueError("Configuration value 'total_num_of_data' is not a numeric value: {}".format(delete_commit_unit))

    def view_setting_config(self):
        return " [CONFIG SETTING INFORMATION] \n" \
               "  LOG LEVEL: " + logging.getLevelName(self.log_level) + "\n" \
               "  NLS_LANG: " + self.nls_lang + "\n"

    def get_source_connection_string(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        return self.source_db_type + "://" + self.source_user_id + ":" + self.source_user_password + "@" + \
            self.source_host_name + ":" + self.source_port + "/" + self.source_db_name

    def get_target_connection_string(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        return self.target_db_type + "://" + self.target_user_id + ":" + self.target_user_password + "@" + \
            self.target_host_name + ":" + self.target_port + "/" + self.target_db_name

    def view_source_connection_config(self):
        return " [CONFIG SOURCE DATABASE INFORMATION] \n" \
               "  HOST NAME: " + self.source_host_name + "\n" \
               "  PORT: " + self.source_port + "\n" \
               "  DB TYPE: " + self.source_db_type + "\n" \
               "  DB NAME: " + self.source_db_name + "\n" \
               "  USER ID: " + self.source_user_id + "\n" \
               "  USER PASSWORD: " + self.source_user_password + "\n"

    def get_init_data_info(self):
        return {"update_total_data": self.update_total_num_of_data,
                "update_commit_unit": self.update_commit_unit,
                "delete_total_data": self.delete_total_num_of_data,
                "delete_commit_unit": self.delete_commit_unit}

    def view_init_data_config(self):
        return " [CONFIG INITIALIZE DATA INFORMATION] \n" \
               "  UPDATE_TEST TOTAL DATA: " + str(self.update_total_num_of_data) + "\n" \
               "  UPDATE_TEST COMMIT UNIT: " + str(self.update_commit_unit) + "\n" \
               "  DELETE_TEST TOTAL DATA: " + str(self.delete_total_num_of_data) + "\n" \
               "  DELETE_TEST COMMIT UNIT: " + str(self.delete_commit_unit)

    @staticmethod
    def get_config():
        global CONFIG
        return CONFIG
