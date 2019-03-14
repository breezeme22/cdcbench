from commons.constants import *

import configparser
import logging
import os
import texttable
import cx_Oracle

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
        self.sql_log_level = logging.WARNING
        self.sql_logging = self.config["setting"]["sql_logging"]
        self.nls_lang = self.config["setting"]["nls_lang"]
        # self.lob_save = self.config["setting"]["lob_save"]

        self.source_host_name = self.config["source_database"]["host_name"]
        self.source_port = self.config["source_database"]["port"]
        self.source_dbms_type = self.config["source_database"]["dbms_type"]
        self.source_db_name = self.config["source_database"]["db_name"]
        self.source_schema_name = self.config["source_database"]["schema_name"]
        self.source_user_id = self.config["source_database"]["user_id"]
        self.source_user_password = self.config["source_database"]["user_password"]

        self.target_host_name = self.config["target_database"]["host_name"]
        self.target_port = self.config["target_database"]["port"]
        self.target_dbms_type = self.config["target_database"]["dbms_type"]
        self.target_db_name = self.config["target_database"]["db_name"]
        self.target_schema_name = self.config["target_database"]["schema_name"]
        self.target_user_id = self.config["target_database"]["user_id"]
        self.target_user_password = self.config["target_database"]["user_password"]

        self.update_number_of_data = self.config["initial_update_test_data"]["number_of_data"]
        self.update_commit_unit = self.config["initial_update_test_data"]["commit_unit"]
        self.delete_number_of_data = self.config["initial_delete_test_data"]["number_of_data"]
        self.delete_commit_unit = self.config["initial_delete_test_data"]["commit_unit"]

        global CONFIG
        CONFIG = self

        # curdir이 ~/cdcbench/conf일 경우 ~/cdcbench로 Working Directory 변경
        if os.path.basename(os.getcwd()) == "conf":
            os.chdir(os.path.pardir)

    def __repr__(self):
        return {
            "setting": {
                "log_level": logging.getLevelName(self.log_level),
                "sql_logging": self.sql_logging,
                "nls_lang": self.nls_lang
                # , "lob_save": self.lob_save
            },
            "source_database": {
                "host_name": self.source_host_name,
                "port": self.source_port,
                "dbms_type": self.get_dbms_alias(self.source_dbms_type),
                "db_name": self.source_db_name,
                "schema_name": self.source_schema_name,
                "user_id": self.source_user_id,
                "user_password": self.source_user_password
            },
            "target_database": {
                "host_name": self.target_host_name,
                "port": self.target_port,
                "dbms_type": self.get_dbms_alias(self.target_dbms_type),
                "db_name": self.target_db_name,
                "schema_name": self.target_schema_name,
                "user_id": self.target_user_id,
                "user_password": self.target_user_password
            },
            "initial_update_test_data": {
                "number_of_data": self.update_number_of_data,
                "commit_unit": self.update_commit_unit
            },
            "initial_delete_test_data": {
                "number_of_data": self.delete_number_of_data,
                "commit_unit": self.delete_commit_unit
            }
        }

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
            if log_level.upper() == logging.getLevelName(logging.CRITICAL):
                self.__log_level = logging.CRITICAL
            elif log_level.upper() == logging.getLevelName(logging.ERROR):
                self.__log_level = logging.ERROR
            elif log_level.upper() == logging.getLevelName(logging.WARNING):
                self.__log_level = logging.WARNING
            elif log_level.upper() == logging.getLevelName(logging.INFO):
                self.__log_level = logging.INFO
            elif log_level.upper() == logging.getLevelName(logging.DEBUG):
                self.__log_level = logging.DEBUG
            else:
                raise ValueError("Configuration value 'log_level' not a valid : {}".format(log_level))
        else:
            raise ValueError("Configuration value 'log_level' type not a valid string: {}".format(log_level))

    @property
    def sql_logging(self):
        return self.__sql_logging

    # sql_log_level 유효성 검사
    @sql_logging.setter
    def sql_logging(self, check):
        upper_check = check.upper()

        if upper_check == "YES" or upper_check == "Y":
            self.__sql_logging = "YES"
            self.sql_log_level = logging.INFO
        elif upper_check == "NO" or upper_check == "N":
            self.__sql_logging = "NO"
            self.sql_log_level = logging.WARNING
        else:
            raise ValueError("Configuration value 'sql_log_level' type not a valid string: {}".format(check))

    @property
    def nls_lang(self):
        return self.__nls_lang

    @nls_lang.setter
    def nls_lang(self, nls_lang):
        self.__nls_lang = nls_lang
        os.putenv("NLS_LANG", nls_lang)

    # @property
    # def lob_save(self):
    #     return self.__lob_save
    #
    # @lob_save.setter
    # def lob_save(self, check):
    #     upper_check = check.upper()
    #     if upper_check == "YES" or upper_check == "Y":
    #         self.__lob_save = "YES"
    #     elif upper_check == "NO" or upper_check == "N":
    #         self.__lob_save = "NO"
    #     else:
    #         raise ValueError("Configuration value 'lob_save' not a valid : {}".format(check))

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
        if 1024 <= int(port) <= 65535:
            self.__source_port = port
        else:
            raise ValueError("Configuration value 'source_port' not a valid : {}".format(port))

    @property
    def source_dbms_type(self):
        return self.__source_dbms_type

    @source_dbms_type.setter
    def source_dbms_type(self, dbms_type):
        upper_dbms_type = dbms_type.upper()

        if upper_dbms_type in dialect_driver:
            if upper_dbms_type == ORACLE:
                self.__source_dbms_type = dialect_driver[ORACLE]
            elif upper_dbms_type == MYSQL:
                self.__source_dbms_type = dialect_driver[MYSQL]
            elif upper_dbms_type == SQLSERVER:
                self.__source_dbms_type = dialect_driver[SQLSERVER]
            elif upper_dbms_type == POSTGRESQL:
                self.__source_dbms_type = dialect_driver[POSTGRESQL]
        else:
            raise ValueError("Configuration value 'source_dbms_type' not a valid : {}".format(dbms_type))

    @property
    def source_db_name(self):
        return self.__source_db_name

    @source_db_name.setter
    def source_db_name(self, db_name):
        self.__source_db_name = db_name

    @property
    def source_schema_name(self):
        return self.__source_schema_name

    @source_schema_name.setter
    def source_schema_name(self, schema_name):
        self.__source_schema_name = schema_name

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
        if 1024 <= int(port) <= 65535:
            self.__target_port = port
        else:
            raise ValueError("Configuration value 'target_port' not a valid : {}".format(port))

    @property
    def target_dbms_type(self):
        return self.__target_dbms_type

    @target_dbms_type.setter
    def target_dbms_type(self, dbms_type):
        upper_dbms_type = dbms_type.upper()

        if upper_dbms_type in dialect_driver:
            if upper_dbms_type == ORACLE:
                self.__target_dbms_type = dialect_driver[ORACLE]
            elif upper_dbms_type == MYSQL:
                self.__target_dbms_type = dialect_driver[MYSQL]
            elif upper_dbms_type == SQLSERVER:
                self.__target_dbms_type = dialect_driver[SQLSERVER]
            elif upper_dbms_type == POSTGRESQL:
                self.__target_dbms_type = dialect_driver[POSTGRESQL]
        else:
            raise ValueError("Configuration value 'target_dbms_type' not a valid : {}".format(dbms_type))

    @property
    def target_db_name(self):
        return self.__target_db_name

    @target_db_name.setter
    def target_db_name(self, db_name):
        self.__target_db_name = db_name

    @property
    def target_schema_name(self):
        return self.__target_schema_name

    @target_schema_name.setter
    def target_schema_name(self, schema_name):
        self.__target_schema_name = schema_name

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
    def update_number_of_data(self):
        return self.__update_number_of_data

    @update_number_of_data.setter
    def update_number_of_data(self, update_number_of_data):
        if int(update_number_of_data) >= 1:
            self.__update_number_of_data = int(update_number_of_data)
        else:
            raise ValueError(
                "Configuration value 'update_number_of_data' is not a numeric value: {}".format(update_number_of_data))

    @property
    def update_commit_unit(self):
        return self.__update_commit_unit

    @update_commit_unit.setter
    def update_commit_unit(self, update_commit_unit):
        if int(update_commit_unit) >= 1:
            self.__update_commit_unit = int(update_commit_unit)
        else:
            raise ValueError("Configuration value 'update_commit_unit' is not a numeric value: {}".format(update_commit_unit))

    @property
    def delete_number_of_data(self):
        return self.__delete_number_of_data

    @delete_number_of_data.setter
    def delete_number_of_data(self, delete_number_of_data):
        if int(delete_number_of_data):
            self.__delete_number_of_data = int(delete_number_of_data)
        else:
            raise ValueError(
                "Configuration value 'delete_number_of_data' is not a numeric value: {}".format(delete_number_of_data))

    @property
    def delete_commit_unit(self):
        return self.__delete_commit_unit

    @delete_commit_unit.setter
    def delete_commit_unit(self, delete_commit_unit):
        if int(delete_commit_unit) >= 1:
            self.__delete_commit_unit = int(delete_commit_unit)
        else:
            raise ValueError("Configuration value 'delete_commit_unit' is not a numeric value: {}".format(delete_commit_unit))

    def get_src_conn_string(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        if self.source_dbms_type == dialect_driver[ORACLE]:
            dsn = cx_Oracle.makedsn(self.source_host_name, self.source_port, service_name=self.source_db_name)
            return self.source_dbms_type + "://" + self.source_user_id + ":" + self.source_user_password + "@" + dsn
        elif self.source_dbms_type == dialect_driver[MYSQL]:
            return self.source_dbms_type + "://" + self.source_user_id + ":" + self.source_user_password + "@" + \
                   self.source_host_name + ":" + self.source_port + "/" + self.source_db_name + "?charset=utf8"
        else:
            return self.source_dbms_type + "://" + self.source_user_id + ":" + self.source_user_password + "@" + \
                   self.source_host_name + ":" + self.source_port + "/" + self.source_db_name

    def get_trg_conn_string(self):
        """
        config에서 connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

        :return: SQLAlchemy에서 사용되는 DB Connection String을 return
        """
        if self.target_dbms_type == dialect_driver[ORACLE]:
            dsn = cx_Oracle.makedsn(self.target_host_name, self.target_port, service_name=self.target_db_name)
            return self.target_dbms_type + "://" + self.target_user_id + ":" + self.target_user_password + "@" + dsn
        elif self.target_dbms_type == dialect_driver[MYSQL]:
            return self.target_dbms_type + "://" + self.target_user_id + ":" + self.target_user_password + "@" + \
                   self.target_host_name + ":" + self.target_port + "/" + self.target_db_name + "?charset=utf8"
        else:
            return self.target_dbms_type + "://" + self.target_user_id + ":" + self.target_user_password + "@" + \
                   self.target_host_name + ":" + self.target_port + "/" + self.target_db_name

    @staticmethod
    def get_dbms_alias(dbms_type):
        """
        intializer 실행시 출력하는 Config에서 dbms_type 부분의 값을 connection string 값이 아닌 보기 좋은 형태로 변환

        :return: DBMS Alias에 해당하는 값
        """

        if dbms_type == dialect_driver[ORACLE]:
            return "Oracle"
        elif dbms_type == dialect_driver[MYSQL]:
            return "MySQL"
        elif dbms_type == dialect_driver[SQLSERVER]:
            return "SQL Server"
        elif dbms_type == dialect_driver[POSTGRESQL]:
            return "PostgreSQL"

    def get_init_data_info(self):
        return {
                "update_number_of_data": self.update_number_of_data,
                "update_commit_unit": self.update_commit_unit,
                "delete_number_of_data": self.delete_number_of_data,
                "delete_commit_unit": self.delete_commit_unit
                }

    def get_config_dict(self):
        return {
            "setting": {
                "log_level": logging.getLevelName(self.log_level),
                "sql_logging": self.sql_logging,
                "nls_lang": self.nls_lang
                # , "lob_save": self.lob_save
            },
            "source_database": {
                "host_name": self.source_host_name,
                "port": self.source_port,
                "dbms_type": self.get_dbms_alias(self.source_dbms_type),
                "db_name": self.source_db_name,
                "schema_name": self.source_schema_name,
                "user_id": self.source_user_id,
                "user_password": self.source_user_password
            },
            "target_database": {
                "host_name": self.target_host_name,
                "port": self.target_port,
                "dbms_type": self.get_dbms_alias(self.target_dbms_type),
                "db_name": self.target_db_name,
                "schema_name": self.target_schema_name,
                "user_id": self.target_user_id,
                "user_password": self.target_user_password
            },
            "initial_update_test_data": {
                "number_of_data": self.update_number_of_data,
                "commit_unit": self.update_commit_unit
            },
            "initial_delete_test_data": {
                "number_of_data": self.delete_number_of_data,
                "commit_unit": self.delete_commit_unit
            }
        }

    def view_setting_config(self):

        dict_conf = self.get_config_dict()

        setting_conf = dict_conf.get("setting")
        setting_tab = texttable.Texttable()
        setting_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
        setting_tab.set_cols_width([20, 35])
        setting_tab.set_cols_align(["r", "l"])
        setting_tab.header(["[Setting Info.]", ""])

        for x, y in zip(setting_conf.keys(), setting_conf.values()):
            setting_tab.add_row([x, y])

        return setting_tab.draw()

    def view_source_connection_config(self):

        dict_conf = self.get_config_dict()

        src_db_conf = dict_conf.get("source_database")
        db_tab = texttable.Texttable()
        db_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
        db_tab.set_cols_width([20, 35])
        db_tab.set_cols_align(["r", "l"])
        db_tab.header(["[Database Info.]", "Source"])

        for x, y in zip(src_db_conf.keys(), src_db_conf.values()):
            db_tab.add_row([x, y])

        return db_tab.draw()

    def view_target_connection_config(self):

        dict_conf = self.get_config_dict()

        trg_db_conf = dict_conf.get("target_database")
        db_tab = texttable.Texttable()
        db_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
        db_tab.set_cols_width([20, 35])
        db_tab.set_cols_align(["r", "l"])
        db_tab.header(["[Database Info.]", "Target"])

        for x, y in zip(trg_db_conf.keys(), trg_db_conf.values()):
            db_tab.add_row([x, y])

        return db_tab.draw()

    def view_both_connection_config(self):

        dict_conf = self.get_config_dict()

        src_db_conf = dict_conf.get("source_database")
        trg_db_conf = dict_conf.get("target_database")
        db_tab = texttable.Texttable()
        db_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
        db_tab.set_cols_width([20, 16, 16])
        db_tab.set_cols_align(["r", "l", "l"])
        db_tab.header(["[Database Info.]", "Source", "Target"])

        for x, y, z in zip(src_db_conf.keys(), src_db_conf.values(), trg_db_conf.values()):
            db_tab.add_row([x, y, z])

        return db_tab.draw()

    def view_init_data_config(self):

        dict_conf = self.get_config_dict()

        init_update_conf = dict_conf.get("initial_update_test_data")
        init_delete_conf = dict_conf.get("initial_delete_test_data")
        init_tab = texttable.Texttable()
        init_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
        init_tab.set_cols_width([20, 16, 16])
        init_tab.set_cols_align(["r", "l", "l"])
        init_tab.header(["[Initial Info.]", "UPDATE_TEST", "DELETE_TEST"])

        for x, y, z in zip(init_update_conf.keys(), init_update_conf.values(), init_delete_conf.values()):
            init_tab.add_row([x, y, z])
            # init_tab.add_row(["  {}".format(x), y, z])

        return init_tab.draw()

    def view_config(self):

        print("  [File: {}]\n".format(self.config_name))

        print(self.view_setting_config())
        print()

        print(self.view_both_connection_config())
        print()

        print(self.view_init_data_config())
        print()

    @staticmethod
    def get_config():
        global CONFIG
        return CONFIG
