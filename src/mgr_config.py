from src.constants import *
from src.funcs_common import print_error_msg

import configparser
import logging
import os


DEFAULT_NLS_LANG = "AMERICAN_AMERICA.AL32UTF8"


def get_value_invalid_msg(config_name, value):
    return f"Configuration '{config_name}' value is invalid : {value}"


class ConfigManager:

    # CONFIG = None

    def __init__(self, config_name):

        # 현재 경로가 ~ ~/cdcbench/conf Directory로 Working Directory 변경
        os.chdir(os.path.join(os.getcwd(), "conf"))

        if config_name is None:
            config_name = default_config_name

        if os.path.splitext(config_name)[1] == "":
            config_name = "".join([config_name, ".conf"])

        self.config_name = config_name

        self.config = configparser.ConfigParser()
        self.config.clear()
        self.config.read(self.config_name, encoding="utf-8")

        try:
            self.log_level = self.config.get("SETTING", "LOG_LEVEL")
            self.sql_log_level = logging.WARNING
            self.sql_logging = self.config.get("SETTING", "SQL_LOGGING")
            self.nls_lang = self.config.get("SETTING", "NLS_LANG")

            self.source_host_name = self.config.get("SOURCE_DATABASE", "HOST_NAME")
            self.source_port = self.config.get("SOURCE_DATABASE", "PORT")
            self.source_dbms_type = self.config.get("SOURCE_DATABASE", "DBMS_TYPE")
            self.source_db_name = self.config.get("SOURCE_DATABASE", "DB_NAME")
            self.source_schema_name = self.config.get("SOURCE_DATABASE", "SCHEMA_NAME")
            self.source_user_id = self.config.get("SOURCE_DATABASE", "USER_ID")
            self.source_user_password = self.config.get("SOURCE_DATABASE", "USER_PASSWORD")

            self.target_host_name = self.config.get("TARGET_DATABASE", "HOST_NAME")
            self.target_port = self.config.get("TARGET_DATABASE", "PORT")
            self.target_dbms_type = self.config.get("TARGET_DATABASE", "DBMS_TYPE")
            self.target_db_name = self.config.get("TARGET_DATABASE", "DB_NAME")
            self.target_schema_name = self.config.get("TARGET_DATABASE", "SCHEMA_NAME")
            self.target_user_id = self.config.get("TARGET_DATABASE", "USER_ID")
            self.target_user_password = self.config.get("TARGET_DATABASE", "USER_PASSWORD")

            self.update_number_of_data = self.config.get("INITIAL_UPDATE_TEST_DATA", "NUMBER_OF_DATA")
            self.update_commit_unit = self.config.get("INITIAL_UPDATE_TEST_DATA", "COMMIT_UNIT")
            self.delete_number_of_data = self.config.get("INITIAL_DELETE_TEST_DATA", "NUMBER_OF_DATA")
            self.delete_commit_unit = self.config.get("INITIAL_DELETE_TEST_DATA", "COMMIT_UNIT")

        except configparser.NoOptionError as opterr:
            print_error_msg(f"Configuration parameter does not existed: [{opterr.section}] {opterr.option}")

        except configparser.NoSectionError as secerr:
            print_error_msg(f"Configuration section does not existed: [{secerr.section}]")

        # ConfigManager.CONFIG = self

        # curdir이 ~/cdcbench/conf일 경우 ~/cdcbench로 Working Directory 변경
        if os.path.basename(os.getcwd()) == "conf":
            os.chdir(os.path.pardir)

    @property
    def config_name(self):
        return self.__config_name

    # config 존재 유무 검사
    @config_name.setter
    def config_name(self, config_name):
        if os.path.isfile(os.path.join(os.getcwd(), config_name)):
            self.__config_name = config_name
        else:
            print_error_msg(f"Configuration file ( {config_name} ) does not exist.")

    @property
    def log_level(self):
        return self.__log_level

    # log_level 유효성 검사
    @log_level.setter
    def log_level(self, log_level):
        if log_level != "":
            log_level_upper = log_level.upper()
            # CRITICAL < ERROR < WARNING < INFO < DEBUG
            if log_level_upper == logging.getLevelName(logging.CRITICAL):
                self.__log_level = logging.CRITICAL
            elif log_level_upper == logging.getLevelName(logging.ERROR):
                self.__log_level = logging.ERROR
            elif log_level_upper == logging.getLevelName(logging.WARNING):
                self.__log_level = logging.WARNING
            elif log_level_upper == logging.getLevelName(logging.INFO):
                self.__log_level = logging.INFO
            elif log_level_upper == logging.getLevelName(logging.DEBUG):
                self.__log_level = logging.DEBUG
            else:
                print_error_msg(get_value_invalid_msg("log_level", log_level))
        else:
            self.__log_level = logging.ERROR

    @property
    def sql_logging(self):
        return self.__sql_logging

    # sql_log_level 유효성 검사
    @sql_logging.setter
    def sql_logging(self, sql_logging):
        if sql_logging != "":
            sql_logging_upper = sql_logging.upper()

            if sql_logging_upper == "ALL":
                self.__sql_logging = "ALL"
                self.sql_log_level = logging.DEBUG
            elif sql_logging_upper == "SQL":
                self.__sql_logging = "SQL"
                self.sql_log_level = logging.INFO
            elif sql_logging_upper == "NONE":
                self.__sql_logging = "NONE"
                self.sql_log_level = logging.WARNING
            else:
                print_error_msg(get_value_invalid_msg("sql_logging", sql_logging))
        else:
            self.__sql_logging = "NONE"
            self.sql_log_level = logging.WARNING

    @property
    def nls_lang(self):
        return self.__nls_lang

    @nls_lang.setter
    def nls_lang(self, nls_lang):
        if nls_lang != "":
            self.__nls_lang = nls_lang
        else:
            self.__nls_lang = DEFAULT_NLS_LANG
        os.putenv("NLS_LANG", self.nls_lang)

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
        self.__source_port = _port_check(SOURCE, port)

    @property
    def source_dbms_type(self):
        return self.__source_dbms_type

    @source_dbms_type.setter
    def source_dbms_type(self, dbms_type):
        self.__source_dbms_type = _dbms_type_check(SOURCE, dbms_type)

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
        self.__target_port = _port_check(TARGET, port)

    @property
    def target_dbms_type(self):
        return self.__target_dbms_type

    @target_dbms_type.setter
    def target_dbms_type(self, dbms_type):
        self.__target_dbms_type = _dbms_type_check(TARGET, dbms_type)

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
        self.__update_number_of_data = _data_check("INITIAL_UPDATE_TEST_DATA", "NUMBER_OF_DATA", update_number_of_data)

    @property
    def update_commit_unit(self):
        return self.__update_commit_unit

    @update_commit_unit.setter
    def update_commit_unit(self, update_commit_unit):
        self.__update_commit_unit = _data_check("INITIAL_UPDATE_TEST_DATA", "COMMIT_UNIT", update_commit_unit)

    @property
    def delete_number_of_data(self):
        return self.__delete_number_of_data

    @delete_number_of_data.setter
    def delete_number_of_data(self, delete_number_of_data):
        self.__delete_number_of_data = _data_check("INITIAL_DELETE_TEST_DATA", "NUMBER_OF_DATA", delete_number_of_data)

    @property
    def delete_commit_unit(self):
        return self.__delete_commit_unit

    @delete_commit_unit.setter
    def delete_commit_unit(self, delete_commit_unit):
        self.__delete_commit_unit = _data_check("INITIAL_DELETE_TEST_DATA", "COMMIT_UNIT", delete_commit_unit)

    def get_src_conn_info(self):
        return {
            "host_name": self.source_host_name,
            "port": self.source_port,
            "dbms_type": self.source_dbms_type,
            "db_name": self.source_db_name,
            "schema_name": self.source_schema_name,
            "user_id": self.source_user_id,
            "user_password": self.source_user_password
        }

    def get_trg_conn_info(self):
        return {
            "host_name": self.target_host_name,
            "port": self.target_port,
            "dbms_type": self.target_dbms_type,
            "db_name": self.target_db_name,
            "schema_name": self.target_schema_name,
            "user_id": self.target_user_id,
            "user_password": self.target_user_password
        }

    def get_init_data_info(self):
        return {
            "update_number_of_data": self.update_number_of_data,
            "update_commit_unit": self.update_commit_unit,
            "delete_number_of_data": self.delete_number_of_data,
            "delete_commit_unit": self.delete_commit_unit
        }

    def get_config(self):
        return {
            "config_name": self.config_name,
            "setting": {
                "log_level": logging.getLevelName(self.log_level),
                "sql_logging": self.sql_logging,
                "nls_lang": self.nls_lang
            },
            "source_database": {
                "host_name": self.source_host_name,
                "port": self.source_port,
                "dbms_type": _get_dbms_alias(self.source_dbms_type),
                "db_name": self.source_db_name,
                "schema_name": self.source_schema_name,
                "user_id": self.source_user_id,
                "user_password": self.source_user_password
            },
            "target_database": {
                "host_name": self.target_host_name,
                "port": self.target_port,
                "dbms_type": _get_dbms_alias(self.target_dbms_type),
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


def _get_dbms_alias(dbms_type):
    """
    intializer 실행시 출력하는 Config에서 dbms_type 부분의 값을 connection string 값이 아닌 보기 좋은 형태로 변환

    :return: DBMS Alias에 해당하는 값
    """

    if dbms_type == ORACLE:
        return "Oracle"
    elif dbms_type == MYSQL:
        return "MySQL"
    elif dbms_type == SQLSERVER:
        return "SQL Server"
    elif dbms_type == POSTGRESQL:
        return "PostgreSQL"
    else:
        return ""


def _port_check(section, port):
    if port != "":
        if port.isdecimal() and 1024 <= int(port) <= 65535:
            return port
        else:
            print_error_msg(get_value_invalid_msg(f"[{section}] port", port))
    else:
        return ""


def _dbms_type_check(section, dbms_type):
    if dbms_type != "":
        if dbms_type.upper() in support_dbms_list:
            return dbms_type.upper()
        else:
            print_error_msg(get_value_invalid_msg(f"[{section}] dbms_type", dbms_type))
    else:
        return ""


def _data_check(section, config_name, number_of_data):
    if number_of_data != "":
        if number_of_data.isdecimal() and int(number_of_data) >= 1:
            return int(number_of_data)
        else:
            print_error_msg(get_value_invalid_msg(f"[{section}] {config_name}", number_of_data))
    else:
        return 30000 if config_name == "NUMBER_OF_DATA" else 2000
