from src.constants import *
from src.funcs_common import print_error_msg

import configparser
import logging
import os


class ConfigManager():

    CONFIG = None

    def __init__(self, config_name):

        # 현재 경로가 ~ ~/cdcbench/conf Directory로 Working Directory 변경
        os.chdir(os.path.join(os.getcwd(), "conf"))

        if config_name is None:
            config_name = "default.ini"

        if os.path.splitext(config_name)[1] == "":
            config_name = config_name + ".ini"

        self.config_name = config_name

        self.config = configparser.ConfigParser()
        self.config.clear()
        self.config.read(self.config_name, encoding="utf-8")

        try:
            self.log_level = self.config.get("setting", "log_level")
            self.sql_log_level = logging.WARNING
            self.sql_logging = self.config.get("setting", "sql_logging")
            self.nls_lang = self.config.get("setting", "nls_lang")

            self.source_host_name = self.config.get("source_database", "host_name")
            self.source_port = self.config.get("source_database", "port")
            self.source_dbms_type = self.config.get("source_database", "dbms_type")
            self.source_db_name = self.config.get("source_database", "db_name")
            self.source_schema_name = self.config.get("source_database", "schema_name")
            self.source_user_id = self.config.get("source_database", "user_id")
            self.source_user_password = self.config.get("source_database", "user_password")

            self.target_host_name = self.config.get("target_database", "host_name")
            self.target_port = self.config.get("target_database", "port")
            self.target_dbms_type = self.config.get("target_database", "dbms_type")
            self.target_db_name = self.config.get("target_database", "db_name")
            self.target_schema_name = self.config.get("target_database", "schema_name")
            self.target_user_id = self.config.get("target_database", "user_id")
            self.target_user_password = self.config.get("target_database", "user_password")

            self.update_number_of_data = self.config.get("initial_update_test_data", "number_of_data")
            self.update_commit_unit = self.config.get("initial_update_test_data", "commit_unit")
            self.delete_number_of_data = self.config.get("initial_delete_test_data", "number_of_data")
            self.delete_commit_unit = self.config.get("initial_delete_test_data", "commit_unit")

        except configparser.NoOptionError as opterr:
            print_error_msg("Configuration parameter does not existed: [{}] {}".format(opterr.section, opterr.option))

        except configparser.NoSectionError as secerr:
            print_error_msg("Configuration section does not existed: [{}]".format(secerr.section))

        ConfigManager.CONFIG = self

        # curdir이 ~/cdcbench/conf일 경우 ~/cdcbench로 Working Directory 변경
        if os.path.basename(os.getcwd()) == "conf":
            os.chdir(os.path.pardir)

    def __repr__(self):
        return {
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

    @property
    def config_name(self):
        return self.__config_name

    # config 존재 유무 검사
    @config_name.setter
    def config_name(self, config_name):
        if os.path.isfile(os.path.join(os.getcwd(), config_name)):
            self.__config_name = config_name
        else:
            print_error_msg("Configuration file ({}) does not exist.".format(config_name))

    @property
    def log_level(self):
        return self.__log_level

    # log_level 유효성 검사
    @log_level.setter
    def log_level(self, log_level):
        # CRITICAL < ERROR < WARNING < INFO < DEBUG
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
            print_error_msg("Configuration value 'log_level' not a valid : {}".format(log_level))

    @property
    def sql_logging(self):
        return self.__sql_logging

    # sql_log_level 유효성 검사
    @sql_logging.setter
    def sql_logging(self, sql_logging):
        upper_check = sql_logging.upper()

        if upper_check == "ALL":
            self.__sql_logging = "ALL"
            self.sql_log_level = logging.DEBUG
        elif upper_check == "SQL":
            self.__sql_logging = "SQL"
            self.sql_log_level = logging.INFO
        elif upper_check == "NONE":
            self.__sql_logging = "NONE"
            self.sql_log_level = logging.WARNING
        else:
            print_error_msg("Configuration value 'sql_log_level' type not a valid : {}".format(sql_logging))

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
        if 1024 <= int(port) <= 65535:
            self.__source_port = port
        else:
            print_error_msg("Configuration value 'source_port' not a valid : {}".format(port))

    @property
    def source_dbms_type(self):
        return self.__source_dbms_type

    @source_dbms_type.setter
    def source_dbms_type(self, dbms_type):
        upper_dbms_type = dbms_type.upper()

        if upper_dbms_type in [ORACLE, MYSQL, SQLSERVER, POSTGRESQL]:
            self.__source_dbms_type = upper_dbms_type
        else:
            print_error_msg("Configuration value 'source_dbms_type' not a valid : {}".format(dbms_type))

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
            print_error_msg("Configuration value 'target_port' not a valid : {}".format(port))

    @property
    def target_dbms_type(self):
        return self.__target_dbms_type

    @target_dbms_type.setter
    def target_dbms_type(self, dbms_type):
        upper_dbms_type = dbms_type.upper()

        if upper_dbms_type in [ORACLE, MYSQL, SQLSERVER, POSTGRESQL]:
            self.__target_dbms_type = upper_dbms_type
        else:
            print_error_msg("Configuration value 'target_dbms_type' not a valid : {}".format(dbms_type))

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
            print_error_msg("Configuration value 'update_number_of_data' is not a numeric value: {}".format(update_number_of_data))

    @property
    def update_commit_unit(self):
        return self.__update_commit_unit

    @update_commit_unit.setter
    def update_commit_unit(self, update_commit_unit):
        if int(update_commit_unit) >= 1:
            self.__update_commit_unit = int(update_commit_unit)
        else:
            print_error_msg("Configuration value 'update_commit_unit' is not a numeric value: {}".format(update_commit_unit))

    @property
    def delete_number_of_data(self):
        return self.__delete_number_of_data

    @delete_number_of_data.setter
    def delete_number_of_data(self, delete_number_of_data):
        if int(delete_number_of_data) >= 1:
            self.__delete_number_of_data = int(delete_number_of_data)
        else:
            print_error_msg("Configuration value 'delete_number_of_data' is not a numeric value: {}".format(delete_number_of_data))

    @property
    def delete_commit_unit(self):
        return self.__delete_commit_unit

    @delete_commit_unit.setter
    def delete_commit_unit(self, delete_commit_unit):
        if int(delete_commit_unit) >= 1:
            self.__delete_commit_unit = int(delete_commit_unit)
        else:
            print_error_msg("Configuration value 'delete_commit_unit' is not a numeric value: {}".format(delete_commit_unit))

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

    def get_config_dict(self):
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
