
import os
import textwrap
import yaml

from pydantic import BaseModel, Field, ValidationError, validator, NumberNotGeError
from typing import Optional, Dict

from lib.common import print_error, InvalidValueError, join_allow_values, none_set_default_value
from lib.globals import *
from lib.logger import LoggerManager

_CONFIG_DIRECTORY: str = "conf"
_CONFIG_FILE_EXT: str = ".conf"
_DEFAULT_CONFIG_FILE_NAME: str = "default.conf"
_DEFAULT_NLS_LANG: str = "AMERICAN_AMERICA.AL32UTF8"


class SettingsConfig(BaseModel):
    log_level: str = Field("ERROR", alias="LOG_LEVEL")
    sql_logging: str = Field("NONE", alias="SQL_LOGGING")
    nls_lang: str = Field(_DEFAULT_NLS_LANG, alias="NLS_LANG")

    _none_set_default_value = validator("*", pre=True, allow_reuse=True)(none_set_default_value)

    @validator("log_level")
    def check_log_level(cls, log_level):
        allow_log_levels = ["ERROR", "INFO", "DEBUG"]
        log_level_upper = log_level.upper()
        if log_level_upper in allow_log_levels:
            LoggerManager.log_level = log_level_upper
            return log_level_upper
        else:
            raise InvalidValueError(actual_value=log_level, expected_value=join_allow_values(allow_log_levels))

    @validator("sql_logging")
    def check_sql_logging(cls, sql_logging):
        allow_sql_logging = ["NONE", "SQL", "ALL"]
        sql_logging_upper = sql_logging.upper()
        if sql_logging_upper in allow_sql_logging:
            LoggerManager.sql_logging = sql_logging_upper
            return sql_logging_upper
        else:
            raise InvalidValueError(actual_value=sql_logging, expected_value=join_allow_values(allow_sql_logging))

    @validator("nls_lang")
    def set_env_nls_lang(cls, nls_lang):
        os.environ["NLS_LANG"] = nls_lang
        return nls_lang


class DatabaseConfig(BaseModel):
    dbms: Optional[str] = Field(alias="DBMS")
    host: Optional[str] = Field(alias="HOST")
    port: Optional[int] = Field(alias="PORT")
    dbname: Optional[str] = Field(alias="DBNAME")
    username: Optional[str] = Field(alias="USERNAME")
    password: Optional[str] = Field(alias="PASSWORD")
    v_schema: Optional[str] = Field(None, alias="SCHEMA")

    @validator("dbms")
    def check_dbms(cls, dbms):
        if dbms:
            dbms_upper = dbms.upper()
            if dbms_upper in cb_support_dbms:
                if dbms_upper == MARIADB:
                    return MYSQL
                else:
                    return dbms_upper
            else:
                raise InvalidValueError(actual_value=dbms, expected_value=join_allow_values(cb_support_dbms))
        else:
            return None

    @validator("port")
    def check_port(cls, port):
        if port:
            if 1024 <= port <= 65535:
                return port
            else:
                raise InvalidValueError(actual_value=port, expected_value="1024 <= PORT <= 65535")
        else:
            return None


class InitialDataConfig(BaseModel):
    record_count: int = Field(30000, alias="RECORD_COUNT")
    commit_count: int = Field(2000, alias="COMMIT_COUNT")

    _none_set_default_value = validator("*", pre=True, allow_reuse=True)(none_set_default_value)

    def check_value_ge_one(cls, v):
        if v >= 0:
            return v
        else:
            raise NumberNotGeError(limit=1)
    _check_value_ge_one = validator("*", pre=True, allow_reuse=True)(check_value_ge_one)


class ConfigModel(BaseModel):
    config_file_name: str
    settings: SettingsConfig = Field(alias="SETTINGS")
    databases: Dict[str, DatabaseConfig] = Field(alias="DATABASES")
    initial_data: Dict[str, InitialDataConfig] = Field(alias="INITIAL_DATA")

    def upper_key_name(cls, v: Dict):
        for key in v.keys():
            v[key.upper()] = v.pop(key)
        return v
    _upper_key_name = validator("databases", "initial_data", allow_reuse=True)(upper_key_name)


class ConfigManager:

    def __init__(self, config_file_name: str):

        if config_file_name is None:
            config_file_name = _DEFAULT_CONFIG_FILE_NAME

        if os.path.splitext(config_file_name)[1] == "":
            config_file_name += _CONFIG_FILE_EXT

        self.config_file_name = config_file_name

    def get_config(self) -> ConfigModel:
        try:
            with open(os.path.join(_CONFIG_DIRECTORY, self.config_file_name), "r", encoding="utf-8") as f:
                loaded_config = yaml.safe_load(f)

        except FileNotFoundError:
            print_error(f"Configuration file [ {self.config_file_name} ] does not exist.")

        except yaml.YAMLError as YE:
            print_error(f"Invalid YAML format of configuration file [ {YE.args[1].name} ] \n"
                        f"  * line {YE.args[1].line + 1}, column {YE.args[1].column + 1}")

        loaded_config["config_file_name"] = self.config_file_name

        try:
            parsed_config = ConfigModel.parse_obj(loaded_config)
            return parsed_config
        except ValidationError as VE:
            err_msg = "The following parameters have invalid values."
            for error in VE.errors():
                err_msg += f"\n  [ {'.'.join(error['loc'])} ]: {error['msg']}\n"
                if error["type"] == "value_error.invalid_value":
                    err_ctx = error["ctx"]
                    err_msg += textwrap.indent(
                        f"* actual value: {err_ctx['actual_value']}\n"
                        f"* expected value: {err_ctx['expected_value']}",
                        "    ")
                else:
                    err_msg += textwrap.indent(
                        f"* error type: {error['type']}",
                        "    ")
            print_error(err_msg)
