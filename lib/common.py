
from __future__ import annotations

import argparse
import logging
import textwrap

from pydantic import PydanticValueError
from sqlalchemy.sql import select, func
from texttable import Texttable

from lib.globals import *

# Import for type hinting
from pydantic.fields import FieldInfo
from typing import Any, List, Optional, TYPE_CHECKING, NoReturn, Dict
if TYPE_CHECKING:
    from lib.config import SettingsConfig, DatabaseConfig, InitialDataConfig, ConfigModel


class CustomHelpFormatter(argparse.RawTextHelpFormatter):
    """
    --help 명령 formatting Class
    """
    def __init__(self, prog, indent_increment=2, max_help_position=24, width=100):
        super().__init__(prog, indent_increment=indent_increment, max_help_position=max_help_position, width=width)

    def _format_action_invocation(self, action):
        if not action.option_strings or action.nargs == 0:
            return super()._format_action_invocation(action)
        default = self._get_default_metavar_for_optional(action)
        args_string = self._format_args(action, default)
        return ', '.join(action.option_strings) + ' ' + args_string


def get_version() -> str:
    """
    ## Changes

    """
    return "CDCBENCH Version 1.5.0-alpha"


def get_elapsed_time_msg(end_time, start_time):
    """
    작업 소요시간을 CDCBENCH에서 보여주는 format으로 생성
    :param end_time:
    :param start_time:
    :return: 정해진 포맷의 작업 소요시간
    """

    s_time = float(start_time)
    e_time = float(end_time)
    elapse_time = e_time - s_time

    return f"Elapsed Time: {elapse_time:.2f} Sec."


def get_exist_option(args: argparse.Namespace, keys: List) -> Optional[str]:
    for key in keys:
        if hasattr(args, key):
            return key
    return None


def print_error(msg: str) -> None:
    """
    작업 수행 중 예외처리에 의해 종료될 경우 매개변수를 정해진 포맷으로 출력하고 프로그램을 종료
    :param msg: 에러 메시지
    """
    print()
    print("Program was terminated for the following reasons:")
    print(textwrap.indent(msg, "  "))
    exit(1)


def get_object_name(match_object_name, object_name_list):
    """
    :param match_object_name: 찾고자 하는 object name 
    :param object_name_list: object name을 검색할 리스트
    :return: 
    """
    for object_name in object_name_list:
        if object_name.upper() == match_object_name.upper():
            return object_name
    raise KeyError(match_object_name)


def get_start_time_msg(time):
    return f"\n  ::: {time:%Y-%m-%d %H:%M:%S} ::: "


def print_end_msg(msg: str, print_flag: bool = True, end: str = "", separate: bool = True) -> NoReturn:
    if print_flag:
        print(f"{msg}{end}")
    else:
        if separate:
            print()
        else:
            pass


def print_description_msg(dml, table_name, end_flag):

    if end_flag:
        print(f"  {dml.title()}ing data in the \"{table_name}\" Table ", end="", flush=True)
    else:
        print(f"  {dml.title()}ing data in the \"{table_name}\" Table ", flush=True)


def isint(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def proc_database_error(logger: logging.Logger, error: Any, print_fail: bool = True) -> NoReturn:

    if print_fail:
        print_end_msg(FAIL, True)

    from lib.logger import LoggerManager
    sql_logger = LoggerManager.get_sql_logger("sql")
    sql_logger.info(ROLLBACK.upper())

    logger.error(error.args[0])
    logger.error(f"SQL: {error.statement}")
    logger.error(f"params: {error.params}")

    log_level = LoggerManager.log_level
    if log_level == logging.DEBUG:
        logger.exception(error.args[0])

    print_error(error.args[0])


def get_separate_col_val(engine, table, column):
    """
    특정 테이블 (table)의 최대 SEPARATE_COL값을 조회
    :param engine: engine Object
    :param table: Table Object
    :param column: Column (SEPARATE_COL) Object
    :return: select 결과가 null일 경우 1, null이 아닌 경우 결과값 + 1
    """
    sql = select([func.max(table.columns[column]).label("MAX_SEPARATE_COL")])
    result = engine.execute(sql).scalar()
    if result is None:
        return 1
    else:
        return result + 1


def sa_unsupported_dbms_module_limit(dbms_type):

    if dbms_type in sa_unsupported_dbms:
        print_error(f"This module is not available in the following DBMS {sa_unsupported_dbms}")


def connection_string_value_check(conn_info: DatabaseConfig):
    if (not conn_info.host or not conn_info.port or not conn_info.dbms or not conn_info.dbname or
            not conn_info.username or not conn_info.password):
        print_error("Not enough values are available to create the connection string. \n"
                    "  * Note. Please check the configuration file.")


# +----- Functions related to config view -----+

_VIEW_KEY_COL_WIDTH = 18
_VIEW_VALUE_COL_WIDTH = 31


def _view_config_file_name(config_file_name: str) -> str:
    return f"[ File: {config_file_name} ]"


def _view_settings(settings_config: SettingsConfig) -> str:

    setting_tab_result = "[ SETTINGS ] \n"

    setting_tab = Texttable()
    setting_tab.set_deco(Texttable.HEADER | Texttable.VLINES | Texttable.BORDER)
    setting_tab.set_cols_width([_VIEW_KEY_COL_WIDTH, _VIEW_VALUE_COL_WIDTH])
    setting_tab.set_cols_align(["c", "l"])

    for k in settings_config.dict().keys():
        setting_tab.add_row([k, getattr(settings_config, k)])

    return setting_tab_result + setting_tab.draw()


def _view_databases(databases: Dict[str, DatabaseConfig]) -> str:

    db_tab_result = "[ DATABASES ] \n"

    db_tab = Texttable()
    db_tab.set_deco(Texttable.HEADER | Texttable.VLINES | Texttable.HLINES)

    db_tab.header(["DB key", "DBMS", "HOST", "PORT", "DBNAME", "USERNAME", "PASSWORD", "SCHEMA"])
    db_tab.set_cols_width([8, 10, 15, 5, 15, 10, 15, 10])

    for db_key in databases:
        database = databases[db_key].dict()
        db_tab.add_row([db_key] + [database[param] for param in database])

    return db_tab_result + db_tab.draw()


def _view_databases_run_initbench(databases: Dict[str, DatabaseConfig]) -> str:

    db_tab = Texttable()
    db_tab.set_deco(Texttable.HEADER | Texttable.VLINES)
    db_tab.header(["[ Database ]", "DBMS", "Connection"])

    for db_key in databases:
        db = databases[db_key]
        db_tab.add_row([db_key, db.dbms, f"{db.username}@{db.host}:{db.port}/{db.dbname}"])

    return db_tab.draw()


def _view_data_config(initial_data_conf: Dict[str, InitialDataConfig], view_flag=False):

    if view_flag is True:
        return ""

    init_tab = Texttable()
    init_tab.set_deco(Texttable.HEADER | Texttable.VLINES)
    init_tab.set_cols_width([20, 16, 16])
    init_tab.set_cols_align(["r", "l", "l"])
    init_tab.header(["[ Initial Data ]", "RECORD_COUNT", "COMMIT_COUNT"])

    for table_name in initial_data_conf:
        table_initial_data = initial_data_conf[table_name].dict()
        init_tab.add_row([table_name] + [table_initial_data[param] for param in table_initial_data])

    return init_tab.draw()


def _view_initbench_option(args: argparse.Namespace):

    option_tab = Texttable()
    option_tab.set_deco(Texttable.HEADER | Texttable.VLINES)
    option_tab.set_cols_width([20, 35])
    option_tab.set_cols_align(["r", "l"])
    option_tab.header(["[ Execution ]", ""])

    option_dict = {}

    if hasattr(args, "create") or hasattr(args, "reset"):
        option_dict["Command"] = get_exist_option(args, ["create", "reset"]).title()
        if hasattr(args, "without_data"):
            option_dict["Data"] = "Object"
        else:
            option_dict["Data"] = "Object & Data"

        option_dict["Table Key"] = (get_exist_option(args, ["primary_key", "unique", "non_key"])
                                    .replace("_", " ").title())

    else:
        option_dict["Command"] = "Drop"

    for x, y in zip(option_dict.keys(), option_dict.values()):
        option_tab.add_row([x, y])

    return option_tab.draw()


def view_config_file(config: ConfigModel) -> str:
    return (f"\n"
            f"{_view_config_file_name(config.config_file_name)} \n\n"
            f"{_view_settings(config.settings)} \n\n"
            f"{_view_databases(config.databases)} \n\n"
            f"{_view_data_config(config.initial_data)}\n\n")


def view_runtime_config(config: ConfigModel, args: argparse.Namespace):
    return (f"\n"
            f"{_view_config_file_name(config.config_file_name)} \n\n"
            f"{_view_initbench_option(args)} \n\n"
            f"{_view_data_config(config.initial_data)} \n\n"
            f"{_view_databases_run_initbench(config.databases)} \n\n")


# +----- Classes and Functions related to pydantic -----+
class InvalidValueError(PydanticValueError):
    code = "invalid_value"
    msg_template = "value is not valid"


def join_allow_values(values: List[str]):
    return " | ".join(values)


def none_set_default_value(v: Any, field: FieldInfo):
    if field.default is not None and v is None:
        return field.default
    else:
        return v
