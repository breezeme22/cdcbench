
from __future__ import annotations

import argparse
import datetime
import logging
import textwrap

from dataclasses import dataclass, field
from pydantic import PydanticValueError
from pydantic.fields import FieldInfo
from sqlalchemy.future import Engine
from texttable import Texttable
from types import SimpleNamespace
from typing import Any, List, Optional, TYPE_CHECKING, NoReturn, Dict, Type, Union

from lib.globals import *

if TYPE_CHECKING:
    from lib.config import SettingsConfig, DatabaseConfig, InitialDataConfig, ConfigModel
    from lib.connection import ConnectionManager
    from lib.definition import OracleDeclBase, MysqlDeclBase, SqlServerDeclBase, PostgresqlDeclBase


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
    return "CDCBENCH Version 2.0.0-alpha"


def get_elapsed_time_msg(end_time: float, start_time: float) -> str:
    elapsed_time = end_time - start_time
    return f"Elapsed time : {elapsed_time:.2f} Sec."


def get_exist_option(args: argparse.Namespace, keys: List) -> Optional[str]:
    for key in keys:
        if hasattr(args, key):
            return key
    return None


def print_error(msg: str, print_fail: bool = False) -> None:
    """
    작업 수행 중 예외처리에 의해 종료될 경우 매개변수를 정해진 포맷으로 출력하고 프로그램을 종료
    :param msg: 에러 메시지
    :param print_fail: 에러 메시지 출력 전 Fail 메시지 출력 여부 지정
    """

    if print_fail:
        print_end_msg(FAIL, True)

    print()
    print("Program was terminated for the following reasons:")
    print(textwrap.indent(msg, "  "))
    exit(1)


def get_object_name(match_object_name, object_name_list) -> str:
    """
    :param match_object_name: 찾고자 하는 object name 
    :param object_name_list: object name을 검색할 리스트
    :return: 
    """
    for object_name in object_name_list:
        if object_name.upper() == match_object_name.upper():
            return object_name
    raise KeyError(match_object_name)


def get_start_time_msg(dt: datetime.datetime) -> str:
    return f"\n  ::: {dt:%Y-%m-%d %H:%M:%S} ::: "


def print_end_msg(msg: str, print_flag: bool = True, end: str = "", separate: bool = True) -> NoReturn:
    if print_flag:
        print(f"{msg}{end}")
    else:
        if separate:
            print()
        else:
            pass


def isint(s) -> bool:
    try:
        int(s)
        return True
    except ValueError:
        return False


def proc_database_error(error: Any) -> NoReturn:

    from lib.logger import LogManager
    logger = logging.getLogger(CDCBENCH)
    sql_logger = logging.getLogger(SQL)
    sql_logger.info(ROLLBACK.upper())

    logger.error(error.args[0])
    logger.error(f"SQL: {error.statement}")
    logger.error(f"params: {error.params}")

    log_level = LogManager.log_level
    if log_level == logging.DEBUG:
        logger.exception(error.args[0])

    print_error(error.args[0], True)


def sa_unsupported_dbms_module_limit(dbms: str) -> NoReturn:

    if dbms in sa_unsupported_dbms:
        print_error(f"This module is not available in the following DBMS {sa_unsupported_dbms}")


def connection_string_value_check(conn_info: DatabaseConfig) -> NoReturn:
    if (not conn_info.host or not conn_info.port or not conn_info.dbms or not conn_info.dbname or
            not conn_info.username or not conn_info.password):
        print_error("Not enough values are available to create the connection string. \n"
                    "  * Note. Please check the configuration file.")


def check_positive_integer_arg(item: str) -> int:
    item = item.replace(",", "")
    if isint(item):
        int_item = int(item)
        if int_item <= 0:
            raise argparse.ArgumentTypeError(f"argument must be greater than or equal to 1. [ {item} ]")
        return int_item
    else:
        raise argparse.ArgumentTypeError(f"argument allows positive integer. [ {item} ]")


def convert_sample_table_alias(item: str) -> str:

    if item == "S":
        return STRING_TEST
    elif item == "N":
        return NUMERIC_TEST
    elif item == "D":
        return DATETIME_TEST
    elif item == "B":
        return BINARY_TEST
    elif item == "L":
        return LOB_TEST
    elif item == "O":
        return ORACLE_TEST
    elif item == "Q":
        return SQLSERVER_TEST
    else:
        return item


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


def _view_databases_run_initbench(args: argparse.Namespace, databases: Dict[str, DatabaseConfig]) -> str:

    db_tab = Texttable()
    db_tab.set_deco(Texttable.HEADER | Texttable.VLINES)
    db_tab.header(["[ Database ]", "DBMS", "Connection"])

    for db_key in args.database:
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

    if args.command.startswith("c") or args.command.startswith("r"):

        command = "Create" if args.command.startswith("c") else "Reset"
        option_dict["Command"] = command

        if args.data == "WITHOUT":
            option_dict["Data"] = "Object"
        elif args.data == "ONLY":
            option_dict["Data"] = "Data"
        else:
            option_dict["Data"] = "Object & Data"

        option_dict["Table Key"] = args.key.replace("-", " ").title()

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
            f"{_view_databases_run_initbench(args, config.databases)} \n\n")
# +----- Functions related to config view -----+


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
# +----- Classes and Functions related to pydantic -----+


class DatabaseWorkInfo(SimpleNamespace):
    conn_info: DatabaseConfig
    engine: Engine
    decl_base: Type[Union[OracleDeclBase, MysqlDeclBase, SqlServerDeclBase, PostgresqlDeclBase]]
    description: str


# +----- Classes and functions related to DML statistics -----+
@dataclass
class DMLDetail:
    insert: int = 0
    update: int = 0
    delete: int = 0

    def as_dict(self):
        return {"insert": self.insert, "update": self.update, "delete": self.delete}


@dataclass
class DMLSummary:
    dml_record: int = 0
    dml_count: int = 0
    detail: Dict[str, DMLDetail] = field(default_factory=dict)


@dataclass
class TCLSummary:
    commit: int = 0
    rollback: int = 0


@dataclass
class ExecutionInfo:
    start_time: float = 0
    end_time: float = 0


@dataclass
class ResultSummary:
    execution_info: ExecutionInfo = field(default_factory=ExecutionInfo)
    dml: DMLSummary = field(default_factory=DMLSummary)
    tcl: TCLSummary = field(default_factory=TCLSummary)


def record_dml_summary(dml_summary: ResultSummary, table_name: str, dml: str, rowcount: int) -> NoReturn:

    dml_summary.dml.dml_count += 1
    dml_summary.dml.dml_record += rowcount

    if dml == INSERT:
        dml_summary.dml.detail[table_name].insert += rowcount
    elif dml == UPDATE:
        dml_summary.dml.detail[table_name].update += rowcount
    else:
        dml_summary.dml.detail[table_name].delete += rowcount


def print_result_summary(summary: ResultSummary, print_detail: bool = False) -> NoReturn:

    print("  ::: Execution Result :::")
    print(f"  Changed Row Count: {summary.dml.dml_record} | DML Count: {summary.dml.dml_count} | "
          f"DML {get_elapsed_time_msg(summary.execution_info.end_time, summary.execution_info.start_time)}")

    if print_detail:
        for table_name in summary.dml.detail:
            table_dml_detail = summary.dml.detail[table_name].as_dict()
            non_zero_dml_result = (" / ".join(f'{dml.upper()} ({table_dml_detail[dml]})'
                                              for dml in table_dml_detail if table_dml_detail[dml] != 0 ))
            print(f"    {table_name}: {non_zero_dml_result}")
# +----- Classes and functions related to DML statistics -----+
