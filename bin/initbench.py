#!/usr/bin/env python3

import argparse
import logging
import multiprocessing
import os
import sys
import time
import threading

from sqlalchemy.schema import Table, Column
from typing import Dict, NoReturn, Optional, List

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, view_runtime_config, get_elapsed_time_msg, print_error,
                        DBWorkToolBox, check_positive_integer_arg, inspect_table, inspect_columns)
from lib.config import ConfigManager, ConfigModel
from lib.connection import ConnectionManager
from lib.data import DataManager
from lib.definition import SADeclarativeManager, TYPE_DBMS_DECL_BASE
from lib.globals import *
from lib.initial import create_objects, drop_objects, generate_initial_data
from lib.logger import LogManager, close_log_listener


WITHOUT = "WITHOUT"
ONLY = "ONLY"


def get_continue_flag(args: argparse.Namespace) -> Optional[bool]:
    if args.assumeyes:
        print("Y")
        return True

    user_input = input(
        f"Do you want to {args.func.__name__.title()} CDCBENCH related objects and data in the above databases [y/N]: ")

    if len(user_input) == 0 or user_input is None:
        user_input = "NO"

    user_input = user_input.strip().upper()

    if user_input == "Y":
        return True
    elif user_input == "N":
        return False
    else:
        return None


def cli() -> NoReturn:

    parser_main = argparse.ArgumentParser(prog="initbench", formatter_class=CustomHelpFormatter)
    parser_main.add_argument("--version", action="version", version=get_version())

    parser_initbench = argparse.ArgumentParser(add_help=False)

    def check_database_arg_value(item: str) -> list or str:
        if item:
            tmp_item: list or str = None
            if item != ",":
                tmp_item = item.strip(",").upper()
            return tmp_item
        else:
            raise argparse.ArgumentTypeError(f"argument value [ {item} ] is invalid syntax.")

    parser_initbench.add_argument("-db", "--database", action="store", nargs="+", metavar=("<DB Key>", "DB Key"),
                                  type=check_database_arg_value, help="Specifies database.")
    parser_initbench.add_argument("-f", "--config", action="store", metavar="<Configuration file name>",
                                  default=DEFAULT_CONFIG_FILE_NAME,
                                  help="Specifies configuration file.")
    parser_initbench.add_argument("-y", "--assumeyes", action="store_true",
                                  help="Answers yes for question.")
    parser_initbench.add_argument("-v", "--verbose", action="store_false",
                                  help="Displays the progress of the operation.")

    parser_create_reset = argparse.ArgumentParser(add_help=False, parents=[parser_initbench])

    def convert_key_args_alias(item: str) -> str:
        if item.upper().startswith("P"):
            return PRIMARY_KEY_BAR
        elif item.upper().startswith("U"):
            return UNIQUE
        elif item.upper().startswith("N"):
            return NON_KEY_BAR
        else:
            return item
    parser_create_reset.add_argument("-k", "--key", choices=[PRIMARY_KEY_BAR, UNIQUE, NON_KEY_BAR],
                                     type=convert_key_args_alias, default=PRIMARY_KEY_BAR, help="")

    def convert_data_args_alias(item: str) -> str:
        if item.upper().startswith("W"):
            return WITHOUT
        elif item.upper().startswith("O"):
            return ONLY
        else:
            return item
    parser_create_reset.add_argument("-d", "--data", choices=[WITHOUT, ONLY], type=convert_data_args_alias,
                                     help="")

    parser_create_reset.add_argument("--custom-data", action="store_true",
                                     help="DML data is used as user-custom data files")

    parser_command = parser_main.add_subparsers(dest="command", metavar="<Command>", required=True)

    command_create = parser_command.add_parser("create", aliases=["c"], parents=[parser_create_reset],
                                               formatter_class=CustomHelpFormatter,
                                               description="Creates objects and initial data.",
                                               help="Creates objects and initial data.")
    command_create.set_defaults(func=create)

    command_drop = parser_command.add_parser("drop", aliases=["d"], parents=[parser_initbench],
                                             formatter_class=CustomHelpFormatter,
                                             help="Drops objects.")
    command_drop.set_defaults(func=drop)

    command_reset = parser_command.add_parser("reset", aliases=["r"], parents=[parser_create_reset],
                                              formatter_class=CustomHelpFormatter,
                                              help="Re-create objects and initial data.")
    command_reset.set_defaults(func=reset)

    command_config = parser_command.add_parser("config",
                                               help="Opens configuration file.")
    command_config.add_argument(dest="config", action="store", nargs="?", metavar="Configuration file name",
                                default=DEFAULT_CONFIG_FILE_NAME)

    args = parser_main.parse_args()
    log_mgr = LogManager(multiprocessing.Queue())
    log_listener = threading.Thread(target=log_mgr.log_listening)

    try:

        multiprocessing.current_process().name = "Main"

        config_mgr = ConfigManager(args.config)

        if args.command == "config":
            config_mgr.open_config_file()
            exit(1)

        config = config_mgr.get_config()

        log_mgr.configure_logger(config.settings.log_level, config.settings.sql_logging)
        log_listener.start()
        logger = logging.getLogger(CDCBENCH)

        if args.database:
            if "ALL" in args.database:
                args.database = list(config.databases.keys())
            else:
                db_key_names = set(config.databases.keys())
                set_database = set(d.upper() for d in args.database)
                database_opts_diff_db_key_names = set_database.difference(db_key_names)
                if len(database_opts_diff_db_key_names) >= 1:
                    print_error(f"[ {', '.join(database_opts_diff_db_key_names)} ] is DB key name that does not exist.")
        else:
            args.database = [list(config.databases.keys())[0]]

        # DBMS 미지원 옵션 예외처리
        for dbk in args.database:
            if config.databases[dbk].dbms == MYSQL and args.key == NON_KEY_BAR:
                print_error("Non-key table is not supported in MySQL, MariaDB")
            elif config.databases[dbk].dbms in sa_unsupported_dbms and args.command in ("create", "drop"):
                print_error(f"For {config.databases[dbk].dbms}, only --without-data option is supported.")

        print(view_runtime_config(config, args))

        while True:
            continue_flag = get_continue_flag(args)
            if continue_flag is True:
                print()
                break
            elif continue_flag is False:
                print(f"{__file__}: warning: operation is canceled by user")
                exit(1)
            else:
                print(f'{__file__}: warning: invalid value. please enter "Y" or "N".\n')

        main_start_time = time.time()

        tool_boxes: Dict[str, DBWorkToolBox] = {}
        # 한번 초기화가된 DeclarativeBase가 또다시 초기화되지 않도록 관리하는 Dictionary
        dbms_bases: Dict[str, TYPE_DBMS_DECL_BASE] = {}
        dbms_tables: Dict[str, Dict[str, Table]] = {}
        dbms_table_columns: Dict[str, Dict[str, Column]] = {}

        for db_key in args.database:
            tool_boxes[db_key] = DBWorkToolBox()
            tool_boxes[db_key].conn_info = config.databases[db_key]
            tool_boxes[db_key].engine = ConnectionManager(tool_boxes[db_key].conn_info).engine

            if tool_boxes[db_key].conn_info.dbms not in dbms_bases:
                dbms_bases[tool_boxes[db_key].conn_info.dbms] = \
                    SADeclarativeManager(tool_boxes[db_key].conn_info).get_dbms_base()
                dbms_tables[tool_boxes[db_key].conn_info.dbms] = {
                    table.name: table for table in dbms_bases[tool_boxes[db_key].conn_info.dbms].metadata.sorted_tables
                }
                dbms_table_columns[tool_boxes[db_key].conn_info.dbms] = {
                    table_name: inspect_columns(dbms_tables[tool_boxes[db_key].conn_info.dbms][table_name])
                    for table_name in dbms_tables[tool_boxes[db_key].conn_info.dbms]
                }

            tool_boxes[db_key].tables = dbms_tables[tool_boxes[db_key].conn_info.dbms]
            if args.func.__name__.upper() != "DROP":
                tool_boxes[db_key].table_columns = dbms_table_columns[tool_boxes[db_key].conn_info.dbms]
                tool_boxes[db_key].table_data = {table_name: DataManager(table_name, args.custom_data)
                                                 for table_name in config.initial_data}
            tool_boxes[db_key].description = f"{db_key} Database"

        args.func(args, config, tool_boxes)

        print(f"  Main {get_elapsed_time_msg(end_time=time.time(), start_time=main_start_time)}")

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()
        close_log_listener(log_mgr.queue, log_listener)


def create(args: argparse.Namespace, config: ConfigModel, tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:

    if args.data == ONLY:

        print("  Generate initial data ")
        generate_initial_data(args, config, tool_boxes)

        print()

    else:

        print("  Create tables & sequences ")

        for db_key in tool_boxes:
            create_objects(tool_boxes[db_key])

        print()

        if args.data != WITHOUT:
            print("  Generate initial data ")
            generate_initial_data(args, config, tool_boxes)

            print()


def drop(args: argparse.Namespace, config: ConfigModel, tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:

    print("  Drop tables & sequences")

    for db_key in tool_boxes:
        drop_objects(tool_boxes[db_key])

    print()


def reset(args: argparse.Namespace, config: ConfigModel, tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:
    drop(args, config, tool_boxes)
    create(args, config, tool_boxes)


if __name__ == "__main__":
    # Working Directory를 ~/cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
