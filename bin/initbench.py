#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import time

from typing import Dict, NoReturn, Optional

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, view_runtime_config, get_elapsed_time_msg, print_error,
                        DBWorkToolBox, print_end_msg)
from lib.config import ConfigManager
from lib.connection import ConnectionManager
from lib.definition import SADeclarativeManager
from lib.globals import *
from lib.initial import create_objects, drop_objects, generate_initial_data
from lib.logger import LogManager, configure_logger


WITHOUT = "WITHOUT"
ONLY = "ONLY"


def get_continue_flag(args: argparse.Namespace) -> Optional[bool]:
    if args.assumeyes:
        print("Y")
        return True

    user_input = input(
        f"Do you want to {args.command} CDCBENCH related objects and data in the above databases [y/N]: ")

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

    try:

        config_mgr = ConfigManager(args.config)

        if args.command == "config":
            config_mgr.open_config_file()
            exit(1)

        config = config_mgr.get_config()

        log_mgr = LogManager()
        configure_logger(log_mgr.queue, config.settings.log_level, config.settings.sql_logging)
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

        tool_boxes: Dict[str, DBWorkToolBox] = {}
        # 한번 초기화가된 DeclarativeBase가 또다시 초기화되지 않도록 관리하는 Dictionary
        decl_bases: Dict[str, SADeclarativeManager] = {}

        for db_key in args.database:
            tool_boxes[db_key] = DBWorkToolBox()
            tool_boxes[db_key].conn_info = config.databases[db_key]
            tool_boxes[db_key].engine = ConnectionManager(tool_boxes[db_key].conn_info).engine
            if tool_boxes[db_key].conn_info.dbms not in decl_bases:
                decl_bases[tool_boxes[db_key].conn_info.dbms] = SADeclarativeManager(tool_boxes[db_key].conn_info)
            tool_boxes[db_key].decl_base = decl_bases[tool_boxes[db_key].conn_info.dbms].get_dbms_base()
            tool_boxes[db_key].description = f"{db_key} Database"

        start_time = time.time()
        args.func(args, tool_boxes)
        print(f"  {get_elapsed_time_msg(end_time=time.time(), start_time=start_time)}")

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()


def create(args: argparse.Namespace, tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:

    print("  Create tables & sequences ")

    for idx, db_key in enumerate(args.database):
        create_objects(tool_boxes[db_key], args)
        if idx + 1 != len(args.database):
            print_end_msg(COMMIT, args.verbose, separate=False)
        else:
            print_end_msg(COMMIT, args.verbose, end="\n")


def drop(args: argparse.Namespace, tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:

    print("  Drop tables & sequences")

    for idx, db_key in enumerate(args.database):
        drop_objects(tool_boxes[db_key], args)
        if idx + 1 != len(args.database):
            print_end_msg(COMMIT, args.verbose, separate=False)
        else:
            print_end_msg(COMMIT, args.verbose, end="\n")


def reset(args: argparse.Namespace, tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:
    drop(args, tool_boxes)
    create(args, tool_boxes)


if __name__ == "__main__":
    # Working Directory를 ~/cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
