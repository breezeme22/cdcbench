#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time

from datetime import datetime
from typing import NoReturn

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, get_elapsed_time_msg, print_error,
                        DatabaseWorkInfo, print_end_msg, isint)
from lib.config import ConfigManager
from lib.connection import ConnectionManager
from lib.definition import SADeclarativeManager
from lib.globals import (DEFAULT_CONFIG_FILE_NAME, STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST,
                         ORACLE_TEST, SQLSERVER_TEST, INSERT_TEST, UPDATE_TEST, DELETE_TEST)
from lib.logger import LoggerManager


def cli() -> NoReturn:

    parser_main = argparse.ArgumentParser(prog="cdcbench", formatter_class=CustomHelpFormatter)
    parser_main.add_argument("--version", action="version", version=get_version())

    parser_cdcbench = argparse.ArgumentParser(add_help=False)

    def convert_table_args_alias(item: str) -> str:
        if item.upper().startswith("S"):
            return STRING_TEST
        elif item.upper().startswith("N"):
            return NUMERIC_TEST
        elif item.upper().startswith("D"):
            return DATETIME_TEST
        elif item.upper().startswith("B"):
            return BINARY_TEST
        elif item.upper().startswith("L"):
            return LOB_TEST
        elif item.upper().startswith("O"):
            return ORACLE_TEST
        elif item.upper().startswith("Q"):
            return SQLSERVER_TEST
        else:
            return item.upper()

    parser_cdcbench.add_argument("-t", "--table", action="store", metavar="<Table name>", type=convert_table_args_alias,
                                 help="Specifies table.\n"
                                      "Allowed alias: s (STRING_TEST) / n (NUMERIC_TEST) / d (DATETIME_TEST) / \n"
                                      "b (BINARY_TEST) / l (LOB_TEST) / o (ORACLE_TEST) / q (SQLSERVER_TEST)")
    parser_cdcbench.add_argument("-c", "--commit", action="store", metavar="<Commit unit>", type=int, default=1000,
                                 help="Specifies the commit unit.")
    parser_cdcbench.add_argument("-r", "--rollback", action="store_true",
                                 help="Rollbacks the entered data.")

    def validate_columns_args(item: str) -> list or str:
        print(item)
        if item:
            processed_item: list or str = None
            if item != ",":
                processed_item = item.strip(",").upper()
            if isint(processed_item):
                return int(processed_item)
            else:
                return processed_item
        else:
            parser_main.error(f"--columns option value [ {item} ] is invalid syntax")

    parser_cdcbench.add_argument("-C", "--columns", action="store", nargs="+", metavar="<column ID | Name>",
                                 type=validate_columns_args,
                                 help="Specifies the column in which want to perform DML \n"
                                      "(cannot use a combination of column id and column name)")
    parser_cdcbench.add_argument("-db", "--database", action="store", metavar="<DB key>",
                                 type=lambda item: item.upper(),
                                 help="Specifies database.")
    parser_cdcbench.add_argument("-f", "--config", action="store", metavar="<Configuration file name>",
                                 default=DEFAULT_CONFIG_FILE_NAME,
                                 help="Specifies configuration file.")
    parser_cdcbench.add_argument("-v", "--verbose", action="store_false",
                                 help="Displays the progress of the operation.")
    parser_cdcbench.add_argument("--custom-data", action="store_true",
                                 help="DML data is used as user-custom data files when using Non-sample table")

    parser_update_delete = argparse.ArgumentParser(add_help=False, parents=[parser_cdcbench])

    parser_update_delete.add_argument(dest="start_id", action="store", nargs="?", metavar="Start ID", type=int,
                                      default=None,
                                      help="Update/Delete the data in the specified id value range.")
    parser_update_delete.add_argument(dest="end_id", action="store", nargs="?", metavar="End ID", type=int,
                                      default=None,
                                      help="Update/Delete the data in the specified id value range.")

    parser_update_delete.add_argument("-w", "--where", action="store", metavar="<where clause>",
                                      help="Specifies the update or delete conditions \n"
                                           "(ex. --update --where \"product_id = 1\")")
    parser_update_delete.add_argument("-sep", "--separate-tx", action="store", metavar="<column ID | Name>",
                                      type=lambda column: int(column) if isint(column) else column,
                                      help="Separate transactions based on the specified column \n"
                                           "(--where option required)")

    parser_command = parser_main.add_subparsers(dest="command", metavar="<Command>", required=True)

    command_insert = parser_command.add_parser("insert", aliases=["i", "ins"], parents=[parser_cdcbench],
                                               formatter_class=CustomHelpFormatter,
                                               help="Insert data.")
    command_insert.add_argument(dest="record", metavar="<Insert record count>",
                                help="Insert the data as much as the corresponding value.")
    command_insert.add_argument("-s", "--single", action="store_true",
                                help="Changes to single insert.")
    command_insert.set_defaults(func=insert)

    command_update = parser_command.add_parser("update", aliases=["u", "upd"], parents=[parser_update_delete],
                                               formatter_class=CustomHelpFormatter,
                                               help="Update data.")
    command_update.set_defaults(func=update)

    command_delete = parser_command.add_parser("delete", aliases=["d", "del"], parents=[parser_update_delete],
                                               formatter_class=CustomHelpFormatter,
                                               help="Delete data.")
    command_delete.set_defaults(func=delete)

    command_config = parser_command.add_parser("config",
                                               help="Opens configuration file with os default editor.")
    command_config.add_argument(dest="config", metavar="<Configuration file name>")

    args = parser_main.parse_args()
    print(f"non-processed args: {args}")

    try:
        config_mgr = ConfigManager(args.config)

        if args.command == "config":
            config_mgr.open_config_file()
            exit(1)

        config = config_mgr.get_config()
        logger = LoggerManager.get_logger(__file__)

        if args.database:
            if args.database not in (d.upper() for d in config.databases.keys()):
                print_error(f"[ {args.database} ] is DB key name that does not exist.")
        else:
            args.database = list(config.databases.keys())[0]

        db_work_info = DatabaseWorkInfo()
        db_work_info.conn_info = config.databases[args.database]
        db_work_info.engine = ConnectionManager(db_work_info.conn_info).engine
        db_work_info.decl_base = SADeclarativeManager(db_work_info.conn_info).get_dbms_base()
        db_work_info.description = f"{args.database} Database"

        print(f"processed args: {args}")

        start_time = time.time()
        args.func(args, db_work_info)
        print(f"  {get_elapsed_time_msg(time.time(), start_time)}")

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()


def insert(args: argparse.Namespace, db_work_info: DatabaseWorkInfo) -> NoReturn:

    if args.table is None:
        args.table = INSERT_TEST

    try:
        table = db_work_info.decl_base.metadata.tables[args.table]
    except KeyError as KE:
        print_error(f"[ {args.table} ] table does not exist.")

    # TODO. 테이블 체크 및 columns 처리 어느 위치 (함수 안 or cli 단)에서 할지 결정, 결정 후 columns 처리까지 구현


def update(args: argparse.Namespace, db_work_info: DatabaseWorkInfo) -> NoReturn:
    pass


def delete(args: argparse.Namespace, db_work_info: DatabaseWorkInfo) -> NoReturn:
    pass


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()