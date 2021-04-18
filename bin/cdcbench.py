#!/usr/bin/env python3

import argparse
import os
import sys

from datetime import datetime
from typing import NoReturn

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, get_start_time_msg, isint, check_positive_integer_arg,
                        print_error, print_end_msg, ResultSummary, print_result_summary)
from lib.config import ConfigManager, ConfigModel
from lib.dml import DML
from lib.globals import *
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

    parser_cdcbench.add_argument("-c", "--commit", action="store", metavar="<Commit unit>",
                                 type=check_positive_integer_arg, default=1000,
                                 help="Specifies the commit unit.")

    parser_cdcbench.add_argument("-r", "--rollback", action="store_true",
                                 help="Rollbacks the entered data.")

    parser_cdcbench.add_argument("-db", "--database", action="store", metavar="<DB key>",
                                 type=lambda item: item.upper(),
                                 help="Specifies database.")

    parser_cdcbench.add_argument("-f", "--config", action="store", metavar="<Configuration file name>",
                                 default=DEFAULT_CONFIG_FILE_NAME,
                                 help="Specifies configuration file.")

    parser_cdcbench.add_argument("-v", "--verbose", action="store_false",
                                 help="Displays the progress of the operation.")

    parser_update_delete = argparse.ArgumentParser(add_help=False, parents=[parser_cdcbench])

    parser_update_delete.add_argument(dest="start_id", action="store", nargs="?", metavar="Start ID",
                                      type=check_positive_integer_arg, default=None,
                                      help="Update/Delete the data in the specified id value range.")

    parser_update_delete.add_argument(dest="end_id", action="store", nargs="?", metavar="End ID",
                                      type=check_positive_integer_arg, default=None,
                                      help="Update/Delete the data in the specified id value range.")

    parser_update_delete.add_argument("-w", "--where", action="store", metavar="<where clause>",
                                      help="Specifies the update or delete conditions \n"
                                           "(ex. update --where \"t_id = 1\")")
    # parser_update_delete.add_argument("-sep", "--separate-tx", action="store", metavar="<column ID | Name>",
    #                                   type=lambda column: int(column) if isint(column) else column,
    #                                   help="Separate transactions based on the specified column \n"
    #                                        "(--where option required)")

    parser_insert_update = argparse.ArgumentParser(add_help=False)

    def validate_columns_args(item: str) -> list or str:
        if item:
            processed_item: list or str = None
            if item != ",":
                processed_item = item.strip(",").upper()
            if isint(processed_item):
                int_item = int(processed_item)
                if int_item <= 0:
                    raise argparse.ArgumentTypeError(f"Invalid Column ID. [ {int_item} ]")
                return int_item
            else:
                return processed_item
        else:
            raise argparse.ArgumentTypeError(f"--columns option value [ {item} ] is invalid syntax")

    parser_insert_update.add_argument("-C", "--columns", action="store", nargs="+", metavar="<column ID | Name>",
                                      type=validate_columns_args,
                                      help="Specifies the column in which want to perform DML")

    parser_insert_update.add_argument("--custom-data", action="store_true",
                                      help="DML data is used as user-custom data files when using Non-sample table")

    parser_command = parser_main.add_subparsers(dest="command", metavar="<Command>", required=True)

    command_insert = parser_command.add_parser("insert", aliases=["i", "ins"], formatter_class=CustomHelpFormatter,
                                               parents=[parser_cdcbench, parser_insert_update],
                                               help="Insert data.")

    command_insert.add_argument(dest="record", metavar="record count", type=check_positive_integer_arg,
                                help="Insert the data as much as the corresponding value.")

    command_insert.add_argument("-s", "--single", action="store_true",
                                help="Changes to single insert.")

    command_insert.set_defaults(func=insert)

    command_update = parser_command.add_parser("update", aliases=["u", "upd"], formatter_class=CustomHelpFormatter,
                                               parents=[parser_update_delete, parser_insert_update],
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

    try:
        config_mgr = ConfigManager(args.config)

        if args.command == "config":
            config_mgr.open_config_file()
            exit(1)

        config = config_mgr.get_config()
        logger = LoggerManager.get_logger(__name__)

        if args.database:
            if args.database not in (d.upper() for d in config.databases.keys()):
                print_error(f"[ {args.database} ] is DB key name that does not exist.")
        else:
            args.database = list(config.databases.keys())[0]

        result = args.func(args, config)
        print_end_msg(COMMIT if not args.rollback else ROLLBACK, args.verbose, end="\n")

        print_result_summary(result)

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()


def print_description_msg(dml: str, table_name: str, end_flag: bool) -> NoReturn:
    if end_flag:
        print(f"  {dml.title()} data in the \"{table_name}\" Table ... ", end="", flush=True)
    else:
        print(f"  {dml.title()} data in the \"{table_name}\" Table ... ", flush=True)


def insert(args: argparse.Namespace, config: ConfigModel) -> ResultSummary:

    if args.table is None:
        args.table = INSERT_TEST

    dml = DML(args, config)

    print(get_start_time_msg(datetime.now()))
    print_description_msg(INSERT, args.table, args.verbose)

    if args.single:
        dml.single_insert()
    else:
        dml.multi_insert()

    return dml.summary


def update(args: argparse.Namespace, config: ConfigModel) -> NoReturn:

    if args.table is None:
        args.table = UPDATE_TEST

    if args.table == UPDATE_TEST:
        args.columns = ["COL_NAME"]

    if args.start_id and args.end_id is None:
        args.end_id = args.start_id

    dml = DML(args, config)

    print(get_start_time_msg(datetime.now()))
    print_description_msg(UPDATE, args.table, args.verbose)

    if args.start_id is None and args.end_id is None:
        dml.where_update()
    else:
        dml.sequential_update()

    return dml.summary


def delete(args: argparse.Namespace, config: ConfigModel) -> NoReturn:

    if args.table is None:
        args.table = DELETE_TEST

    if args.start_id and args.end_id is None:
        args.end_id = args.start_id

    dml = DML(args, config)

    print(get_start_time_msg(datetime.now()))
    print_description_msg(DELETE, args.table, args.verbose)

    if args.start_id is None and args.end_id is None:
        dml.where_delete()
    else:
        dml.sequential_delete()

    return dml.summary


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
