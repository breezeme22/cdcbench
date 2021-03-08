#!/usr/bin/env python3

import argparse
import json
import os
import sys

from datetime import datetime
from typing import NoReturn

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, get_elapsed_time_msg, print_error,
                        DatabaseMetaData, print_end_msg, isint)
from lib.config import ConfigManager
from lib.connection import ConnectionManager
from lib.definition import SADeclarativeManager
from lib.globals import DEFAULT_CONFIG_FILE_NAME
from lib.logger import LoggerManager


def cli() -> NoReturn:

    parser_main = argparse.ArgumentParser(prog="cdcbench", formatter_class=CustomHelpFormatter)
    parser_main.add_argument("--version", action="version", version=get_version())

    parser_cdcbench = argparse.ArgumentParser(add_help=False)
    parser_cdcbench.add_argument("-t", "--table", action="store", metavar="<Table name>",
                                 help="Specifies table.")
    parser_cdcbench.add_argument("-c", "--commit", action="store", metavar="<Commit unit>", type=int,
                                 help="Specifies the commit unit.")
    parser_cdcbench.add_argument("-r", "--rollback", action="store_true",
                                 help="Rollbacks the entered data.")
    parser_cdcbench.add_argument("-C", "--columns", action="store", nargs="+", metavar="<column ID | Name>",
                                 type=lambda item: int(item) if isint(item) else item,
                                 help="Specifies the column in which want to perform DML \n"
                                      "(cannot use a combination of column id and column name)")
    parser_cdcbench.add_argument("--custom-data", action="store_true",
                                 help="DML data is used as user-custom data files when using Non-sample table")
    parser_cdcbench.add_argument("-db", "--database", action="store", metavar="<DB key>",
                                 help="Specifies database.")
    parser_cdcbench.add_argument("-f", "--config", action="store", metavar="<Configuration file name>",
                                 default=DEFAULT_CONFIG_FILE_NAME,
                                 help="Specifies configuration file.")
    parser_cdcbench.add_argument("-v", "--verbose", action="store_false",
                                 help="Displays the progress of the operation.")

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
    print(args)


def insert():
    pass


def update():
    pass


def delete():
    pass


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
