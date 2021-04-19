#!/usr/bin/env python3

import argparse
import os
import sys

from datetime import datetime
from typing import NoReturn

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, check_positive_integer_arg,
                        get_start_time_msg, isint, print_error, print_end_msg, ResultSummary, print_result_summary)
from lib.config import ConfigManager, ConfigModel
from lib.dml import DML
from lib.globals import *
from lib.logger import LoggerManager


def cli() -> NoReturn:

    parser_main = argparse.ArgumentParser(prog="ranbench", formatter_class=CustomHelpFormatter)
    parser_main.add_argument("--version", action="version", version=get_version())

    parser_ranbench = argparse.ArgumentParser(add_help=False)

    parser_ranbench.add_argument("-n", "--record-range", action="store", nargs="+", required=True,
                                 metavar=("start_count", "end_count"), type=check_positive_integer_arg,
                                 help="Specifies the range of the amount of records to enter per DML.")

    def check_tables_arg_values(item: str) -> list or str:
        if item:
            tmp_item: list or str = None
            if item != ",":
                tmp_item = item.strip(",").upper()

            if tmp_item.startswith("S"):
                return STRING_TEST
            elif tmp_item.startswith("N"):
                return NUMERIC_TEST
            elif tmp_item.startswith("D"):
                return DATETIME_TEST
            elif tmp_item.startswith("B"):
                return BINARY_TEST
            elif tmp_item.startswith("L"):
                return LOB_TEST
            elif tmp_item.startswith("O"):
                return ORACLE_TEST
            elif tmp_item.startswith("Q"):
                return SQLSERVER_TEST
            else:
                return tmp_item

        else:
            raise argparse.ArgumentTypeError(f"argument value [ {item} ] is invalid syntax.")

    parser_ranbench.add_argument("-t", "--tables", action="store", nargs="+", metavar=("table_name", "table_name"),
                                 type=check_tables_arg_values,
                                 default=[STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST],
                                 help="Specifies tables that will generate random DML. \n"
                                      "Allowed alias: \n"
                                      "  S (STRING_TEST) / N (NUMERIC_TEST) / D (DATETIME_TEST) / \n"
                                      "  B (BINARY_TEST) / L (LOB_TEST) / O (ORACLE_TEST) / Q (SQLSERVER_TEST) \n"
                                      "Default: STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST")

    def check_dml_arg_values(item: str) -> list or str:
        if item:
            tmp_item: list or str = None
            if item != ",":
                tmp_item = item.strip(",").upper()

            if tmp_item.startswith("I"):
                return INSERT
            elif tmp_item.startswith("U"):
                return UPDATE
            elif tmp_item.startswith("D"):
                return DELETE
            else:
                return tmp_item

        else:
            raise argparse.ArgumentTypeError(f"argument value [ {item} ] is invalid syntax.")

    parser_ranbench.add_argument("-d", "--dml", choices=[INSERT, UPDATE, DELETE], nargs="+", type=check_dml_arg_values,
                                 metavar="dml_type",
                                 default=[INSERT, UPDATE, DELETE],
                                 help="Specifies the DML to occur. \n"
                                      "Allowed alias: i (INSERT) / u (UPDATE) / d (DELETE) \n"
                                      "Default: INSERT, UPDATE, DELETE")

    parser_ranbench.add_argument("-s", "--sleep", action="store", metavar="idle_time (sec)", type=float, default=0,
                                 help="Specifies the idle time to occur per DML (Default. 0)")

    parser_ranbench.add_argument("-r", "--rollback", action="store_true",
                                 help="Rollbacks the entered data.")

    parser_ranbench.add_argument("-db", "--database", action="store", metavar="db_key",
                                 type=lambda item: item.upper(),
                                 help="Specifies database.")

    parser_ranbench.add_argument("-f", "--config", action="store", metavar="configuration_file_name",
                                 default=DEFAULT_CONFIG_FILE_NAME,
                                 help="Specifies configuration file.")

    parser_ranbench.add_argument("-v", "--verbose", action="store_false",
                                 help="Displays the progress of the operation.")

    parser_command = parser_main.add_subparsers(dest="command", metavar="<Command>", required=True)

    command_total_record = parser_command.add_parser("total-record", aliases=["C", "record"],
                                                     formatter_class=CustomHelpFormatter, parents=[parser_ranbench],
                                                     help="Generates random DMLs by the total number of records.")

    command_total_record.add_argument(dest="total_record", metavar="record count", type=check_positive_integer_arg,
                                      help="")

    command_total_record.set_defaults(func=total_record)

    command_dml_count = parser_command.add_parser("dml-count", aliases=["D", "dml"],
                                                  formatter_class=CustomHelpFormatter, parents=[parser_ranbench],
                                                  help="Generates random DMLs by the DML count.")

    command_dml_count.add_argument(dest="dml_count", metavar="dml count", type=check_positive_integer_arg,
                                   help="")

    command_dml_count.set_defaults(func=dml_count)

    command_run_time = parser_command.add_parser("run-time", aliases=["T", "time"], formatter_class=CustomHelpFormatter,
                                                 parents=[parser_ranbench],
                                                 help="Generates random DMLs for a specified time.")

    command_run_time.add_argument(dest="run_time", metavar="run time", type=check_positive_integer_arg,
                                  help="")

    command_run_time.set_defaults(func=run_time)

    args = parser_main.parse_args()

    if len(args.record_range) == 1:
        args.record_range = [args.record_range[0], args.record_range[0]]
    elif len(args.record_range) == 2:
        if args.record_range[0] <= args.record_range[1]:
            pass
        else:
            parser_ranbench.error("--record-range option's second argument is less than first argument")
    else:
        parser_ranbench.error("--range option's argument is allowed up to two argument")

    print(args)

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
        # print_end_msg(COMMIT if not args.rollback else ROLLBACK, args.verbose, end="\n")
        #
        # print_result_summary(result)

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()


def total_record() -> ResultSummary:
    pass


def dml_count() -> ResultSummary:
    pass


def run_time() -> ResultSummary:
    pass


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
