#!/usr/bin/env python3

import argparse
import logging
import os
import random
import sys
import time

from datetime import datetime
from sqlalchemy.schema import Table
from typing import NoReturn, Tuple
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, check_positive_integer_arg, DMLDetail,
                        get_start_time_msg, isint, print_error, print_end_msg, ResultSummary, print_result_summary)
from lib.config import ConfigManager, ConfigModel
from lib.sql import RandomDML, execute_tcl
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

    parser_ranbench.add_argument("--custom-data", action="store_true",
                                 help="DML data is used as user-custom data files when using Non-sample table")

    parser_command = parser_main.add_subparsers(dest="command", metavar="<Command>", required=True)

    command_total_record = parser_command.add_parser("total-record", aliases=["R", "record"],
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

        print(get_start_time_msg(datetime.now()))
        print_description_msg(args.verbose)

        result = args.func(args, config)

        print_end_msg(COMMIT if not args.rollback else ROLLBACK, args.verbose, end="\n")

        print_result_summary(result, print_detail=True)

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()


# Command
TOTAL_RECORD = "total-record"
DML_COUNT = "dml-count"
RUN_TIME = "run-time"



def print_description_msg(end_flag: bool) -> NoReturn:
    if end_flag:
        print(f"  Generate random dml for each table ... ", end="", flush=True)
    else:
        print(f"  Generate random dml for each table ... ", flush=True)


def make_random_dml_meta(args: argparse.Namespace, rdml: RandomDML) -> Tuple:

    random_record = random.randrange(args.record_range[0], args.record_range[1] + 1)

    random_table = random.choice(rdml.tables)

    random_dml = random.choice(args.dml)

    return random_record, random_table, random_dml


def pre_task_execute_random_dml(random_dml: str, random_table: Table, random_record: int, rdml: RandomDML) -> bool:

    if random_dml in [UPDATE, DELETE]:
        random_table_row_count = rdml.get_table_count(random_table)
        if random_table_row_count < random_record:
            return False

    if random_table.name not in rdml.summary.dml.detail:
        rdml.summary.dml.detail[random_table.name] = DMLDetail()

    return True


def setup_command(args: argparse.Namespace, config: ConfigModel, command: str) -> Tuple:

    rdml = RandomDML(args, config)

    if command == TOTAL_RECORD:
        total = args.total_record
        bar_format = tqdm_bar_format
    elif command == DML_COUNT:
        total = args.dml_count
        bar_format = tqdm_bar_format
    else:
        total = args.run_time
        bar_format = tqdm_bar_float_format

    progress_bar = tqdm(total=total, disable=args.verbose, ncols=tqdm_ncols, bar_format=bar_format,
                        postfix=tqdm_bench_postfix(args.rollback))

    rdml.summary.execution_info.start_time = time.time()

    return rdml, progress_bar


def teardown_command(args: argparse.Namespace, rdml: RandomDML, progress_bar: tqdm) -> NoReturn:

    execute_tcl(rdml.conn, args.rollback, rdml.summary)

    rdml.summary.execution_info.end_time = time.time()

    rdml.conn.close()
    progress_bar.close()


def total_record(args: argparse.Namespace, config: ConfigModel) -> ResultSummary:

    rdml, progress_bar = setup_command(args, config, TOTAL_RECORD)

    while True:

        random_record, random_table, random_dml = make_random_dml_meta(args, rdml)

        if random_record > (args.total_record - rdml.summary.dml.dml_record):
            random_record = args.total_record - rdml.summary.dml.dml_record

        if not pre_task_execute_random_dml(random_dml, random_table, random_record, rdml):
            continue

        rdml.execute_random_dml(random_table, random_record, random_dml)

        progress_bar.update(random_record)
        time.sleep(args.sleep)

        if rdml.summary.dml.dml_record >= args.total_record:
            break

    teardown_command(args, rdml, progress_bar)

    return rdml.summary


def dml_count(args: argparse.Namespace, config: ConfigModel) -> ResultSummary:

    rdml, progress_bar = setup_command(args, config, DML_COUNT)

    while True:

        random_record, random_table, random_dml = make_random_dml_meta(args, rdml)

        if not pre_task_execute_random_dml(random_dml, random_table, random_record, rdml):
            continue

        rdml.execute_random_dml(random_table, random_record, random_dml)

        progress_bar.update(1)
        time.sleep(args.sleep)

        if rdml.summary.dml.dml_count == args.dml_count:
            break

    teardown_command(args, rdml, progress_bar)

    return rdml.summary


def run_time(args: argparse.Namespace, config: ConfigModel) -> ResultSummary:

    rdml, progress_bar = setup_command(args, config, RUN_TIME)

    run_end_time = time.time() + args.run_time

    while True:

        dml_start_time = time.time()

        random_record, random_table, random_dml = make_random_dml_meta(args, rdml)

        if not pre_task_execute_random_dml(random_dml, random_table, random_record, rdml):
            continue

        rdml.execute_random_dml(random_table, random_record, random_dml)

        time.sleep(args.sleep)

        dml_end_time = time.time()

        if time.time() >= run_end_time:
            break
        
        # break 이전에 update하면 progress bar 초과하는 이슈 발생 가능성 존재
        progress_bar.update(dml_end_time - dml_start_time)

    teardown_command(args, rdml, progress_bar)

    return rdml.summary


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
