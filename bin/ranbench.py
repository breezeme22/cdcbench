#!/usr/bin/env python3

import argparse
import logging
import multiprocessing
import os
import queue
import random
import sys
import time

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from sqlalchemy.schema import Table
from typing import NoReturn, Tuple
from yaspin import yaspin
from yaspin.spinners import Spinners

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, check_positive_integer_arg, DMLDetail, get_start_time_msg,
                        print_error, ResultSummary, convert_sample_table_alias, get_elapsed_time_msg,
                        get_total_result_msg, get_detail_result_msg, DBWorkToolBox, inspect_table, inspect_columns,
                        get_end_msg)
from lib.config import ConfigManager, ConfigModel
from lib.data import DataManager
from lib.definition import SADeclarativeManager
from lib.globals import *
from lib.logger import LogManager, configure_logger
from lib.sql import DML, execute_tcl


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
                tmp_item = item.strip(",")
            return convert_sample_table_alias(tmp_item)
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

    parser_ranbench.add_argument("--custom-data", action="store_true",
                                 help="DML data is used as user-custom data files")

    parser_ranbench.add_argument("-u", "--user", type=check_positive_integer_arg, default=1,
                                 help="")

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

    try:

        multiprocessing.current_process().name = "Main"
        print(get_start_time_msg(datetime.now()))
        main_start_time = time.time()

        config_mgr = ConfigManager(args.config)

        if args.command == "config":
            config_mgr.open_config_file()
            exit(1)

        config = config_mgr.get_config()

        log_mgr = LogManager()
        configure_logger(log_mgr.queue, config.settings.log_level, config.settings.sql_logging)
        logger = logging.getLogger(CDCBENCH)

        logger.info(f"ranbench start at {datetime.fromtimestamp(main_start_time)}")

        if args.database:
            if args.database not in (d.upper() for d in config.databases.keys()):
                print_error(f"[ {args.database} ] is DB key name that does not exist.")
        else:
            args.database = list(config.databases.keys())[0]

        tool_box = DBWorkToolBox()
        tool_box.conn_info = config.databases[args.database]

        logger.debug("Get DBMS declarative base")
        decl_base = SADeclarativeManager(tool_box.conn_info, args.tables).get_dbms_base()

        logger.debug("inspect table...")
        tool_box.tables = {table_name: inspect_table(decl_base.metadata, table_name) for table_name in args.tables}

        logger.debug("inspect table columns...")
        tool_box.table_columns = {table_name: inspect_columns(tool_box.tables[table_name])
                                  for table_name in tool_box.tables}

        logger.debug("Make table data")
        tool_box.table_data = {table_name: DataManager(table_name, args.custom_data)
                               for table_name in tool_box.tables}

        logger.info("Execute child process")
        with yaspin(Spinners.line, text=get_description_msg(), side="right") as sp:
            with ProcessPoolExecutor(max_workers=args.user) as executor:
                futures = {proc_id: executor.submit(args.func, proc_id, log_mgr.queue, tool_box)
                           for proc_id in range(args.user+1)}
                future_results = {proc_seq: futures[proc_seq].result() for proc_seq in futures}
            sp.ok(get_end_msg(COMMIT if not args.rollback else ROLLBACK, "\n"))

        print(get_total_result_msg(future_results))
        result_log = f"\n{get_total_result_msg(future_results, print_detail=True)}"
        if args.user > 1:
            print(get_detail_result_msg(future_results))
            result_log += f"\n{get_detail_result_msg(future_results, print_detail=True)}"
        logger.info(result_log)

        logger.info("ranbench end")

        log_mgr.listener.terminate()

        print(f"  Main {get_elapsed_time_msg(end_time=time.time(), start_time=main_start_time)}")

    except KeyboardInterrupt:
        print(f"\n{__file__}: warning: operation is canceled by user\n")
        exit(1)

    finally:
        print()


def get_description_msg() -> str:
    return f"  Generate random dml for each table ..."


def make_random_dml_meta(args: argparse.Namespace, dml: DML) -> Tuple:

    random_record = random.randrange(args.record_range[0], args.record_range[1] + 1)

    random_table = random.choice(list(dml.tables.values()))

    random_dml = random.choice(args.dml)

    return random_record, random_table, random_dml


def pre_task_execute_random_dml(random_dml: str, random_table: Table, random_record: int, dml: DML) -> bool:

    if random_dml in [UPDATE, DELETE]:
        random_table_row_count = dml.get_table_count(random_table)
        if random_table_row_count < random_record:
            return False

    if random_table.name not in dml.summary.dml.detail:
        dml.summary.dml.detail[random_table.name] = DMLDetail()

    return True


def setup_child_process(args: argparse.Namespace, config: ConfigModel, tool_box: DBWorkToolBox,
                        proc_id: int, log_queue: queue.Queue) -> DML:

    multiprocessing.current_process().name = f"User{proc_id}"

    configure_logger(log_queue, config.settings.log_level, config.settings.sql_logging)

    dml = DML(args, config, tool_box)

    dml.summary.execution_info.start_time = time.time()

    return dml


def teardown_command(args: argparse.Namespace, dml: DML) -> NoReturn:

    execute_tcl(dml.conn, args.rollback, dml.summary)

    dml.summary.execution_info.end_time = time.time()

    dml.conn.close()


def total_record(args: argparse.Namespace, config: ConfigModel, tool_box: DBWorkToolBox,
                 proc_id: int, log_queue: queue.Queue) -> ResultSummary:

    dml = setup_child_process(args, config, tool_box, proc_id, log_queue)

    while True:

        random_record, random_table, random_dml = make_random_dml_meta(args, dml)

        if random_record > (args.total_record - dml.summary.dml.dml_record):
            random_record = args.total_record - dml.summary.dml.dml_record

        if not pre_task_execute_random_dml(random_dml, random_table, random_record, dml):
            continue

        dml.execute_random_dml(random_table, random_record, random_dml)

        time.sleep(args.sleep)

        if dml.summary.dml.dml_record >= args.total_record:
            break

    teardown_command(args, dml)

    return dml.summary


def dml_count(args: argparse.Namespace, config: ConfigModel, tool_box: DBWorkToolBox,
              proc_id: int, log_queue: queue.Queue) -> ResultSummary:

    dml = setup_child_process(args, config, tool_box, proc_id, log_queue)

    while True:

        random_record, random_table, random_dml = make_random_dml_meta(args, dml)

        if not pre_task_execute_random_dml(random_dml, random_table, random_record, dml):
            continue

        dml.execute_random_dml(random_table, random_record, random_dml)

        time.sleep(args.sleep)

        if dml.summary.dml.dml_count == args.dml_count:
            break

    teardown_command(args, dml)

    return dml.summary


def run_time(args: argparse.Namespace, config: ConfigModel, tool_box: DBWorkToolBox,
             proc_id: int, log_queue: queue.Queue) -> ResultSummary:

    dml = setup_child_process(args, config, tool_box, proc_id, log_queue)

    run_end_time = time.time() + args.run_time

    while True:

        random_record, random_table, random_dml = make_random_dml_meta(args, dml)

        if not pre_task_execute_random_dml(random_dml, random_table, random_record, dml):
            continue

        dml.execute_random_dml(random_table, random_record, random_dml)

        time.sleep(args.sleep)

        if time.time() >= run_end_time:
            break

    teardown_command(args, dml)

    return dml.summary


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
