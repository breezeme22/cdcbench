#!/usr/bin/env python3

import argparse
import logging
import multiprocessing
import os
import queue
import sys
import time

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import NoReturn

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from lib.common import (CustomHelpFormatter, get_version, get_start_time_msg, isint, check_positive_integer_arg,
                        print_error, print_end_msg, ResultSummary, convert_sample_table_alias, get_elapsed_time_msg,
                        get_total_result_msg, get_detail_result_msg, DBWorkToolBox, inspect_table, inspect_columns)
from lib.config import ConfigManager, ConfigModel
from lib.data import DataManager
from lib.definition import SADeclarativeManager
from lib.globals import *
from lib.logger import LogManager, configure_logger
from lib.sql import DML


def cli() -> NoReturn:

    parser_main = argparse.ArgumentParser(prog="cdcbench", formatter_class=CustomHelpFormatter)
    parser_main.add_argument("--version", action="version", version=get_version())

    parser_cdcbench = argparse.ArgumentParser(add_help=False)

    parser_cdcbench.add_argument("-t", "--table", action="store", metavar="<Table name>",
                                 type=convert_sample_table_alias,
                                 help="Specifies table.\n"
                                      "Allowed alias: S (STRING_TEST) / N (NUMERIC_TEST) / D (DATETIME_TEST) / \n"
                                      "B (BINARY_TEST) / L (LOB_TEST) / O (ORACLE_TEST) / Q (SQLSERVER_TEST)")

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

    command_insert.add_argument("-u", "--user", type=check_positive_integer_arg, default=1,
                                help="")

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

        logger.info(f"cdcbench start at {datetime.fromtimestamp(main_start_time)}")

        if args.database:
            if args.database not in (d.upper() for d in config.databases.keys()):
                print_error(f"[ {args.database} ] is DB key name that does not exist.")
        else:
            args.database = list(config.databases.keys())[0]

        command_default_table = {"i": INSERT_TEST, "u": UPDATE_TEST, "d": DELETE_TEST}
        if args.table is None:
            args.table = command_default_table[args.command[0]]

        print_description_msg(INSERT, args.table, args.verbose)

        tool_box = DBWorkToolBox()
        tool_box.conn_info = config.databases[args.database]

        logger.debug("Get DBMS declarative base")
        decl_base = SADeclarativeManager(tool_box.conn_info, [args.table]).get_dbms_base()

        logger.debug("inspect table...")
        tool_box.tables = {table_name: inspect_table(decl_base.metadata, table_name) for table_name in [args.table]}

        logger.debug("inspect table columns...")
        tool_box.table_columns = {table_name: inspect_columns(tool_box.tables[table_name])
                                  for table_name in tool_box.tables}

        logger.debug("Make table data")
        if args.command[0] in ["i", "u"]:
            tool_box.data_managers = {table_name: DataManager(table_name, args.custom_data)
                                      for table_name in tool_box.tables}

        logger.info("Execute child process")
        with ProcessPoolExecutor(max_workers=args.user) as executor:
            futures = {i+1: executor.submit(args.func, args, config, log_mgr.queue, i+1, tool_box)
                       for i in range(args.user)}
            future_results = {proc_seq: futures[proc_seq].result(timeout=5) for proc_seq in futures}

        print_end_msg(COMMIT if not args.rollback else ROLLBACK, args.verbose, end="\n")

        print(get_total_result_msg(future_results))
        result_log = f"\n{get_total_result_msg(future_results)}"
        if args.user > 1:
            print(get_detail_result_msg(future_results))
            result_log += f"\n{get_detail_result_msg(future_results)}"
        logger.info(result_log)

        logger.info("cdcbench end")

        # cdcbench end 로그가 리스너 terminate로 인해 로깅되지 않는 것으로 보임
        log_mgr.listener.terminate()

        print(f"  Main {get_elapsed_time_msg(end_time=time.time(), start_time=main_start_time)}")

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


def setup_child_process(config: ConfigModel, log_queue: queue.Queue, proc_id: int) -> NoReturn:

    multiprocessing.current_process().name = f"User{proc_id}"

    configure_logger(log_queue, config.settings.log_level, config.settings.sql_logging)


def insert(args: argparse.Namespace, config: ConfigModel, log_queue: queue.Queue, proc_seq: int,
           tool_box: DBWorkToolBox) -> NoReturn:

    setup_child_process(config, log_queue, proc_seq)

    dml = DML(args, config, tool_box)

    if args.single:
        dml.single_insert(args.table)
    else:
        dml.multi_insert(args.table)

    dml.conn.close()

    return dml.summary


def update(args: argparse.Namespace, config: ConfigModel, log_queue: queue.Queue, proc_id: int,
           tool_box: DBWorkToolBox) -> ResultSummary:

    setup_child_process(config, log_queue, proc_id)

    if args.table == UPDATE_TEST:
        args.columns = ["COL_NAME"]

    if args.start_id and args.end_id is None:
        args.end_id = args.start_id

    dml = DML(args, config, tool_box)

    if args.start_id is None and args.end_id is None:
        dml.where_update(args.table)
    else:
        dml.sequential_update(args.table)

    dml.conn.close()

    return dml.summary


def delete(args: argparse.Namespace, config: ConfigModel, log_queue: queue.Queue, proc_id: int,
           tool_box: DBWorkToolBox) -> ResultSummary:

    setup_child_process(config, log_queue, proc_id)

    if args.start_id and args.end_id is None:
        args.end_id = args.start_id

    dml = DML(args, config, tool_box)

    if args.start_id is None and args.end_id is None:
        dml.where_delete(args.table)
    else:
        dml.sequential_delete(args.table)

    dml.conn.close()

    return dml.summary


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()
