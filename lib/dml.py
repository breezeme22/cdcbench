
import argparse
import time

from sqlalchemy.engine import Connection, Result
from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.sql.expression import func, bindparam, text
from tqdm import tqdm
from typing import Any, TextIO, Dict, Union, Optional, List, NoReturn

from lib.common import (print_error, proc_database_error, get_elapsed_time_msg,
                        DMLDetail, ResultSummary, ExecutionInfo, record_dml_summary)
from lib.config import ConfigModel
from lib.connection import ConnectionManager
from lib.data import DataManager
from lib.definition import SADeclarativeManager
from lib.globals import tqdm_bar_format, tqdm_ncols, tqdm_bench_postfix, INSERT, UPDATE, DELETE
from lib.logger import LoggerManager


def inspect_table(metadata: MetaData, table_name: str) -> Table:
    try:
        return metadata.tables[table_name]
    except KeyError as KE:
        print_error(f"[ {table_name} ] table does not exist.")


def inspect_columns(table: Table, selected_column_items: List[str or int]) -> List[Column]:

    all_columns = table.columns
    all_column_names = all_columns.keys()

    if selected_column_items is None:
        return [column for column in all_columns if column.default is None]
    else:
        selected_columns = []
        for column_item in selected_column_items:
            if isinstance(column_item, int):
                try:
                    selected_columns.append(all_columns[all_column_names[column_item - 1]])
                except IndexError as IE:
                    print_error(f"The column is a column that does not exist in the table. [ {column_item} ]")
            else:
                try:
                    selected_columns.append(all_columns[column_item])
                except KeyError as KE:
                    print_error(f"The column is a column that does not exist in the table. [ {column_item} ]")
        return selected_columns


class DML:

    def __init__(self, args: argparse.Namespace, config: ConfigModel):

        self.logger = LoggerManager.get_logger(__file__)
        self.args = args
        self.config = config

        conn_info = config.databases[args.database]
        self.engine = ConnectionManager(conn_info).engine
        decl_base = SADeclarativeManager(conn_info, [args.table]).get_dbms_base()
        self.dbms = conn_info.dbms

        self.table = inspect_table(decl_base.metadata, args.table)
        self.selected_columns = inspect_columns(self.table, args.columns)

        self.data_mgr = DataManager(args.table, args.custom_data)

        self.summary = ResultSummary()
        self.summary.dml.detail[self.table.name] = DMLDetail()

    def execute_tcl(self, conn: Connection, rollback: bool) -> NoReturn:
        if rollback:
            conn.rollback()
            self.summary.tcl.rollback += 1
        else:
            conn.commit()
            self.summary.tcl.commit += 1

    def multi_insert(self) -> NoReturn:

        self.logger.info(f"Multi Insert to {self.table.name} ( Record: {self.args.record}, "
                         f"Commit: {self.args.commit} )")

        list_row_data = []

        connect_start_time = time.time()

        try:
            with self.engine.connect() as conn:

                self.logger.info(f"connect() {get_elapsed_time_msg(time.time(), connect_start_time)}")

                self.summary.execution_info.start_time = time.time()

                for i in tqdm(range(1, self.args.record+1), disable=self.args.verbose, ncols=tqdm_ncols,
                              bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(self.args.rollback)):

                    if self.args.custom_data:
                        row_data = self.data_mgr.column_name_based_get_data(self.selected_columns, self.dbms)
                    else:
                        row_data = self.data_mgr.data_type_based_get_data(self.selected_columns, self.dbms)

                    list_row_data.append(row_data)

                    if i % self.args.commit == 0:
                        result = conn.execute(self.table.insert(), list_row_data)
                        record_dml_summary(self.summary, self.table.name, INSERT, result.rowcount)
                        self.execute_tcl(conn, self.args.rollback)
                        list_row_data.clear()
                    elif len(list_row_data) >= self.config.settings.dml_array_size:
                        self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                        result = conn.execute(self.table.insert(), list_row_data)
                        record_dml_summary(self.summary, self.table.name, INSERT, result.rowcount)
                        list_row_data.clear()

                if self.args.record % self.args.commit != 0:
                    result = conn.execute(self.table.insert(), list_row_data)
                    record_dml_summary(self.summary, self.table.name, INSERT, result.rowcount)
                    self.execute_tcl(conn, self.args.rollback)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

