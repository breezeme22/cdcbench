
import argparse
import logging
import time

from sqlalchemy.exc import DatabaseError
from sqlalchemy.future import Connection
from sqlalchemy.schema import Table, Column
from sqlalchemy.sql.expression import func, bindparam, text, select
from typing import Any, Dict, List, NoReturn

from lib.common import (proc_database_error, ResultSummary, record_dml_summary, DBWorkToolBox, inspect_columns)
from lib.config import ConfigModel
from lib.connection import ConnectionManager
from lib.data import DataManager
from lib.globals import *


class DML:

    def __init__(self, args: argparse.Namespace, config: ConfigModel, tool_box: DBWorkToolBox):

        self.logger = logging.getLogger(CDCBENCH)
        self.args = args
        self.config = config

        # Unix에서 지원되는 fork 방식의 경우 dispose 활용하여 Connection object를 공유할 수 있을지 모르겠으나,
        # Windows의 경우 dispose 함수 호출하여도 pickling 실패 (메뉴얼 상에도 os.fork()로 명시되어 있어, spawn 방식은 미지원하는듯..?)
        self.engine = ConnectionManager(tool_box.conn_info).engine
        try:
            self.conn = self.engine.connect()
        except DatabaseError as DE:
            proc_database_error(DE)

        self.dbms = tool_box.conn_info.dbms
        self.tables: Dict[str, Table] = tool_box.tables
        self.table_columns: Dict[str, List[Column]] = tool_box.table_columns

        self.table_data: Dict[str, DataManager] = tool_box.table_data

        self.summary = ResultSummary()

    def execute_multi_dml(self, dml: str, stmt: Any, list_row_data: List[Dict], table_name: str) -> NoReturn:
        result = self.conn.execute(stmt, list_row_data)
        record_dml_summary(self.summary, table_name, dml, result.rowcount)

    def get_table_count(self, table: Table) -> int:
        return self.conn.execute(select(func.count()).select_from(table)).scalar()

    def single_insert(self, table_name: str) -> NoReturn:

        self.logger.info(f"Single Insert to {table_name} ( record: {self.args.record}, "
                         f"commit: {self.args.commit} )")

        try:

            self.summary.execution_info.start_time = time.time()

            for i in range(1, self.args.record + 1):

                result = self.conn.execute(self.tables[table_name].insert(),
                                           self.table_data[table_name].get_row_data(self.table_columns[table_name],
                                                                                    self.dbms))
                record_dml_summary(self.summary, table_name, INSERT, result.rowcount)

                if i % self.args.commit == 0:
                    execute_tcl(self.conn, self.args.rollback, self.summary)

            if self.args.record % self.args.commit != 0:
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def multi_insert(self, table_name: str) -> NoReturn:

        self.logger.info(f"Multi Insert to {table_name} ( record: {self.args.record}, "
                         f"commit: {self.args.commit} )")

        list_row_data = []

        try:

            self.summary.execution_info.start_time = time.time()

            for i in range(1, self.args.record + 1):

                list_row_data.append(
                    self.table_data[table_name].get_row_data(self.table_columns[table_name], self.dbms))

                if i % self.args.commit == 0:
                    self.execute_multi_dml(INSERT, self.tables[table_name].insert(), list_row_data, table_name)
                    execute_tcl(self.conn, self.args.rollback, self.summary)
                    list_row_data.clear()
                elif len(list_row_data) >= self.config.settings.dml_array_size:
                    self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                    self.execute_multi_dml(INSERT, self.tables[table_name].insert(), list_row_data, table_name)
                    list_row_data.clear()

            if self.args.record % self.args.commit != 0:
                self.execute_multi_dml(INSERT, self.tables[table_name].insert(), list_row_data, table_name)
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def where_update(self, table_name: str) -> NoReturn:

        self.logger.info(f"Where Update to {table_name} ( where: {self.args.where} )")

        nowhere = True if self.args.where is None else False

        if nowhere:
            update_stmt = (self.tables[table_name]
                               .update()
                               .values({column.name: bindparam(column.name)
                                        for column in self.table_columns[table_name]}))
        else:
            update_stmt = (self.tables[table_name]
                               .update()
                               .values({column.name: bindparam(column.name)
                                        for column in self.table_columns[table_name]})
                               .where(text(self.args.where)))

        try:

            self.summary.execution_info.start_time = time.time()

            result = self.conn.execute(update_stmt,
                                       self.table_data[table_name].get_row_data(self.table_columns[table_name],
                                                                                self.dbms))

            record_dml_summary(self.summary, table_name, UPDATE, result.rowcount)
            execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def sequential_update(self, table_name: str) -> NoReturn:

        self.logger.info(f"Sequential Update to {table_name} ( start id: {self.args.start_id}, "
                         f"end id: {self.args.end_id}, commit: {self.args.commit})")

        list_row_data = []

        where_column = inspect_columns(self.tables[table_name],
                                        [self.tables[table_name].custom_attrs.identifier_column])[0]

        update_stmt = (self.tables[table_name]
                           .update()
                           .values({column.name: bindparam(column.name) for column in self.table_columns[table_name]})
                           .where(where_column == bindparam(f"v_{where_column.name}")))

        try:

            self.summary.execution_info.start_time = time.time()

            for row_id in range(self.args.start_id, self.args.end_id+1):

                row_data = self.table_data[table_name].get_row_data(self.table_columns[table_name], self.dbms)
                row_data[f"v_{where_column.name}"] = row_id

                list_row_data.append(row_data)

                if len(list_row_data) % self.args.commit == 0:
                    self.execute_multi_dml(UPDATE, update_stmt, list_row_data, table_name)
                    execute_tcl(self.conn, self.args.rollback, self.summary)
                    list_row_data.clear()
                elif len(list_row_data) >= self.config.settings.dml_array_size:
                    self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                    self.execute_multi_dml(UPDATE, update_stmt, list_row_data, table_name)
                    list_row_data.clear()

            if len(list_row_data) % self.args.commit != 0:
                self.execute_multi_dml(UPDATE, update_stmt, list_row_data, table_name)
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def where_delete(self, table_name: str) -> NoReturn:

        self.logger.info(f"Where Delete to {table_name} ( where: {self.args.where} )")

        nowhere = True if self.args.where is None else False

        if nowhere:
            delete_stmt = self.tables[table_name].delete()
        else:
            delete_stmt = self.tables[table_name].delete().where(text(self.args.where))

        try:

            self.summary.execution_info.start_time = time.time()

            result = self.conn.execute(delete_stmt)
            record_dml_summary(self.summary, table_name, DELETE, result.rowcount)
            execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def sequential_delete(self, table_name: str) -> NoReturn:

        self.logger.info(f"Sequential Delete to {table_name} ( start id: {self.args.start_id}, "
                         f"end id: {self.args.end_id}, commit: {self.args.commit})")

        list_row_data = []

        where_column = inspect_columns(self.tables[table_name],
                                       [self.tables[table_name].custom_attrs.identifier_column])[0]

        delete_stmt = self.tables[table_name].delete().where(where_column == bindparam(f"v_{where_column.name}"))

        try:

            self.summary.execution_info.start_time = time.time()

            for row_id in range(self.args.start_id, self.args.end_id+1):

                list_row_data.append({f"v_{where_column.name}": row_id})

                if len(list_row_data) % self.args.commit == 0:
                    self.execute_multi_dml(DELETE, delete_stmt, list_row_data, table_name)
                    execute_tcl(self.conn, self.args.rollback, self.summary)
                    list_row_data.clear()
                elif len(list_row_data) >= self.config.settings.dml_array_size:
                    self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                    self.execute_multi_dml(DELETE, delete_stmt, list_row_data, table_name)
                    list_row_data.clear()

            if len(list_row_data) % self.args.commit != 0:
                self.execute_multi_dml(DELETE, delete_stmt, list_row_data, table_name)
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def execute_random_dml(self, random_table: Table, random_record: int, random_dml: str) -> NoReturn:

        self.logger.info(f"random_dml: {random_dml}, random_table: {random_table.name}, "
                         f"random_record: {random_record}")

        try:
            if random_dml == INSERT:
                random_data = (self.table_data[random_table.name]
                                   .get_list_row_data(self.table_columns[random_table.name], self.dbms, random_record))
                self.execute_multi_dml(INSERT, random_table.insert(), random_data, random_table.name)

            elif random_dml == UPDATE:

                random_data = (self.table_data[random_table.name]
                                   .get_list_row_data(self.table_columns[random_table.name], self.dbms, random_record))

                where_column = inspect_columns(random_table, [random_table.custom_attrs.identifier_column])[0]

                order_by_clause = _get_dbms_rand_function(self.dbms)
                target_row_ids_stmt = select(where_column).order_by(order_by_clause).limit(random_record)

                update_stmt = (random_table.update()
                               .values({column.name: bindparam(column.name)
                                        for column in self.table_columns[random_table.name]})
                               .where(where_column == bindparam(f"v_{where_column.name}")))

                update_target_row_ids = self.conn.execute(target_row_ids_stmt).all()

                for row_data, row in zip(random_data, update_target_row_ids):
                    row_data[f"v_{where_column.name}"] = row._mapping[where_column.name]

                self.execute_multi_dml(UPDATE, update_stmt, random_data, random_table.name)

            else:   # DELETE

                where_column = inspect_columns(random_table, [random_table.custom_attrs.identifier_column])[0]

                order_by_clause = _get_dbms_rand_function(self.dbms)
                target_row_ids_stmt = select(where_column).order_by(order_by_clause).limit(random_record)

                delete_stmt = random_table.delete().where(where_column == bindparam(f"v_{where_column.name}"))

                delete_target_row_ids = self.conn.execute(target_row_ids_stmt).all()
                random_data = [{f"v_{where_column.name}": row._mapping[where_column.name]}
                               for row in delete_target_row_ids]

                self.execute_multi_dml(DELETE, delete_stmt, random_data, random_table.name)

        except DatabaseError as DE:
            proc_database_error(DE)


def execute_tcl(conn: Connection, rollback: bool, summary: ResultSummary) -> NoReturn:
    if rollback:
        conn.rollback()
        summary.tcl.rollback += 1
    else:
        conn.commit()
        summary.tcl.commit += 1


def _get_dbms_rand_function(dbms: str) -> Any:

    if dbms == ORACLE:
        return text("dbms_random.value")
    elif dbms == MYSQL:
        return func.rand()
    elif dbms == SQLSERVER:
        return func.newid()
    else:  # PostgreSQL
        return func.random()
