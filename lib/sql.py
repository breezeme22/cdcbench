
import argparse
import logging
import time

from sqlalchemy.exc import DatabaseError
from sqlalchemy.future import Connection
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.sql.expression import func, bindparam, text, select
from typing import Any, Dict, List, NoReturn

from lib.common import print_error, proc_database_error, DMLDetail, ResultSummary, record_dml_summary, DBWorkToolBox
from lib.config import ConfigModel
from lib.connection import ConnectionManager
from lib.data import DataManager
from lib.globals import *


def _inspect_table(metadata: MetaData, table_name: str) -> Table:
    try:
        return metadata.tables[table_name]
    except KeyError:
        print_error(f"[ {table_name} ] table does not exist.")


def _inspect_columns(table: Table, selected_column_items: List[str or int] = None) -> List[Column]:

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
                    print_error(f"The column [ {column_item} ] is a column that does not exist in the table.")
        return selected_columns


def execute_tcl(conn: Connection, rollback: bool, summary: ResultSummary) -> NoReturn:
    if rollback:
        conn.rollback()
        summary.tcl.rollback += 1
    else:
        conn.commit()
        summary.tcl.commit += 1


class DML:

    def __init__(self, tool_box: DBWorkToolBox):

        self.logger = logging.getLogger(CDCBENCH)
        self.args = tool_box.args
        self.config = tool_box.config

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

        self.data_mgr: Dict[str, DataManager] = tool_box.data_managers

        self.summary = ResultSummary()

    def _get_row_data(self, table_name: str) -> Dict:

        if self.args.custom_data:
            return self.data_mgr[table_name].column_name_based_get_data(self.table_columns[table_name], self.dbms)
        else:
            return self.data_mgr[table_name].data_type_based_get_data(self.table_columns[table_name], self.dbms)

    def _get_list_row_data(self, table_name: str, record_count: int) -> List:

        if self.args.custom_data:
            return [self.data_mgr[table_name].column_name_based_get_data(self.table_columns[table_name], self.dbms)
                    for _ in range(record_count)]
        else:
            return [self.data_mgr[table_name].data_type_based_get_data(self.table_columns[table_name], self.dbms)
                    for _ in range(record_count)]

    def _execute_multi_dml(self, dml: str, stmt: Any, list_row_data: List[Dict], summary: ResultSummary,
                           table: Table) -> NoReturn:
        result = self.conn.execute(stmt, list_row_data)
        record_dml_summary(summary, table.name, dml, result.rowcount)

    def get_table_count(self, table: Table) -> int:
        return self.conn.execute(select(func.count()).select_from(table)).scalar()

    def single_insert(self, table_name: str) -> NoReturn:

        self.logger.info(f"Single Insert to {table_name} ( record: {self.args.record}, "
                         f"commit: {self.args.commit} )")

        self.summary.dml.detail[table_name] = DMLDetail()

        try:

            self.summary.execution_info.start_time = time.time()

            for i in range(1, self.args.record + 1):

                result = self.conn.execute(self.tables[table_name].insert(), self._get_row_data(table_name))
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

        self.summary.dml.detail[table_name] = DMLDetail()

        list_row_data = []

        try:

            self.summary.execution_info.start_time = time.time()

            for i in range(1, self.args.record + 1):

                list_row_data.append(self._get_row_data(table_name))

                if i % self.args.commit == 0:
                    self._execute_multi_dml(INSERT, self.tables[table_name].insert(), list_row_data, self.summary,
                                            self.tables[table_name])
                    execute_tcl(self.conn, self.args.rollback, self.summary)
                    list_row_data.clear()
                elif len(list_row_data) >= self.config.settings.dml_array_size:
                    self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                    self._execute_multi_dml(INSERT, self.tables[table_name].insert(), list_row_data, self.summary,
                                            self.tables[table_name])
                    list_row_data.clear()

            if self.args.record % self.args.commit != 0:
                self._execute_multi_dml(INSERT, self.tables[table_name].insert(), list_row_data, self.summary,
                                        self.tables[table_name])
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def where_update(self, table_name: str) -> NoReturn:

        self.logger.info(f"Where Update to {table_name} ( where: {self.args.where} )")

        self.summary.dml.detail[table_name] = DMLDetail()

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

            result = self.conn.execute(update_stmt, self._get_row_data(table_name))
            record_dml_summary(self.summary, table_name, UPDATE, result.rowcount)
            execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def sequential_update(self, table_name: str) -> NoReturn:

        self.logger.info(f"Sequential Update to {table_name} ( start id: {self.args.start_id}, "
                         f"end id: {self.args.end_id}, commit: {self.args.commit})")

        self.summary.dml.detail[table_name] = DMLDetail()

        list_row_data = []

        where_column = _inspect_columns(self.tables[table_name],
                                        [self.tables[table_name].custom_attrs.identifier_column])[0]

        update_stmt = (self.tables[table_name]
                           .update()
                           .values({column.name: bindparam(column.name) for column in self.table_columns[table_name]})
                           .where(where_column == bindparam(f"v_{where_column.name}")))

        try:

            self.summary.execution_info.start_time = time.time()

            for row_id in range(self.args.start_id, self.args.end_id+1):

                row_data = self._get_row_data(table_name)
                row_data[f"v_{where_column.name}"] = row_id

                list_row_data.append(row_data)

                if len(list_row_data) % self.args.commit == 0:
                    self._execute_multi_dml(UPDATE, update_stmt, list_row_data, self.summary, self.tables[table_name])
                    execute_tcl(self.conn, self.args.rollback, self.summary)
                    list_row_data.clear()
                elif len(list_row_data) >= self.config.settings.dml_array_size:
                    self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                    self._execute_multi_dml(UPDATE, update_stmt, list_row_data, self.summary, self.tables[table_name])
                    list_row_data.clear()

            if len(list_row_data) % self.args.commit != 0:
                self._execute_multi_dml(UPDATE, update_stmt, list_row_data, self.summary, self.tables[table_name])
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def where_delete(self, table_name: str) -> NoReturn:

        self.logger.info(f"Where Delete to {table_name} ( where: {self.args.where} )")

        self.summary.dml.detail[table_name] = DMLDetail()

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

        self.summary.dml.detail[table_name] = DMLDetail()

        list_row_data = []

        where_column = _inspect_columns(self.tables[table_name],
                                        [self.tables[table_name].custom_attrs.identifier_column])[0]

        delete_stmt = self.tables[table_name].delete().where(where_column == bindparam(f"v_{where_column.name}"))

        try:

            self.summary.execution_info.start_time = time.time()

            for row_id in range(self.args.start_id, self.args.end_id+1):

                list_row_data.append({f"v_{where_column.name}": row_id})

                if len(list_row_data) % self.args.commit == 0:
                    self._execute_multi_dml(DELETE, delete_stmt, list_row_data, self.summary, self.tables[table_name])
                    execute_tcl(self.conn, self.args.rollback, self.summary)
                    list_row_data.clear()
                elif len(list_row_data) >= self.config.settings.dml_array_size:
                    self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                    self._execute_multi_dml(DELETE, delete_stmt, list_row_data, self.summary, self.tables[table_name])
                    list_row_data.clear()

            if len(list_row_data) % self.args.commit != 0:
                self._execute_multi_dml(DELETE, delete_stmt, list_row_data, self.summary, self.tables[table_name])
                execute_tcl(self.conn, self.args.rollback, self.summary)

            self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def execute_random_dml(self, random_table: Table, random_record: int, random_dml: str) -> NoReturn:

        self.logger.info(f"random_dml: {random_dml}, random_table: {random_table.name}, "
                         f"random_record: {random_record}")

        try:
            if random_dml == INSERT:
                random_data = self._get_list_row_data(random_table.name, random_record)
                self._execute_multi_dml(INSERT, random_table.insert(), random_data, self.summary, random_table)

            elif random_dml == UPDATE:

                random_data = self._get_list_row_data(random_table.name, random_record)

                where_column = _inspect_columns(random_table, [random_table.custom_attrs.identifier_column])[0]

                order_by_clause = _get_dbms_rand_function(self.dbms)
                target_row_ids_stmt = select(where_column).order_by(order_by_clause).limit(random_record)

                update_stmt = (random_table.update()
                               .values({column.name: bindparam(column.name)
                                        for column in self.table_columns[random_table.name]})
                               .where(where_column == bindparam(f"v_{where_column.name}")))

                update_target_row_ids = self.conn.execute(target_row_ids_stmt).all()

                for row_data, row in zip(random_data, update_target_row_ids):
                    row_data[f"v_{where_column.name}"] = row._mapping[where_column.name]

                self._execute_multi_dml(UPDATE, update_stmt, random_data, self.summary, random_table)

            else:   # DELETE

                where_column = _inspect_columns(random_table, [random_table.custom_attrs.identifier_column])[0]

                order_by_clause = _get_dbms_rand_function(self.dbms)
                target_row_ids_stmt = select(where_column).order_by(order_by_clause).limit(random_record)

                delete_stmt = random_table.delete().where(where_column == bindparam(f"v_{where_column.name}"))

                delete_target_row_ids = self.conn.execute(target_row_ids_stmt).all()
                random_data = [{f"v_{where_column.name}": row._mapping[where_column.name]}
                               for row in delete_target_row_ids]

                self._execute_multi_dml(DELETE, delete_stmt, random_data, self.summary, random_table)

        except DatabaseError as DE:
            proc_database_error(DE)


def _get_dbms_rand_function(dbms: str) -> Any:

    if dbms == ORACLE:
        return text("dbms_random.value")
    elif dbms == MYSQL:
        return func.rand()
    elif dbms == SQLSERVER:
        return func.newid()
    else:  # PostgreSQL
        return func.random()
