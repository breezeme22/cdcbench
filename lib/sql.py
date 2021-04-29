
import argparse
import random
import time

from sqlalchemy.exc import DatabaseError
from sqlalchemy.future import Connection
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.sql.expression import func, bindparam, text, select, and_
from tqdm import tqdm
from typing import Any, Dict, List, NoReturn

from lib.common import print_error, proc_database_error, DMLDetail, ResultSummary, record_dml_summary
from lib.config import ConfigModel
from lib.connection import ConnectionManager
from lib.data import DataManager
from lib.definition import SADeclarativeManager
from lib.globals import *
from lib.logger import LoggerManager


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


def _execute_multi_dml(dml: str, stmt: Any, conn: Connection, list_row_data: List[Dict], summary: ResultSummary,
                       table: Table) -> NoReturn:
    result = conn.execute(stmt, list_row_data)
    record_dml_summary(summary, table.name, dml, result.rowcount)


def execute_tcl(conn: Connection, rollback: bool, summary: ResultSummary) -> NoReturn:
    if rollback:
        conn.rollback()
        summary.tcl.rollback += 1
    else:
        conn.commit()
        summary.tcl.commit += 1


class DML:

    def __init__(self, args: argparse.Namespace, config: ConfigModel):

        self.logger = LoggerManager.get_logger(__file__)
        self.args = args
        self.config = config

        conn_info = config.databases[args.database]
        self.engine = ConnectionManager(conn_info).engine
        decl_base = SADeclarativeManager(conn_info, [args.table]).get_dbms_base()
        self.dbms = conn_info.dbms

        self.table = _inspect_table(decl_base.metadata, args.table)
        self.selected_columns = _inspect_columns(self.table, args.columns)

        self.data_mgr = DataManager(args.table, args.custom_data)

        self.summary = ResultSummary()
        self.summary.dml.detail[self.table.name] = DMLDetail()

    def get_row_data(self) -> Dict:

        if self.args.custom_data:
            return self.data_mgr.column_name_based_get_data(self.selected_columns, self.dbms)
        else:
            return self.data_mgr.data_type_based_get_data(self.selected_columns, self.dbms)

    def single_insert(self) -> NoReturn:

        self.engine.dispose()

        self.logger.info(f"Single Insert to {self.table.name} ( record: {self.args.record}, "
                         f"commit: {self.args.commit} )")

        try:
            with self.engine.connect() as conn:

                self.summary.execution_info.start_time = time.time()

                for i in tqdm(range(1, self.args.record + 1), disable=self.args.verbose, ncols=tqdm_ncols,
                              bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(self.args.rollback)):

                    result = conn.execute(self.table.insert(), self.get_row_data())
                    record_dml_summary(self.summary, self.table.name, INSERT, result.rowcount)

                    if i % self.args.commit == 0:
                        execute_tcl(conn, self.args.rollback, self.summary)

                if self.args.record % self.args.commit != 0:
                    execute_tcl(conn, self.args.rollback, self.summary)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def multi_insert(self) -> NoReturn:

        self.engine.dispose()

        self.logger.info(f"Multi Insert to {self.table.name} ( record: {self.args.record}, "
                         f"commit: {self.args.commit} )")

        list_row_data = []

        try:
            with self.engine.connect() as conn:

                self.summary.execution_info.start_time = time.time()

                for i in tqdm(range(1, self.args.record+1), disable=self.args.verbose, ncols=tqdm_ncols,
                              bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(self.args.rollback)):

                    list_row_data.append(self.get_row_data())

                    if i % self.args.commit == 0:
                        _execute_multi_dml(INSERT, self.table.insert(), conn, list_row_data, self.summary, self.table)
                        execute_tcl(conn, self.args.rollback, self.summary)
                        list_row_data.clear()
                    elif len(list_row_data) >= self.config.settings.dml_array_size:
                        self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                        _execute_multi_dml(INSERT, self.table.insert(), conn, list_row_data, self.summary, self.table)
                        list_row_data.clear()

                if self.args.record % self.args.commit != 0:
                    _execute_multi_dml(INSERT, self.table.insert(), conn, list_row_data, self.summary, self.table)
                    execute_tcl(conn, self.args.rollback, self.summary)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def where_update(self) -> NoReturn:

        self.engine.dispose()

        self.logger.info(f"Where Update to {self.table.name} ( where: {self.args.where} )")

        nowhere = True if self.args.where is None else False

        if nowhere:
            update_stmt = (self.table.update()
                                     .values({column.name: bindparam(column.name) for column in self.selected_columns}))
        else:
            update_stmt = (self.table.update()
                                     .values({column.name: bindparam(column.name) for column in self.selected_columns})
                                     .where(text(self.args.where)))

        try:
            with self.engine.connect() as conn:

                self.summary.execution_info.start_time = time.time()

                for _ in tqdm(range(1), disable=self.args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                              postfix=tqdm_bench_postfix(self.args.rollback)):

                    result = conn.execute(update_stmt, self.get_row_data())
                    record_dml_summary(self.summary, self.table.name, UPDATE, result.rowcount)
                    execute_tcl(conn, self.args.rollback, self.summary)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def sequential_update(self) -> NoReturn:

        self.engine.dispose()

        self.logger.info(f"Sequential Update to {self.table.name} ( start id: {self.args.start_id}, "
                         f"end id: {self.args.end_id}, commit: {self.args.commit})")

        list_row_data = []

        where_column = _inspect_columns(self.table, [self.table.custom_attrs.identifier_column])[0]

        target_row_ids_stmt = (select(where_column)
                               .where(and_(self.args.start_id <= where_column,
                                           where_column <= self.args.end_id))
                               .order_by(where_column.name))

        target_row_count_stmt = (select(func.count(where_column))
                                 .where(and_(self.args.start_id <= where_column,
                                             where_column <= self.args.end_id)))

        update_stmt = (self.table.update()
                                 .values({column.name: bindparam(column.name) for column in self.selected_columns})
                                 .where(where_column == bindparam(f"v_{where_column.name}")))

        try:
            with self.engine.connect() as conn:

                # TODO. target_row 받아오는 부분 메모리 사용량 테스트해서 필요한 경우 yield_per 함수 사용하도록 변경
                update_target_row_ids = conn.execute(target_row_ids_stmt).all()
                update_target_row_count = conn.execute(target_row_count_stmt).scalar()

                self.summary.execution_info.start_time = time.time()

                for row in tqdm(update_target_row_ids, total=update_target_row_count, disable=self.args.verbose,
                                ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                postfix=tqdm_bench_postfix(self.args.rollback)):

                    row_data = self.get_row_data()
                    row_data[f"v_{where_column.name}"] = row._mapping[where_column.name]

                    list_row_data.append(row_data)

                    if len(list_row_data) % self.args.commit == 0:
                        _execute_multi_dml(UPDATE, update_stmt, conn, list_row_data, self.summary, self.table)
                        execute_tcl(conn, self.args.rollback, self.summary)
                        list_row_data.clear()
                    elif len(list_row_data) >= self.config.settings.dml_array_size:
                        self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                        _execute_multi_dml(UPDATE, update_stmt, conn, list_row_data, self.summary, self.table)
                        list_row_data.clear()

                if len(list_row_data) % self.args.commit != 0:
                    _execute_multi_dml(UPDATE, update_stmt, conn, list_row_data, self.summary, self.table)
                    execute_tcl(conn, self.args.rollback, self.summary)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def where_delete(self) -> NoReturn:

        self.engine.dispose()

        self.logger.info(f"Where Delete to {self.table.name} ( where: {self.args.where} )")

        nowhere = True if self.args.where is None else False

        if nowhere:
            delete_stmt = self.table.delete()
        else:
            delete_stmt = self.table.delete().where(text(self.args.where))

        try:
            with self.engine.connect() as conn:

                self.summary.execution_info.start_time = time.time()

                for _ in tqdm(range(1), disable=self.args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                              postfix=tqdm_bench_postfix(self.args.rollback)):
                    result = conn.execute(delete_stmt, self.get_row_data())
                    record_dml_summary(self.summary, self.table.name, DELETE, result.rowcount)
                    execute_tcl(conn, self.args.rollback, self.summary)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)

    def sequential_delete(self) -> NoReturn:

        self.engine.dispose()

        self.logger.info(f"Sequential Delete to {self.table.name} ( start id: {self.args.start_id}, "
                         f"end id: {self.args.end_id}, commit: {self.args.commit})")

        list_row_data = []

        where_column = _inspect_columns(self.table, [self.table.custom_attrs.identifier_column])[0]

        target_row_ids_stmt = (select(where_column)
                               .where(and_(self.args.start_id <= where_column, where_column <= self.args.end_id))
                               .order_by(where_column.name))

        target_row_count_stmt = (select(func.count(where_column))
                                 .where(and_(self.args.start_id <= where_column,
                                             where_column <= self.args.end_id)))

        delete_stmt = self.table.delete().where(where_column == bindparam(f"v_{where_column.name}"))

        try:
            with self.engine.connect() as conn:

                delete_target_row_ids = conn.execute(target_row_ids_stmt).all()
                delete_target_row_count = conn.execute(target_row_count_stmt).scalar()

                self.summary.execution_info.start_time = time.time()

                for row in tqdm(delete_target_row_ids, total=delete_target_row_count, disable=self.args.verbose,
                                ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                postfix=tqdm_bench_postfix(self.args.rollback)):

                    list_row_data.append({f"v_{where_column.name}": row._mapping[where_column.name]})

                    if len(list_row_data) % self.args.commit == 0:
                        _execute_multi_dml(DELETE, delete_stmt, conn, list_row_data, self.summary, self.table)
                        execute_tcl(conn, self.args.rollback, self.summary)
                        list_row_data.clear()
                    elif len(list_row_data) >= self.config.settings.dml_array_size:
                        self.logger.debug("Data list exceeded the DML_ARRAY_SIZE value.")
                        _execute_multi_dml(DELETE, delete_stmt, conn, list_row_data, self.summary, self.table)
                        list_row_data.clear()

                if len(list_row_data) % self.args.commit != 0:
                    _execute_multi_dml(DELETE, delete_stmt, conn, list_row_data, self.summary, self.table)
                    execute_tcl(conn, self.args.rollback, self.summary)

                self.summary.execution_info.end_time = time.time()

        except DatabaseError as DE:
            proc_database_error(DE)


class RandomDML:

    def __init__(self, args: argparse.Namespace, config: ConfigModel):

        self.logger = LoggerManager.get_logger(__name__)
        self.args = args
        self.config = config

        conn_info = config.databases[args.database]
        self.engine = ConnectionManager(conn_info).engine
        try:
            self.conn = self.engine.connect()
        except DatabaseError as DE:
            proc_database_error(DE)
        decl_base = SADeclarativeManager(conn_info, args.tables).get_dbms_base()
        self.dbms = conn_info.dbms

        self.tables = [_inspect_table(decl_base.metadata, table) for table in args.tables]
        self.table_columns = {table.name: _inspect_columns(table) for table in self.tables}

        self.data_mgr: Dict[str, DataManager] = {table.name: DataManager(table.name, args.custom_data)
                                                 for table in self.tables}

        self.summary = ResultSummary()

    def _get_list_row_data(self, table_name: str, random_record: int) -> List:

        if self.args.custom_data:
            return [self.data_mgr[table_name].column_name_based_get_data(self.table_columns[table_name], self.dbms)
                    for _ in range(random_record)]
        else:
            return [self.data_mgr[table_name].data_type_based_get_data(self.table_columns[table_name], self.dbms)
                    for _ in range(random_record)]

    def get_table_count(self, table: Table) -> int:
        return self.conn.execute(select(func.count()).select_from(table)).scalar()

    def execute_random_dml(self, random_table: Table, random_record: int, random_dml: str) -> NoReturn:

        self.logger.info(f"random_dml: {random_dml}, random_table: {random_table.name}, "
                         f"random_record: {random_record}")

        try:
            if random_dml == INSERT:
                random_data = self._get_list_row_data(random_table.name, random_record)
                _execute_multi_dml(INSERT, random_table.insert(), self.conn, random_data, self.summary, random_table)

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

                _execute_multi_dml(UPDATE, update_stmt, self.conn, random_data, self.summary, random_table)

            else:   # DELETE

                where_column = _inspect_columns(random_table, [random_table.custom_attrs.identifier_column])[0]

                order_by_clause = _get_dbms_rand_function(self.dbms)
                target_row_ids_stmt = select(where_column).order_by(order_by_clause).limit(random_record)

                delete_stmt = random_table.delete().where(where_column == bindparam(f"v_{where_column.name}"))

                delete_target_row_ids = self.conn.execute(target_row_ids_stmt).all()
                random_data = [{f"v_{where_column.name}": row._mapping[where_column.name]}
                               for row in delete_target_row_ids]

                _execute_multi_dml(DELETE, delete_stmt, self.conn, random_data, self.summary, random_table)

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
