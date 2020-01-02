from src.constants import tqdm_bar_format, tqdm_ncols, INSERT_TEST
from src.funcs_common import get_commit_msg, get_rollback_msg, get_elapsed_time_msg, get_object_name, \
                             get_start_time_msg, print_complete_msg, print_description_msg, print_error_msg
from src.funcs_datagen import get_sample_table_data, get_file_data, data_file_name, get_separate_col_val
from src.mgr_logger import LoggerManager

from sqlalchemy import text, func
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.exc import DatabaseError
from datetime import datetime
from tqdm import tqdm

import time
import logging


class FuncsDml:

    def __init__(self, conn, mapper):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.engine = conn.engine
        self.connection = conn.engine.connect()
        self.db_session = conn.db_session
        self.dbms_type = conn.connection_info["dbms_type"]

        self.mapper = mapper

    def single_insert(self, table_name, columns, number_of_data, commit_unit, rollback, verbose):

        class Table(self.mapper):
            __table__ = self.mapper.metadata.tables[get_object_name(table_name, self.mapper.metadata.tables.keys())]
            column_names = list(column.name for column in __table__.c)

            def __init__(self, **kwargs):
                data = kwargs["data"]
                for column_name in data.keys():
                    setattr(self, self.column_names[self.column_names.index(column_name)], data[column_name])

        print(get_start_time_msg(datetime.now()))
        print_description_msg("INSERT", Table.__table__, verbose)
        self.logger.info(f"Start data insert in the \"{Table.__table__}\" Table")

        insert_info_msg = f"Insert Information: {{\"Table Name\" : {Table.__table__}, " \
                          f"\"Number of Data\": {number_of_data}, \"Commit Unit\": {commit_unit}}}"

        self.logger.info(insert_info_msg)

        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

        all_column_names = Table.column_names[:]
        selected_column_names = get_inspected_column_names(columns, all_column_names)

        start_time = time.time()

        try:
            if table_name == INSERT_TEST:
                separate_col_val = get_separate_col_val(self.engine, Table.__table__, all_column_names[3])
            else:
                separate_col_val = None

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                row_data = get_sample_table_data(file_data, table_name, selected_column_names, separate_col_val)

                self.db_session.add(Table(data=row_data))

                if i % commit_unit == 0:
                    if rollback:
                        self.db_session.rollback()
                        self.logger.debug(get_rollback_msg(separate_col_val))
                    else:
                        self.db_session.commit()
                        self.logger.debug(get_commit_msg(separate_col_val))
                    separate_col_val += 1

            if number_of_data % commit_unit != 0:
                if rollback:
                    self.db_session.rollback()
                    self.logger.debug(get_rollback_msg(separate_col_val))
                else:
                    self.db_session.commit()
                    self.logger.debug(get_commit_msg(separate_col_val))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

        end_time = time.time()

        print_complete_msg(verbose, separate=False)

        elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
        print(f"  {elapse_time_msg}\n")
        self.logger.info(elapse_time_msg)

        self.logger.info(f"End data insert in the \"{Table.__table__}\" Table")

    def _tx_end(self, tx, rollback, end_count):
        if rollback is True:
            tx.rollback()
            self.logger.debug(get_rollback_msg(end_count))
        else:
            tx.commit()
            self.logger.debug(get_commit_msg(end_count))

    def multi_insert(self, table_name, columns, number_of_data, commit_unit, rollback, verbose):

        table = self.mapper.metadata.tables[get_object_name(table_name, self.mapper.metadata.tables.keys())]

        insert_info_msg = f"Insert Information: {{\"Table Name\" : {table}, \"Number of Data\": {number_of_data}, " \
                          f"\"Commit Unit\": {commit_unit}}}"

        self.logger.info(insert_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("INSERT", table, verbose)
        self.logger.info(f"Start data insert in the \"{table}\" Table")

        all_column_names = table.columns.keys()[:]  # table column name 획득
        selected_column_names = get_inspected_column_names(columns, all_column_names)

        list_of_row_data = []
        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])
        end_count = 1

        start_time = time.time()

        try:

            # INSERT_TEST 테이블 separate_col_val 처리
            if table_name == INSERT_TEST:
                separate_col_val = get_separate_col_val(self.engine, table, all_column_names[3])
            else:
                separate_col_val = None

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                list_of_row_data.append(
                    get_sample_table_data(file_data, table_name, selected_column_names, separate_col_val,
                                          dbms_type=self.dbms_type)
                )

                if i % commit_unit == 0:
                    with self.connection.begin() as tx:
                        self.connection.execute(table.insert(), list_of_row_data)
                        self._tx_end(tx, rollback, end_count)

                    end_count += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:
                with self.connection.begin() as tx:
                    self.connection.execute(table.insert(), list_of_row_data)
                    self._tx_end(tx, rollback, end_count)

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

        e_time = time.time()

        print_complete_msg(verbose, separate=False)

        elapse_time_msg = get_elapsed_time_msg(e_time, start_time)
        print(f"  {elapse_time_msg}\n")
        self.logger.info(elapse_time_msg)

        self.logger.info(f"End data insert in the \"{table}\" Table")

    def update(self, table_name, updated_columns, where_clause, rollback, verbose, nowhere=False):

        table = self.mapper.metadata.tables[get_object_name(table_name, self.mapper.metadata.tables.keys())]

        update_info_msg = f"Update Information: {{\"Table Name\" : {table}, " \
                          f"\"Updated Columns\": {updated_columns}, " \
                          f"\"Where Clause\": {where_clause} }}"

        self.logger.info(update_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("UPDAT", table, verbose)
        self.logger.info(f"Start data update in the \"{table}\" Table")

        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

        all_column_names = table.columns.keys()[:]  # table column name 획득
        selected_column_names = get_inspected_column_names(updated_columns, all_column_names)
        end_count = 1

        try:

            if nowhere:
                update_stmt = table.update() \
                                   .values(dict((column_name, bindparam(column_name))
                                                for column_name in selected_column_names))
            else:
                update_stmt = table.update() \
                                   .values(dict((column_name, bindparam(column_name))
                                                for column_name in selected_column_names)) \
                                   .where(text(where_clause))

            start_time = time.time()

            for i in tqdm(range(1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                row_data = get_sample_table_data(file_data, table_name, selected_column_names, dbms_type=self.dbms_type)

                with self.connection.begin() as tx:
                    self.connection.execute(update_stmt, row_data)
                    self._tx_end(tx, rollback, end_count)

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print(f"  {elapse_time_msg}\n")
            self.logger.info(elapse_time_msg)

            self.logger.info(f"End data update in the \"{table}\" Table")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

    def separated_update(self, table_name, updated_columns, where_clause, separate_column,
                         rollback, verbose, commit_unit=None):

        table = self.mapper.metadata.tables[get_object_name(table_name, self.mapper.metadata.tables.keys())]

        update_info_msg = f"Update Information: {{\"Table Name\" : {table}, " \
                          f"\"Updated Columns\": {updated_columns}, " \
                          f"\"Where Clause\": {where_clause} }}" \

        self.logger.info(update_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("UPDAT", table, verbose)
        self.logger.info("Start data update in the \"{}\" Table".format(table))

        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

        all_column_names = table.columns.keys()[:]  # table column name 획득
        updated_column_names = get_inspected_column_names(updated_columns, all_column_names)
        where_column_name = get_inspected_column_names([separate_column], all_column_names)
        end_count = 1

        list_of_row_data = []

        try:

            where_column = table.columns[where_column_name[0]]
            select_where_clause = text(where_clause)

            # Update 대상 Row 조회
            update_rows = self.db_session.query(where_column.label(where_column.name))\
                                         .filter(select_where_clause)\
                                         .group_by(where_column.name)\
                                         .order_by(where_column.name)

            update_row_count = self.engine.execute(update_rows.statement.with_only_columns(
                                                    [func.count(where_column).label("ID_COUNT")])).scalar()

            update_where_clause = where_column == bindparam(f"b_{where_column.name}")
            update_stmt = table.update() \
                               .values(dict((column_name, bindparam(column_name))
                                            for column_name in updated_column_names)) \
                               .where(update_where_clause)

            start_time = time.time()

            for i in tqdm(update_rows, total=update_row_count, disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format):

                row_data = get_sample_table_data(file_data, table_name, updated_column_names, dbms_type=self.dbms_type)
                row_data["b_{}".format(where_column.name)] = i[0]

                list_of_row_data.append(row_data)

                if commit_unit is not None:
                    if len(list_of_row_data) % commit_unit == 0:
                        with self.connection.begin() as tx:
                            self.connection.execute(update_stmt, list_of_row_data)
                            self._tx_end(tx, rollback, end_count)

                        end_count += 1
                        list_of_row_data.clear()
                    else:
                        continue

                else:
                    with self.connection.begin() as tx:
                        self.connection.execute(update_stmt, list_of_row_data)
                        self._tx_end(tx, rollback, end_count)

                    end_count += 1
                    list_of_row_data.clear()

            # Commit 단위별로 처리된 후 남은 Row 마저 Commit
            if commit_unit is not None and update_row_count is not None and (update_row_count % commit_unit != 0):
                with self.connection.begin() as tx:
                    self.connection.execute(update_stmt, list_of_row_data)
                    self._tx_end(tx, rollback, end_count)

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

    def delete(self, table_name, where_clause, rollback, verbose, nowhere=False):

        table = self.mapper.metadata.tables[get_object_name(table_name, self.mapper.metadata.tables.keys())]

        delete_info_msg = f"Delete Information: {{\"Table Name\" : {table}, \"Where Clause\": {where_clause}}}"

        self.logger.info(delete_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("DELET", table, verbose)
        self.logger.info(f"Start data delete in the \"{table}\" Table")
        end_count = 1

        try:

            if nowhere:
                delete_stmt = table.delete()
            else:
                delete_stmt = table.delete().where(text(where_clause))

            start_time = time.time()

            for i in tqdm(range(1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                with self.connection.begin() as tx:
                    self.connection.execute(delete_stmt)
                    self._tx_end(tx, rollback, end_count)

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info(f"End data delete in the \"{table}\" Table")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

    def separated_delete(self, table_name, where_clause, separate_column, rollback, verbose, commit_unit=None):

        table = self.mapper.metadata.tables[get_object_name(table_name, self.mapper.metadata.tables.keys())]

        delete_info_msg = f"Delete Information: {{\"Table Name\" : {table}, \"Where Clause\": {where_clause}}}"

        self.logger.info(delete_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("DELET", table, verbose)
        self.logger.info(f"Start data delete in the \"{table}\" Table")

        all_column_names = table.columns.keys()[:]  # Table Column Name List 획득
        where_column_name = get_inspected_column_names([separate_column], all_column_names)
        end_count = 1

        list_of_row_data = []

        try:

            where_column = table.columns[where_column_name[0]]
            select_where_clause = text(where_clause)

            # Delete 대상 Row 조회
            delete_rows = self.db_session.query(where_column.label(where_column.name)) \
                                         .filter(select_where_clause) \
                                         .group_by(where_column.name) \
                                         .order_by(where_column.name)

            delete_row_count = self.engine.execute(delete_rows.statement.with_only_columns(
                                                    [func.count(where_column).label("ID_COUNT")])).scalar()

            delete_where_clause = where_column == bindparam(f"b_{where_column.name}")
            delete_stmt = table.delete().where(delete_where_clause)

            start_time = time.time()

            for i in tqdm(delete_rows, total=delete_row_count, disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format):

                list_of_row_data.append({f"b_{where_column.name}": i[0]})

                if commit_unit is not None:
                    if len(list_of_row_data) % commit_unit ==0:
                        with self.connection.begin() as tx:
                            self.connection.execute(delete_stmt, list_of_row_data)
                            self._tx_end(tx, rollback, end_count)
                        end_count += 1
                        list_of_row_data.clear()
                    else:
                        continue

                else:
                    with self.connection.begin() as tx:
                        self.connection.execute(delete_stmt, list_of_row_data)
                        self._tx_end(tx, rollback, end_count)
                    end_count += 1
                    list_of_row_data.clear()

            # Commit 단위별로 처리된 후 남은 Row 마저 Commit
            if commit_unit is not None and delete_row_count is not None and (delete_row_count % commit_unit != 0):
                with self.connection.begin() as tx:
                    self.connection.execute(delete_stmt, list_of_row_data)
                    self._tx_end(tx, rollback, end_count)

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info(f"End data delete in the \"{table}\" Table")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])


def get_inspected_column_names(columns, all_column_names):

    if columns is None:
        return all_column_names[1:]
    else:
        column_names = []
        if all(isinstance(item, int) for item in columns):
            for column_id in columns:
                if column_id <= 0:
                    print("... Fail")
                    print_error_msg(f"Invalid Column ID. [{column_id}]")
                try:
                    column_names.append(all_column_names[column_id-1])
                except IndexError:
                    print("... Fail")
                    print_error_msg(f"The column is a column that does not exist in the table. [{column_id}]")
        else:
            for column_name in columns:
                try:
                    column_names.append(get_object_name(column_name, all_column_names))
                except KeyError:
                    print("... Fail")
                    print_error_msg(f"The column is a column that does not exist in the table. [{column_name}]")

        return column_names
