from src.constants import tqdm_bar_format, tqdm_ncols, tqdm_bench_postfix, INSERT_TEST
from src.funcs_common import get_commit_msg, get_rollback_msg, get_elapsed_time_msg, get_object_name, \
                             get_start_time_msg, print_complete_msg, print_description_msg, print_error_msg, \
                             exec_database_error
from src.funcs_datagen import get_sample_table_data, get_file_data, data_file_name, get_separate_col_val
from src.mgr_logger import LoggerManager

from sqlalchemy import text, func
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.expression import bindparam
from tqdm import tqdm

import time


class FuncsDml:

    def __init__(self, conn):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.engine = conn.engine
        self.connection = conn.engine.connect()
        self.db_session = conn.db_session
        self.dbms_type = conn.connection_info["dbms_type"]

    def single_insert(self, table, selected_column_names, number_of_data, commit_unit, file_data, rollback, verbose):

        start_time = time.time()

        try:
            if table.__table__.name == INSERT_TEST:
                separate_col_val = get_separate_col_val(self.engine, table.__table__, table.column_names[3])
            else:
                separate_col_val = None

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):

                row_data = get_sample_table_data(file_data, table.__table__.name, selected_column_names, separate_col_val)

                self.db_session.add(table(data=row_data))

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

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def _complete_tx(self, tx, rollback, end_count):
        if rollback is True:
            tx.rollback()
            self.logger.debug(get_rollback_msg(end_count))
        else:
            tx.commit()
            self.logger.debug(get_commit_msg(end_count))

    def multi_insert(self, table, selected_column_names, number_of_data, commit_unit, file_data, rollback, verbose):

        list_of_row_data = []
        end_count = 1

        start_time = time.time()

        try:

            # INSERT_TEST 테이블 separate_col_val 처리
            if table.name == INSERT_TEST:
                separate_col_val = get_separate_col_val(self.engine, table, table.columns.keys()[3])
            else:
                separate_col_val = None

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):

                list_of_row_data.append(
                    get_sample_table_data(file_data, table.name, selected_column_names, separate_col_val,
                                          dbms_type=self.dbms_type)
                )

                if i % commit_unit == 0:
                    with self.connection.begin() as tx:
                        self.connection.execute(table.insert(), list_of_row_data)
                        self._complete_tx(tx, rollback, end_count)

                    end_count += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:
                with self.connection.begin() as tx:
                    self.connection.execute(table.insert(), list_of_row_data)
                    self._complete_tx(tx, rollback, end_count)

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def update(self, table, selected_column_names, where_clause, file_data, rollback, verbose, nowhere=False):

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

            for i in tqdm(range(1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):

                row_data = get_sample_table_data(file_data, table.name, selected_column_names, dbms_type=self.dbms_type)

                with self.connection.begin() as tx:
                    self.connection.execute(update_stmt, row_data)
                    self._complete_tx(tx, rollback, end_count)

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def separated_update(self, table, selected_column_names, select_where, update_where_column_name, file_data,
                         rollback, verbose, commit_unit=None):

        end_count = 1
        list_of_row_data = []

        try:

            where_column = table.columns[update_where_column_name[0]]
            select_where_clause = text(select_where)

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
                                            for column_name in selected_column_names)) \
                               .where(update_where_clause)

            start_time = time.time()

            for i in tqdm(update_rows, total=update_row_count, disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(rollback)):

                row_data = get_sample_table_data(file_data, table.name, selected_column_names, dbms_type=self.dbms_type)
                row_data["b_{}".format(where_column.name)] = i[0]

                list_of_row_data.append(row_data)

                if commit_unit is not None:
                    if len(list_of_row_data) % commit_unit == 0:
                        with self.connection.begin() as tx:
                            self.connection.execute(update_stmt, list_of_row_data)
                            self._complete_tx(tx, rollback, end_count)

                        end_count += 1
                        list_of_row_data.clear()
                    else:
                        continue

                else:
                    with self.connection.begin() as tx:
                        self.connection.execute(update_stmt, list_of_row_data)
                        self._complete_tx(tx, rollback, end_count)

                    end_count += 1
                    list_of_row_data.clear()

            # Commit 단위별로 처리된 후 남은 Row 마저 Commit
            if commit_unit is not None and update_row_count is not None and (update_row_count % commit_unit != 0):
                with self.connection.begin() as tx:
                    self.connection.execute(update_stmt, list_of_row_data)
                    self._complete_tx(tx, rollback, end_count)

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def delete(self, table, where_clause, rollback, verbose, nowhere=False):

        end_count = 1

        try:

            if nowhere:
                delete_stmt = table.delete()
            else:
                delete_stmt = table.delete().where(text(where_clause))

            start_time = time.time()

            for i in tqdm(range(1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):

                with self.connection.begin() as tx:
                    self.connection.execute(delete_stmt)
                    self._complete_tx(tx, rollback, end_count)

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def separated_delete(self, table, where_clause, delete_where_column_name, rollback, verbose, commit_unit=None):

        end_count = 1
        list_of_row_data = []

        try:

            where_column = table.columns[delete_where_column_name[0]]
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
                          bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(rollback)):

                list_of_row_data.append({f"b_{where_column.name}": i[0]})

                if commit_unit is not None:
                    if len(list_of_row_data) % commit_unit ==0:
                        with self.connection.begin() as tx:
                            self.connection.execute(delete_stmt, list_of_row_data)
                            self._complete_tx(tx, rollback, end_count)
                        end_count += 1
                        list_of_row_data.clear()
                    else:
                        continue

                else:
                    with self.connection.begin() as tx:
                        self.connection.execute(delete_stmt, list_of_row_data)
                        self._complete_tx(tx, rollback, end_count)
                    end_count += 1
                    list_of_row_data.clear()

            # Commit 단위별로 처리된 후 남은 Row 마저 Commit
            if commit_unit is not None and delete_row_count is not None and (delete_row_count % commit_unit != 0):
                with self.connection.begin() as tx:
                    self.connection.execute(delete_stmt, list_of_row_data)
                    self._complete_tx(tx, rollback, end_count)

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)
