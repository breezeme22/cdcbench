from commons.constants import tqdm_bar_format, tqdm_ncols, tqdm_bench_postfix, INSERT_TEST, sample_tables
from commons.funcs_common import get_commit_msg, get_rollback_msg, exec_database_error, get_separate_col_val
from commons.mgr_logger import LoggerManager

from sqlalchemy import text, func
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.expression import bindparam
from tqdm import tqdm

import time


class FuncsDml:

    def __init__(self, conn):

        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.engine = conn.engine
        self.connection = conn.engine.connect()
        self.db_session = conn.db_session
        self.dbms_type = conn.conn_info["dbms_type"]

    def single_insert(self, table, selected_columns, number_of_data, commit_unit, data_maker, rollback, verbose):
        """
        Oracle의 Single Insert 수행
        :param table: Table Object
        :param selected_columns: 데이터를 insert할 column list
        :param number_of_data: insert할 데이터 수
        :param commit_unit: Commit 단위
        :param data_maker: DataMaker instance
        :param rollback: Rollback 수행 여부
        :param verbose: 작업 진행도 (Progress bar) 표시 여부
        :return: {작업 시작시간, 작업 종료시간}
        """

        start_time = time.time()

        try:
            table_name = table.__table__.name
            if table_name == INSERT_TEST:
                separate_col_val = get_separate_col_val(self.engine, table.__table__, table.column_names[3])
            else:
                separate_col_val = None

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):
                
                if table_name.upper() in sample_tables:
                    row_data = data_maker.get_sample_table_data(table_name, selected_columns, separate_col_val,
                                                                dbms_type=self.dbms_type)
                else:
                    row_data = data_maker.get_user_table_data(selected_columns, self.dbms_type)

                self.db_session.add(table(data=row_data))

                if i % commit_unit == 0:
                    if rollback:
                        self.db_session.rollback()
                        self.logger.debug(get_rollback_msg(separate_col_val))
                    else:
                        self.db_session.commit()
                        self.logger.debug(get_commit_msg(separate_col_val))
                    separate_col_val += 1

            # 총 데이터 수가 커밋 단위로 나누어 떨어지지 않은 경우
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

    def multi_insert(self, table, selected_columns, number_of_data, commit_unit, data_maker, rollback, verbose):
        """
        Oracle Multi Insert
        :param table: Table Object
        :param selected_columns: 데이터를 insert할 column list
        :param number_of_data: insert할 데이터 수
        :param commit_unit: Commit 단위
        :param data_maker: DataMaker instance
        :param rollback: Rollback 수행 여부
        :param verbose: 작업 진행도 (Progress bar) 표시 여부
        :return: {작업 시작시간, 작업 종료시간}
        """

        list_of_row_data = []
        end_count = 1

        start_time = time.time()

        try:

            # INSERT_TEST 테이블 separate_col_val 처리
            if table.name.upper() == INSERT_TEST:
                separate_col_val = get_separate_col_val(self.engine, table, table.columns.keys()[3])
            else:
                separate_col_val = None

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):

                if table.name.upper() in sample_tables:
                    row_data = data_maker.get_sample_table_data(table.name, selected_columns, separate_col_val,
                                                                dbms_type=self.dbms_type)
                else:
                    row_data = data_maker.get_user_table_data(selected_columns, self.dbms_type)

                list_of_row_data.append(row_data)

                if i % commit_unit == 0:
                    with self.connection.begin() as tx:
                        self.connection.execute(table.insert(), list_of_row_data)
                        self._complete_tx(tx, rollback, end_count)
                        separate_col_val += 1

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

    def update(self, table, selected_columns, where_clause, data_maker, rollback, verbose, nowhere=False):
        """
        where 조건에 따라 update를 수행함
        :param table: Update 대상 Table
        :param selected_columns: Update가 수행될 Column list
        :param where_clause: --where 인자
        :param data_maker: DataMaker instance
        :param rollback: rollback 수행 여부
        :param verbose: 작업 진행도 (progress bar) 표시 여부
        :param nowhere: where 조건 없이 dpdate 수행 여부
        :return: {작업 시작시간, 작업 종료시간}
        """

        end_count = 1

        try:

            if nowhere:
                update_stmt = table.update() \
                                   .values(dict((column.name, bindparam(column.name))
                                                for column in selected_columns))
            else:
                update_stmt = table.update() \
                                   .values(dict((column.name, bindparam(column.name))
                                                for column in selected_columns)) \
                                   .where(text(where_clause))

            start_time = time.time()

            for i in tqdm(range(1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                          postfix=tqdm_bench_postfix(rollback)):

                if table.name.upper() in sample_tables:
                    row_data = data_maker.get_sample_table_data(table.name, selected_columns, dbms_type=self.dbms_type)
                else:
                    row_data = data_maker.get_user_table_data(selected_columns, self.dbms_type)

                with self.connection.begin() as tx:
                    self.connection.execute(update_stmt, row_data)
                    self._complete_tx(tx, rollback, end_count)

            end_time = time.time()

            return {"start_time": start_time, "end_time": end_time}

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def separated_update(self, table, selected_columns, select_where, update_where_column, data_maker,
                         rollback, verbose, commit_unit=None):
        """
        where 조건에 따라 update를 update_where_column 기준으로 순차적으로 나누어 수행함
        :param table: Table
        :param selected_columns: Update가 수행될 Column List 
        :param select_where: update 대상 record 조회 조건
        :param update_where_column: update 기준 column
        :param data_maker: DataMaker instance
        :param rollback: rollback 수행 여부
        :param verbose: 작업 진행도 (progress bar) 표시 여부
        :param commit_unit: commit 단위
        :return: {작업 시작시간, 작업 종료시간}
        """

        end_count = 1
        list_of_row_data = []

        try:

            select_where_clause = text(select_where)

            # Update 대상 Row 조회
            update_row_count_query = self.db_session.query(update_where_column.label(update_where_column.name))\
                                                    .filter(select_where_clause)

            update_row_count = self.engine.execute(update_row_count_query.statement.with_only_columns(
                                                    [func.count(update_where_column).label("ID_COUNT")])).scalar()

            # Update 대상 Row 조회
            update_rows = self.db_session.query(update_where_column.label(update_where_column.name)) \
                                         .filter(select_where_clause) \
                                         .group_by(update_where_column.name) \
                                         .order_by(update_where_column.name)

            update_where_clause = update_where_column == bindparam(f"b_{update_where_column.name}")
            update_stmt = table.update() \
                               .values(dict((column.name, bindparam(column.name)) for column in selected_columns)) \
                               .where(update_where_clause)

            start_time = time.time()

            for i in tqdm(update_rows, total=update_row_count, disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(rollback)):

                if table.name.upper() in sample_tables:
                    row_data = data_maker.get_sample_table_data(table.name, selected_columns, dbms_type=self.dbms_type)
                else:
                    row_data = data_maker.get_user_table_data(selected_columns, self.dbms_type)

                row_data["b_{}".format(update_where_column.name)] = i[0]

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
        """
        where 조건에 따라 delete를 수행
        :param table: Table
        :param where_clause: --where 인자
        :param rollback: rollback 수행 여부
        :param verbose: 작업 진행도 (progress bar) 표시 여부
        :param nowhere: where 조건 없이 dpdate 수행 여부
        :return: {작업 시작시간, 작업 종료시간}
        """

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

    def separated_delete(self, table, where_clause, delete_where_column, rollback, verbose, commit_unit=None):
        """
        where 조건에 따라 delete를 delete_where_column 기준으로 순차적으로 나누어 수행함
        :param table: table
        :param where_clause: --where 인자
        :param delete_where_column: delete 기준 컬럼
        :param rollback: rollback 수행 여부
        :param verbose: 작업 진행도 (progress bar) 표시 여부
        :param commit_unit: commit 단위
        :return: {작업 시작시간, 작업 종료시간}
        """

        end_count = 1
        list_of_row_data = []

        try:

            select_where_clause = text(where_clause)

            delete_row_count_query = self.db_session.query(delete_where_column.label(delete_where_column.name)) \
                                                    .filter(select_where_clause)

            delete_row_count = self.engine.execute(delete_row_count_query.statement.with_only_columns(
                                                    [func.count(delete_where_column).label("ID_COUNT")])).scalar()

            # Delete 대상 Row 조회
            delete_rows = self.db_session.query(delete_where_column.label(delete_where_column.name)) \
                                         .filter(select_where_clause) \
                                         .group_by(delete_where_column.name) \
                                         .order_by(delete_where_column.name)

            delete_where_clause = delete_where_column == bindparam(f"b_{delete_where_column.name}")
            delete_stmt = table.delete().where(delete_where_clause)

            start_time = time.time()

            for i in tqdm(delete_rows, total=delete_row_count, disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format, postfix=tqdm_bench_postfix(rollback)):

                list_of_row_data.append({f"b_{delete_where_column.name}": i[0]})

                if commit_unit is not None:
                    if len(list_of_row_data) % commit_unit == 0:
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
