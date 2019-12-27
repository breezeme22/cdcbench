from src.constants import tqdm_bar_format, tqdm_ncols, INSERT_TEST
from src.funcs_common import get_commit_msg, get_rollback_msg, get_elapsed_time_msg, get_object_name, \
                             get_start_time_msg, print_complete_msg, print_description_msg, print_error_msg
from src.funcs_datagen import get_sample_table_data, get_file_data, data_file_name, get_separate_col_val
from src.mgr_logger import LoggerManager

from sqlalchemy import func, select
from sqlalchemy.sql import and_, or_ ,not_
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

    def single_insert(self, number_of_data, commit_unit, rollback, verbose):

        table_name = get_object_name(self.mapper.metadata.tables.keys(), INSERT_TEST)

        class InsertTest(self.mapper):
            __table__ = self.mapper.metadata.tables[table_name]
            column_names = list([column.name for column in __table__.c])

            def __init__(self, product_name=None, product_date=None, separate_col=None):
                setattr(self, self.column_names[1], product_name)
                setattr(self, self.column_names[2], product_date)
                setattr(self, self.column_names[3], separate_col)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("INSERT", InsertTest.__table__, verbose)
        self.logger.info("Start data insert in the \"{}\" Table".format(InsertTest.__table__))

        insert_info_msg = "Insert Information: {0}\"number of data\": {1}, \"commit unit\": {2}, \"single\": {3}{4}" \
                          .format("{", number_of_data, commit_unit, True, "}")

        self.logger.info(insert_info_msg)

        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

        column_names = InsertTest.column_names[:]
        separate_col_name = column_names[3]

        start_time = time.time()

        try:
            separate_col_val = get_separate_col_val(self.engine, InsertTest.__table__, separate_col_name)

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                row_data = list(get_sample_table_data(file_data, INSERT_TEST, column_names, separate_col_val).values())

                self.db_session.add(InsertTest(*row_data))

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
        print("  {}\n".format(elapse_time_msg))
        self.logger.info(elapse_time_msg)

        self.logger.info("End data insert in the \"{}\" Table".format(InsertTest.__table__))

    def _tx_end(self, tx, rollback, end_count):
        if rollback is True:
            tx.rollback()
            self.logger.debug(get_rollback_msg(end_count))
        else:
            tx.commit()
            self.logger.debug(get_commit_msg(end_count))

    def insert(self, table_name, column_items, number_of_data, commit_unit, rollback, verbose):

        table = self.mapper.metadata.tables[get_object_name(self.mapper.metadata.tables.keys(), table_name)]

        all_column_names = table.columns.keys()[:]  # table column name 획득
        selected_column_names = get_selected_column_names(column_items, all_column_names)

        insert_info_msg = "Insert Information: {}\"Table Name\" : {}, \"Number of Data\": {}, " \
                          "\"Commit Unit\": {} {}".format("{", table, number_of_data, commit_unit, "}")

        self.logger.info(insert_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("INSERT", table, verbose)
        self.logger.info("Start data insert in the \"{}\" Table".format(table))

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
        print("  {}\n".format(elapse_time_msg))
        self.logger.info(elapse_time_msg)

        self.logger.info("End data insert in the \"{}\" Table".format(table))

    def update(self, table_name, column_names, start_id, end_id, commit_unit, rollback, verbose):

        table = self.mapper.metadata.tables[get_object_name(self.mapper.metadata.tables.keys(), table_name)]

        update_info_msg = "Update Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
                          .format("{", table_name, start_id, end_id, "}")

        self.logger.info(update_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("UPDAT", table, verbose)
        self.logger.info("Start data update in the \"{}\" Table".format(table))

        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

        all_column_names = table.columns.keys()[:]  # table column name 획득
        selected_column_names = get_selected_column_names(column_names, all_column_names)
        end_count = 1

        list_of_row_data = []

        try:

            where_column = table.columns[all_column_names[0]]
            select_where_clause = and_(start_id <= where_column, where_column <= end_id)
            # Update 대상 Row 조회
            update_rows = self.db_session.query(where_column.name).filter(select_where_clause).order_by(where_column.name).yield_per(commit_unit)
            update_row_count = update_rows.count()

            update_where_clause = where_column == bindparam("b_{}".format(where_column.name))
            update_stmt = table.update() \
                .values(dict((column_name, bindparam(column_name)) for column_name in selected_column_names)) \
                .where(update_where_clause)

            start_time = time.time()

            for i in tqdm(update_rows, total=update_row_count, disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                row_data = get_sample_table_data(file_data, table_name, selected_column_names, dbms_type=self.dbms_type)
                row_data["b_{}".format(where_column.name)] = i[0]

                list_of_row_data.append(row_data)

                if len(list_of_row_data) % commit_unit == 0:
                    with self.connection.begin() as tx:
                        self.connection.execute(update_stmt, list_of_row_data)
                        self._tx_end(tx, rollback, end_count)

                    end_count += 1
                    list_of_row_data.clear()

            # Commit 단위별로 처리된 후 남은 Row 마저 Commit
            if update_row_count % commit_unit != 0:
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

    def delete(self, table_name, start_id, end_id, commit_unit, rollback, verbose):

        table = self.mapper.metadata.tables[get_object_name(self.mapper.metadata.tables.keys(), table_name)]

        delete_info_msg = "Delete Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
            .format("{", table, start_id, end_id, "}")

        self.logger.info(delete_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("DELET", table, verbose)
        self.logger.info("Start data delete in the \"{}\" Table".format(table))

        list_of_row_data = []
        end_count = 1

        try:

            t_id = table.columns.keys()[0]
            where_column = table.columns[t_id]
            select_where_clause = and_(start_id <= where_column, where_column <= end_id)
            delete_rows = self.db_session.query(where_column.name).filter(select_where_clause).order_by(
                where_column.name).yield_per(commit_unit)
            delete_row_count = delete_rows.count()

            delete_where_clause = where_column == bindparam("b_{}".format(where_column.name))
            delete_stmt = table.delete().where(delete_where_clause)

            start_time = time.time()

            for i in tqdm(delete_rows, total=delete_row_count, disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                row_data = {"b_{}".format(where_column.name): i[0]}
                list_of_row_data.append(row_data)

                if len(list_of_row_data) % commit_unit == 0:
                    with self.connection.begin() as tx:
                        self.connection.execute(delete_stmt, list_of_row_data)
                        self._tx_end(tx, rollback, end_count)

                    end_count += 1
                    list_of_row_data.clear()

            if delete_row_count % commit_unit != 0:
                with self.connection.begin() as tx:
                    self.connection.execute(delete_stmt, list_of_row_data)
                    self._tx_end(tx, rollback, end_count)

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func.delete is ended")


def get_selected_column_names(column_items, all_column_names):

    if column_items is None:
        return all_column_names[1:]
    else:
        if all(isinstance(item, int) for item in column_items):
            return [all_column_names[column_id-1] for column_id in column_items]
        else:
            return [get_object_name(all_column_names, column_name) for column_name in column_items]