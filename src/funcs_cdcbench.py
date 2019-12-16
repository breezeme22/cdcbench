from src.constants import *
from src.funcs_common import get_elapsed_time_msg, get_commit_msg, get_rollback_msg, \
                                 get_object_name, get_start_time_msg, print_complete_msg, print_description_msg
from src.funcs_datagen import get_separate_col_val, get_file_data, get_sample_table_data, data_file_name
from src.mgr_logger import LoggerManager

from sqlalchemy.exc import DatabaseError
from datetime import datetime
from tqdm import tqdm

import time
import logging


class FuncsCdcbench:

    def __init__(self, conn, mapper):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.engine = conn.engine
        self.connection = self.engine.connect()
        self.db_session = conn.db_session

        self.dbms_type = conn.connection_info["dbms_type"]

        self.mapper = mapper.get_mappers()

    def single_insert(self, number_of_data, commit_unit, rollback, verbose):
        """
        SQLAlchemy ORM 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Single Insert로 동작.

        :param number_of_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        :param rollback: Transaction Commit/Rollback 여부를 지정
        """

        try:

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
            separate_col_val = get_separate_col_val(self.engine, InsertTest.__table__, InsertTest.column_names[3])

            column_names = InsertTest.column_names[:]
            # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거
            column_names.remove(column_names[0])

            start_time = time.time()

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                row_data = list(get_sample_table_data(file_data, INSERT_TEST, column_names, separate_col_val).values())

                self.db_session.add(InsertTest(*row_data))

                if i % commit_unit == 0:
                    if rollback is True:
                        self.db_session.rollback()
                        self.logger.debug(get_rollback_msg(separate_col_val))
                    else:
                        self.db_session.commit()
                        self.logger.debug(get_commit_msg(separate_col_val))
                    separate_col_val += 1

            if number_of_data % commit_unit != 0:
                if rollback is True:
                    self.db_session.rollback()
                    self.logger.debug(get_rollback_msg(separate_col_val))
                else:
                    self.db_session.commit()
                    self.logger.debug(get_commit_msg(separate_col_val))

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(InsertTest.__table__))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. single_insert is ended")

    def multi_insert(self, number_of_data, commit_unit, rollback, verbose):
        """
        SQLAlchemy Core 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Multi insert로 동작.

        :param number_of_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        :param rollback: Transaction Commit/Rollback 여부를 지정
        """

        try:

            table_name = get_object_name(self.mapper.metadata.tables.keys(), INSERT_TEST)
            table = self.mapper.metadata.tables[table_name]

            column_names = table.columns.keys()[:]

            print(get_start_time_msg(datetime.now()))
            print_description_msg("INSERT", table, verbose)
            self.logger.info("Start data insert in the \"{}\" Table".format(table))

            insert_info_msg = "Insert Information: {0}'number of data': {1}, 'commit unit': {2}, 'single': {3}{4}" \
                .format("{", number_of_data, commit_unit, False, "}")

            self.logger.info(insert_info_msg)

            list_of_row_data = []
            file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])
            separate_col_val = get_separate_col_val(self.engine, table, column_names[3])

            # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거
            column_names.remove(column_names[0])

            start_time = time.time()

            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                list_of_row_data.append(get_sample_table_data(file_data, INSERT_TEST, column_names, separate_col_val))

                if i % commit_unit == 0:

                    with self.connection.begin() as tx:
                        self.connection.execute(table.insert(), list_of_row_data)
                        if rollback is True:
                            tx.rollback()
                            self.logger.debug(get_rollback_msg(separate_col_val))
                        else:
                            tx.commit()
                            self.logger.debug(get_commit_msg(separate_col_val))

                    separate_col_val += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:

                with self.connection.begin() as tx:
                    self.connection.execute(table.insert(), list_of_row_data)
                    if rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(separate_col_val))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(separate_col_val))

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. multi_insert is ended")

    def update(self, start_separate_col, end_separate_col, rollback, verbose):
        """
        SQLAlchemy Core 방식으로 update_test 테이블의 product_name 컬럼의 값을 변경.

        :param start_separate_col: update시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                    그러므로 update할 데이터의 시작 separate_col 값을 지정.
        :param end_separate_col: update할 데이터의 마지막 separate_col 값을 지정.
        :param rollback: Transaction Commit/Rollback 여부를 지정
        """

        try:

            table_name = get_object_name(self.mapper.metadata.tables.keys(), UPDATE_TEST)
            table = self.mapper.metadata.tables[table_name]

            column_names = table.columns.keys()[:]
            product_name = column_names[1]
            separate_col = column_names[3]

            print(get_start_time_msg(datetime.now()))
            print_description_msg("UPDAT", table, verbose)
            self.logger.info("Start data update in the \"{}\" Table".format(table))

            update_info_msg = "Update Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(update_info_msg)

            file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

            # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거
            column_names.remove(column_names[0])

            start_time = time.time()

            for i in tqdm(range(start_separate_col, end_separate_col+1), disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format):

                row_data = get_sample_table_data(file_data, UPDATE_TEST, column_names, update=True)

                with self.connection.begin() as tx:
                    self.connection.execute(
                        table.update().values({product_name: row_data[product_name]})
                             .where(table.columns[separate_col] == i)
                    )
                    if rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(i))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(i))

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
            raise

        finally:
            self.logger.debug("Func. update is ended")

    # delete core
    def delete(self, start_separate_col, end_separate_col, rollback, verbose):

        try:

            table_name = get_object_name(self.mapper.metadata.tables.keys(), DELETE_TEST)
            table = self.mapper.metadata.tables[table_name]

            column_names = table.columns.keys()[:]

            delete_info_msg = "Delete Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(delete_info_msg)

            print(get_start_time_msg(datetime.now()))
            print_description_msg("DELET", table, verbose)
            self.logger.info("Start data delete in the \"{}\" Table".format(table))

            start_time = time.time()

            for i in tqdm(range(start_separate_col, end_separate_col + 1), disable=verbose, ncols=tqdm_ncols,
                          bar_format=tqdm_bar_format):

                with self.connection.begin() as tx:
                    self.connection.execute(table.delete().where(table.columns[column_names[3]] == i))
                    if rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(i))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(i))

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
            self.logger.debug("Func. delete is ended")
