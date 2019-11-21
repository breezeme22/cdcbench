from commons.constants import *
from commons.funcs_common import get_elapsed_time_msg, get_commit_msg, get_rollback_msg, get_json_data
from commons.mgr_logger import LoggerManager

from mappers import oracle_mappers, mysql_mappers, sqlserver_mappers, postgresql_mappers

from sqlalchemy.exc import DatabaseError
from datetime import datetime

import random
import os
import time
import logging


class FuncsCdcbench:

    __data_dir = "data"

    def __init__(self, conn, source_dbms_type):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.src_connection = conn.src_engine.connect()
        self.src_db_session = conn.src_db_session

        self.src_mapper = conn.get_src_mapper()
        self.trg_mapper = conn.get_trg_mapper()

        self.source_dbms_type = source_dbms_type

    def single_insert(self, number_of_data, commit_unit, is_rollback):
        """
        SQLAlchemy ORM 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Single Insert로 동작.

        :param number_of_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        :param is_rollback: Transaction Commit/Rollback 여부를 지정
        """

        try:

            tab_insert_test = None

            if self.source_dbms_type == ORACLE:
                tab_insert_test = oracle_mappers.InsertTest
            elif self.source_dbms_type == MYSQL:
                tab_insert_test = mysql_mappers.InsertTest
            elif self.source_dbms_type == SQLSERVER:
                tab_insert_test = sqlserver_mappers.InsertTest
            elif self.source_dbms_type == POSTGRESQL:
                tab_insert_test = postgresql_mappers.InsertTest

            file_name = 'dml.dat'
            file_data = get_json_data(os.path.join(self.__data_dir, file_name))
            list_of_product_name = file_data.get("PRODUCT_NAME")
            list_of_product_date = file_data.get("PRODUCT_DATE")
            self.logger.debug("Load data file ({})".format(file_name))

            print("  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(tab_insert_test.__tablename__), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(tab_insert_test.__tablename__))

            insert_info_msg = "Insert Information: {0}\"number of data\": {1}, \"commit unit\": {2}, \"single\": {3}{4}" \
                .format("{", number_of_data, commit_unit, True, "}")

            self.logger.info(insert_info_msg)

            start_val = 1

            start_time = time.time()

            for i in range(1, number_of_data+1):
                random_pn = list_of_product_name[random.randrange(0, len(list_of_product_name))]
                random_pd = list_of_product_date[random.randrange(0, len(list_of_product_date))]

                formatted_pd = datetime.strptime(random_pd, '%Y-%m-%d %H:%M:%S')

                row_data = tab_insert_test(random_pn, formatted_pd, start_val)

                self.src_db_session.add(row_data)

                if i % commit_unit == 0:
                    if is_rollback is True:
                        self.src_db_session.rollback()
                        self.logger.debug(get_rollback_msg(start_val))
                    else:
                        self.src_db_session.commit()
                        self.logger.debug(get_commit_msg(start_val))
                    start_val += 1

            if number_of_data % commit_unit != 0:
                if is_rollback is True:
                    self.src_db_session.rollback()
                    self.logger.debug(get_rollback_msg(start_val))
                else:
                    self.src_db_session.commit()
                    self.logger.debug(get_commit_msg(start_val))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(tab_insert_test.__tablename__))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. insert_orm is ended")

    def multi_insert(self, number_of_data, commit_unit, is_rollback):
        """
        SQLAlchemy Core 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Multi insert로 동작.

        :param number_of_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        :param is_rollback: Transaction Commit/Rollback 여부를 지정
        """

        try:

            if self.source_dbms_type == POSTGRESQL:
                tab_insert_test = self.src_mapper.metadata.tables[INSERT_TEST.lower()]
            else:
                tab_insert_test = self.src_mapper.metadata.tables[INSERT_TEST]

            column_names = tab_insert_test.columns.keys()[:]

            file_name = 'dml.dat'
            self.logger.debug("Load data file ({})".format(file_name))
            file_data = get_json_data(os.path.join(self.__data_dir, file_name))
            list_of_product_name = file_data.get("PRODUCT_NAME")
            list_of_product_date = file_data.get("PRODUCT_DATE")

            print("  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(tab_insert_test), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(tab_insert_test))

            insert_info_msg = "Insert Information: {0}'number of data': {1}, 'commit unit': {2}, 'single': {3}{4}" \
                .format("{", number_of_data, commit_unit, False, "}")

            self.logger.info(insert_info_msg)

            list_of_row_data = []
            start_val = 1

            start_time = time.time()

            for i in range(1, number_of_data+1):

                random_pn = list_of_product_name[random.randrange(0, len(list_of_product_name))]
                random_pd = list_of_product_date[random.randrange(0, len(list_of_product_date))]
                formatted_pd = datetime.strptime(random_pd, '%Y-%m-%d %H:%M:%S')

                list_of_row_data.append({
                    column_names[1]: random_pn, column_names[2]: formatted_pd, column_names[3]: start_val
                })

                if i % commit_unit == 0:

                    with self.src_connection.begin() as tx:
                        self.src_connection.execute(tab_insert_test.insert(), list_of_row_data)
                        if is_rollback is True:
                            tx.rollback()
                            self.logger.debug(get_rollback_msg(start_val))
                        else:
                            tx.commit()
                            self.logger.debug(get_commit_msg(start_val))

                    start_val += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:

                with self.src_connection.begin() as tx:
                    self.src_connection.execute(tab_insert_test.insert(), list_of_row_data)
                    if is_rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(start_val))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(start_val))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(tab_insert_test))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. insert_core is ended")

    def update(self, start_separate_col, end_separate_col, is_rollback):
        """
        SQLAlchemy Core 방식으로 update_test 테이블의 product_name 컬럼의 값을 변경.

        :param start_separate_col: update시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                    그러므로 update할 데이터의 시작 separate_col 값을 지정.
        :param end_separate_col: update할 데이터의 마지막 separate_col 값을 지정.
        :param is_rollback: Transaction Commit/Rollback 여부를 지정
        """

        try:

            if self.source_dbms_type == POSTGRESQL:
                tab_update_test = self.src_mapper.metadata.tables[UPDATE_TEST.lower()]
            else:
                tab_update_test = self.src_mapper.metadata.tables[UPDATE_TEST]

            column_names = tab_update_test.columns.keys()[:]

            file_name = 'dml.dat'
            file_data = get_json_data(os.path.join(self.__data_dir, file_name))
            list_of_product_name = file_data.get("PRODUCT_NAME")
            self.logger.debug("Load data file ({})".format(file_name))

            print("  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Updating data in the \"{}\" Table".format(tab_update_test), flush=True, end=" ")
            self.logger.info("Start data update in the \"{}\" Table".format(tab_update_test))

            update_info_msg = "Update Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(update_info_msg)

            start_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):

                random_pn = list_of_product_name[random.randrange(0, len(list_of_product_name))]

                with self.src_connection.begin() as tx:
                    self.src_connection.execute(tab_update_test.update()
                                                .values({column_names[1]: random_pn})
                                                .where(tab_update_test.columns[column_names[3]] == i))
                    if is_rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(i))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(tab_update_test))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. update_core is ended")

    # delete core
    def delete(self, start_separate_col, end_separate_col, is_rollback):

        try:

            if self.source_dbms_type == POSTGRESQL:
                tab_delete_test = self.src_mapper.metadata.tables[DELETE_TEST.lower()]
            else:
                tab_delete_test = self.src_mapper.metadata.tables[DELETE_TEST]

            column_names = tab_delete_test.columns.keys()[:]

            delete_info_msg = "Delete Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(delete_info_msg)

            print("  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(tab_delete_test), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(tab_delete_test))

            start_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):

                with self.src_connection.begin() as tx:
                    self.src_connection.execute(tab_delete_test.delete()
                                                               .where(tab_delete_test.columns[column_names[3]] == i))
                    if is_rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(i))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(tab_delete_test))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. delete_core is ended")
