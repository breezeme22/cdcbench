from commons.constants import *
from commons.funcs_common import get_elapsed_time_msg, get_commit_msg, get_json_data
from commons.mgr_config import ConfigManager
from commons.mgr_connection import ConnectionManager
from commons.mgr_logger import LoggerManager

from sqlalchemy.exc import DatabaseError
from datetime import datetime

import random
import os
import time
import logging


class DmlFuntions:

    __data_dir = "data"

    def __init__(self):

        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()
        self.src_engine = conn_mgr.src_engine
        self.src_db_session = conn_mgr.src_db_session

        self.src_mapper = conn_mgr.get_src_mapper()

        if self.config.source_dbms_type == dialect_driver[SQLSERVER]:
            for table in self.src_mapper.metadata.sorted_tables:
                table.schema = self.config.source_user_id

        file_name = 'dml.dat'
        self.file_data = get_json_data(os.path.join(self.__data_dir, file_name))
        self.list_of_product_name = self.file_data.get("product_name")
        self.list_of_product_date = self.file_data.get("product_date")
        self.logger.debug("Load data file ({})".format(file_name))

    def insert_orm(self, number_of_data, commit_unit):
        """
        SQLAlchemy ORM 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Single Insert로 동작.

        :param number_of_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        """
        self.logger.debug("Func. insert_orm is started")

        if self.config.sql_log_level != logging.WARNING:
            self.logger = LoggerManager.get_sql_logger(self.config.sql_log_level)

        try:

            tab_insert_test = self.src_mapper.metadata.tables[INSERT_TEST]

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(tab_insert_test), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(tab_insert_test))

            insert_info_msg = "Insert Information: {0}\"number of data\": {1}, \"commit unit\": {2}, \"single\": {3}{4}" \
                .format("{", number_of_data, commit_unit, True, "}")

            self.logger.info(insert_info_msg)

            start_val = 1

            start_time = time.time()

            for i in range(1, number_of_data+1):
                random_pn = self.list_of_product_name[random.randrange(0, len(self.list_of_product_name))]
                random_pd = self.list_of_product_date[random.randrange(0, len(self.list_of_product_date))]

                formatted_pd = datetime.strptime(random_pd, '%Y-%m-%d-%H-%M-%S')

                row_data = tab_insert_test(random_pn, formatted_pd, start_val)

                self.src_db_session.add(row_data)

                if i % commit_unit == 0:
                    self.src_db_session.commit()
                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1

            if number_of_data % commit_unit != 0:
                self.src_db_session.commit()
                self.logger.debug(get_commit_msg(start_val))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(tab_insert_test))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. insert_orm is ended")

    def insert_core(self, number_of_data, commit_unit):
        """
        SQLAlchemy Core 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Multi insert로 동작.

        :param number_of_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        """

        self.logger.debug("Func. insert_core is started")

        try:

            tab_insert_test = self.src_mapper.metadata.tables[INSERT_TEST]

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(tab_insert_test), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(tab_insert_test))

            insert_info_msg = "Insert Information: {0}'number of data': {1}, 'commit unit': {2}, 'single': {3}{4}" \
                .format("{", number_of_data, commit_unit, False, "}")

            self.logger.info(insert_info_msg)

            list_of_insert_data = []
            start_val = 1

            start_time = time.time()

            for i in range(1, number_of_data+1):
                random_pn = self.list_of_product_name[random.randrange(0, len(self.list_of_product_name))]
                random_pd = self.list_of_product_date[random.randrange(0, len(self.list_of_product_date))]

                formatted_pd = datetime.strptime(random_pd, '%Y-%m-%d-%H-%M-%S')

                list_of_insert_data.append({"PRODUCT_NAME": random_pn, "PRODUCT_DATE": formatted_pd, "SEPARATE_COL": start_val})

                if i % commit_unit == 0:
                    self.src_engine.execute(tab_insert_test.insert(), list_of_insert_data)
                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1
                    list_of_insert_data.clear()

            if number_of_data % commit_unit != 0:
                self.src_engine.execute(tab_insert_test.insert(), list_of_insert_data)
                self.logger.debug(get_commit_msg(start_val))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(tab_insert_test))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. insert_core is ended")

    def update_orm(self, start_separate_col, end_separate_col):
        """
        SQLAlchemy ORM 방식으로 update_test 테이블의 product_name 컬럼의 값을 변경.

        :param start_separate_col: update시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                    그러므로 update할 데이터의 시작 separate_col 값을 지정.
        :param end_separate_col: update할 데이터의 마지막 separate_col 값을 지정.
        """

        self.logger.debug("Func. update_orm is started")

        try:

            tab_update_test = self.src_mapper.metadata.tables[UPDATE_TEST]

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Updating data in the \"{}\" Table".format(tab_update_test), flush=True, end=" ")
            self.logger.info("Start data update in the \"{}\" Table".format(tab_update_test))

            update_info_msg = "Update Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(update_info_msg)

            start_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                random_pn = self.list_of_product_name[random.randrange(0, len(self.list_of_product_name))]

                self.src_db_session.query(tab_update_test)\
                                   .update({tab_update_test.columns["PRODUCT_NAME"]: random_pn})\
                                   .filter(tab_update_test.columns["SEPARATE_COL"] == i)

                self.src_db_session.commit()
                self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(tab_update_test))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. update_orm is ended")

    def update_core(self, start_separate_col=1, end_separate_col=15):
        """
        SQLAlchemy Core 방식으로 update_test 테이블의 product_name 컬럼의 값을 변경.

        :param start_separate_col: update시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                    그러므로 update할 데이터의 시작 separate_col 값을 지정.
        :param end_separate_col: update할 데이터의 마지막 separate_col 값을 지정.
        """

        self.logger.debug("Func. update_core is started")

        try:

            tab_update_test = self.src_mapper.metadata.tables[UPDATE_TEST]

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Updating data in the \"{}\" Table".format(tab_update_test), flush=True, end=" ")
            self.logger.info("Start data update in the \"{}\" Table".format(tab_update_test))

            update_info_msg = "Update Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(update_info_msg)

            start_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                random_pn = self.list_of_product_name[random.randrange(0, len(self.list_of_product_name))]

                self.src_engine.execute(tab_update_test.update()
                                                       .values(PRODUCT_NAME=random_pn)
                                                       .where(tab_update_test.columns["SEPARATE_COL"] == i))

                self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(tab_update_test))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. update_core is ended")

    def delete_orm(self, start_separate_col=1, end_separate_col=15):
        """
        ORM 방식으로 delete_test 테이블의 데이터를 삭제.

        :param start_separate_col: delete시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                    그러므로 delete할 데이터의 시작 separate_col 값을 지정.
        :param end_separate_col: delete할 데이터의 마지막 separate_col 값을 지정.
        """

        self.logger.debug("Func. delete_orm is started")

        try:

            tab_delete_test = self.src_mapper.metadata.tables[DELETE_TEST]

            delete_info_msg = "Delete Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(delete_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(tab_delete_test), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(tab_delete_test))

            start_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                self.src_db_session.query(tab_delete_test).delete()\
                                                          .filter(tab_delete_test.columns["SEPARATE_COL"] == i)
                self.src_db_session.commit()
                self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(tab_delete_test))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. delete_orm is ended")

    # delete core
    def delete_core(self, start_separate_col=1, end_separate_col=15):

        self.logger.debug("Func. delete_core is started")

        try:

            tab_delete_test = self.src_mapper.metadata.tables[DELETE_TEST]

            delete_info_msg = "Delete Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(delete_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(tab_delete_test), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(tab_delete_test))

            start_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                self.src_engine.execute(tab_delete_test.delete()
                                                       .where(tab_delete_test.columns["SEPARATE_COL"] == i))
                self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(tab_delete_test))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. delete_core is ended")
