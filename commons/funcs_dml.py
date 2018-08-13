from commons.mgr_config import ConfigManager
from commons.mgr_logger import LoggerManager
from commons.mgr_connection import ConnectionManager
from commons.funcs_common import get_elapsed_time_msg, get_commit_msg, get_json_data
from mappers.oracle_mappings import InsertTest, UpdateTest, DeleteTest

from sqlalchemy.exc import DatabaseError
from datetime import datetime

import random
import os
import time


class DmlFuntions:

    def __init__(self):

        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()
        self.src_engine = conn_mgr.src_engine
        self.src_db_session = conn_mgr.src_db_session

        file_name = 'dml.dat'
        self.bench_data = get_json_data(os.path.join("data", file_name))
        self.product_name_data = self.bench_data.get("product_name")
        self.product_date_data = self.bench_data.get("product_date")
        self.logger.debug("Load data file ({})".format(file_name))

    def insert_orm(self, insert_data, commit_unit):
        """
        SQLAlchemy ORM 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Single Insert로 동작.

        :param insert_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        """
        self.logger.debug("Func. insert_orm is started")

        try:

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(InsertTest.__tablename__), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(InsertTest.__tablename__))

            insert_info_msg = "Insert Information: {0}\"number of data\": {1}, \"commit unit\": {2}, \"single\": {3}{4}" \
                .format("{", insert_data, commit_unit, True, "}")

            self.logger.info(insert_info_msg)

            start_val = 1

            s_time = time.time()

            for i in range(1, insert_data+1):
                pn = self.product_name_data[random.randrange(0, len(self.product_name_data))]
                pd = self.product_date_data[random.randrange(0, len(self.product_date_data))]

                product_date = datetime.strptime(pd, '%Y-%m-%d-%H-%M-%S')
                new_data = InsertTest(pn, product_date, start_val)

                self.src_db_session.add(new_data)

                if i % commit_unit == 0:
                    self.src_db_session.commit()
                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1

            if insert_data % commit_unit != 0:
                self.src_db_session.commit()
                self.logger.debug(get_commit_msg(start_val))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(InsertTest.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. insert_orm is ended")

    def insert_core(self, insert_data, commit_unit):
        """
        SQLAlchemy Core 방식으로 insert_test 테이블에 데이터를 insert.
            Oracle에서 Multi insert로 동작.

        :param insert_data: 테이블에 insert할 데이터의 양을 지정
        :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
        """

        self.logger.debug("Func. insert_core is started")

        try:

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(InsertTest.__tablename__), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(InsertTest.__tablename__))

            insert_info_msg = "Insert Information: {0}'number of data': {1}, 'commit unit': {2}, 'single': {3}{4}" \
                .format("{", insert_data, commit_unit, False, "}")

            self.logger.info(insert_info_msg)

            data_list = []
            start_val = 1

            s_time = time.time()

            for i in range(1, insert_data+1):
                pn = self.product_name_data[random.randrange(0, len(self.product_name_data))]
                pd = self.product_date_data[random.randrange(0, len(self.product_date_data))]

                product_date = datetime.strptime(pd, '%Y-%m-%d-%H-%M-%S')
                data_list.append({"product_name": pn, "product_date": product_date, "separate_col": start_val})

                if i % commit_unit == 0:
                    self.src_engine.execute(InsertTest.__table__.insert(), data_list)
                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1
                    data_list.clear()

            if insert_data % commit_unit != 0:
                a = self.src_engine.execute(InsertTest.__table__.insert(), data_list)
                self.logger.debug(get_commit_msg(start_val))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(InsertTest.__tablename__))

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

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Updating data in the \"{}\" Table".format(UpdateTest.__tablename__), flush=True, end=" ")
            self.logger.info("Start data update in the \"{}\" Table".format(UpdateTest.__tablename__))

            update_info_msg = "Update Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(update_info_msg)

            s_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                pn = self.product_name_data[random.randrange(0, len(self.product_name_data))]

                self.src_db_session.query(UpdateTest)\
                                   .update({UpdateTest.product_name: pn})\
                                   .filter(UpdateTest.separate_col == i)

                self.src_db_session.commit()
                self.logger.debug(get_commit_msg(i))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(UpdateTest.__tablename__))

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

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Updating data in the \"{}\" Table".format(UpdateTest.__tablename__), flush=True, end=" ")
            self.logger.info("Start data update in the \"{}\" Table".format(UpdateTest.__tablename__))

            update_info_msg = "Update Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(update_info_msg)

            s_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                pn = self.product_name_data[random.randrange(0, len(self.product_name_data))]

                self.src_engine.execute(UpdateTest.__table__.update()
                                                            .values(product_name=pn)
                                                            .where(UpdateTest.separate_col == i))

                self.logger.debug(get_commit_msg(i))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(UpdateTest.__tablename__))

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

            delete_info_msg = "Delete Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(delete_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(DeleteTest.__tablename__), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(DeleteTest.__tablename__))

            s_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                self.src_db_session.query(DeleteTest).delete().filter(DeleteTest.separate_col == i)
                self.src_db_session.commit()
                self.logger.debug(get_commit_msg(i))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(DeleteTest.__tablename__))

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

            delete_info_msg = "Delete Information: {}'start separate_col': {}, 'end separate_col': {}{}" \
                .format("{", start_separate_col, end_separate_col, "}")

            self.logger.info(delete_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(DeleteTest.__tablename__), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(DeleteTest.__tablename__))

            s_time = time.time()

            for i in range(start_separate_col, end_separate_col+1):
                self.src_engine.execute(DeleteTest.__table__.delete().where(DeleteTest.separate_col == i))
                self.logger.debug(get_commit_msg(i))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(DeleteTest.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. delete_core is ended")
