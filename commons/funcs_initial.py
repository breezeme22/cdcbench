from commons.mgr_config import ConfigManager
from commons.mgr_logger import LoggerManager
from commons.mgr_connection import ConnectionManager, MapperBase
from commons.funcs_common import get_commit_msg, get_json_data
from mappers.oracle_mappings import UpdateTest, DeleteTest

from sqlalchemy.exc import DatabaseError
from datetime import datetime

import random
import os


class InitialFunctions:

    def __init__(self):

        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()
        self.src_engine = conn_mgr.src_engine

        file_name = "dml.dat"
        self.bench_data = get_json_data(os.path.join("data", file_name))
        self.product_name_data = self.bench_data.get("product_name")
        self.product_date_data = self.bench_data.get("product_date")
        self.logger.debug("Load data file ({})".format(file_name))

    # update_test & delete_test table initialize
    def initializing_data(self, table, total_data=300000, commit_unit=20000):
        """
        update_test & delete_test table의 초기 데이터 생성 함수

        :param table: 어느 테이블에 데이터를 insert 할 것인지 지정. Mapper Class 그대로 입력 ex) UpdateTest / DeleteTest
        :param total_data: insert할 데이터의 양을 지정. 기본 값은 300000.
        :param commit_unit: Commit 기준을 지정. 기본 값은 20000건당 commit 수행
        """

        self.logger.debug("Func. initializing_data is started")

        print("  Generate {} Table's data ".format(table.__tablename__), end="", flush=True)
        self.logger.info("Start \"{}\" Table's data generation".format(table.__tablename__))

        data_list = []

        self.logger.info("  table_name: " + table.__tablename__)
        self.logger.info("  total_data: " + str(total_data))
        self.logger.info("  commit_unit: " + str(commit_unit))

        try:

            start_val = 1

            for i in range(1, total_data + 1):
                pn = self.product_name_data[random.randrange(0, len(self.product_name_data))]
                pd = self.product_date_data[random.randrange(0, len(self.product_date_data))]
                # self.logger.debug(t)
                product_date = datetime.strptime(pd, "%Y-%m-%d-%H-%M-%S")

                if table == UpdateTest:
                    product_name = "1"
                else:
                    product_name = pn

                data_list.append({"product_name": product_name, "product_date": product_date, "separate_col": start_val})

                if i % commit_unit == 0:
                    self.src_engine.execute(table.__table__.insert(), data_list)
                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1
                    data_list.clear()

            if total_data % commit_unit != 0:
                self.src_engine.execute(table.__table__.insert(), data_list)
                self.logger.debug(get_commit_msg(start_val))

            print("... Success\n")
            self.logger.info("\"{}\" Table's data generation is completed".format(table.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. initializing_data is ended")

    def create(self):

        self.logger.debug("Func. create is started")

        try:
            print("  Create CDCBENCH's objects ", end="", flush=True)
            MapperBase.metadata.create_all(bind=self.src_engine)
            print("... Success\n")
            self.logger.info("CDCBENCH's objects is created")

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. create is ended")

    def drop(self):

        self.logger.debug("Func. drop is started")

        try:
            print("  Drop CDCBENCH's objects ", end="", flush=True)
            MapperBase.metadata.drop_all(bind=self.src_engine)
            print("... Success\n")
            self.logger.info("CDCBENCH's objects is dropped")

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. drop is ended")
