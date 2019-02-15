from commons.mgr_config import ConfigManager
from commons.mgr_logger import LoggerManager
from commons.mgr_connection import ConnectionManager
from commons.funcs_common import get_commit_msg, get_json_data, get_mapper
from commons.constants import *

from sqlalchemy.exc import DatabaseError
from datetime import datetime

import random
import os


class InitialFunctions:

    __data_dir = "data"

    def __init__(self):

        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()
        self.src_engine = conn_mgr.src_engine
        self.trg_engine = conn_mgr.trg_engine
        self.src_mapper = conn_mgr.get_src_mapper()
        self.trg_mapper = conn_mgr.get_trg_mapper()

        file_name = "dml.dat"
        self.bench_data = get_json_data(os.path.join(self.__data_dir, file_name))
        self.product_name_data = self.bench_data.get("product_name")
        self.product_date_data = self.bench_data.get("product_date")
        self.logger.debug("Load data file ({})".format(file_name))

    def create(self, destination):

        self.logger.debug("Func. create is started")

        try:
            print("  Create CDCBENCH's objects ", end="", flush=True)

            if destination == SOURCE:
                self.src_mapper.metadata.create_all(bind=self.src_engine)
            elif destination == TARGET:
                self.trg_mapper.metadata.create_all(bind=self.trg_engine)
            elif destination == BOTH:
                self.src_mapper.metadata.create_all(bind=self.src_engine)
                self.trg_mapper.metadata.create_all(bind=self.trg_engine)

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

    def drop(self, destination):

        self.logger.debug("Func. drop is started")

        try:
            print("  Drop CDCBENCH's objects ", end="", flush=True)

            if destination == SOURCE:
                self.src_mapper.metadata.drop_all(bind=self.src_engine)
            elif destination == TARGET:
                self.trg_mapper.metadata.drop_all(bind=self.trg_engine)
            elif destination == BOTH:
                self.src_mapper.metadata.drop_all(bind=self.src_engine)
                self.trg_mapper.metadata.drop_all(bind=self.trg_engine)

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

    # update_test & delete_test table initialize
    def initializing_data(self, destination, table_name, total_data, commit_unit):
        """
        update_test & delete_test table의 초기 데이터 생성 함수
        
        :param destination: initial 대상을 SOURCE / TARGET / BOTH 로 지정
        :param table_name: 어느 테이블에 데이터를 insert 할 것인지 지정. Mapper Class 그대로 입력
        :param total_data: insert할 데이터의 양을 지정. 기본 값은 300000.
        :param commit_unit: Commit 기준을 지정. 기본 값은 20000건당 commit 수행
        """

        self.logger.debug("Func. initializing_data is started")

        print("  Generate {} Table's data ".format(table_name), end="", flush=True)
        self.logger.info("Start \"{}\" Table's data generation".format(table_name))

        data_list = []
        src_table = get_mapper(self.config.source_dbms_type, table_name)
        trg_table = get_mapper(self.config.target_dbms_type, table_name)

        self.logger.info("  table_name: " + table_name)
        self.logger.info("  total_data: " + str(total_data))
        self.logger.info("  commit_unit: " + str(commit_unit))

        try:

            start_val = 1

            for i in range(1, total_data + 1):
                pn = self.product_name_data[random.randrange(0, len(self.product_name_data))]
                pd = self.product_date_data[random.randrange(0, len(self.product_date_data))]
                product_date = datetime.strptime(pd, "%Y-%m-%d-%H-%M-%S")

                if table_name == UPDATE_TEST:
                    product_name = "1"
                else:
                    product_name = pn

                data_list.append({"PRODUCT_NAME": product_name, "PRODUCT_DATE": product_date, "SEPARATE_COL": start_val})

                if i % commit_unit == 0:
                    if destination == SOURCE:
                        self.src_engine.execute(src_table.__table__.insert(), data_list)
                    elif destination == TARGET:
                        self.trg_engine.execute(trg_table.__table__.insert(), data_list)
                    elif destination == BOTH:
                        self.src_engine.execute(src_table.__table__.insert(), data_list)
                        self.trg_engine.execute(trg_table.__table__.insert(), data_list)
                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1
                    data_list.clear()

            if total_data % commit_unit != 0:
                if destination == SOURCE:
                    self.src_engine.execute(src_table.__table__.insert(), data_list)
                elif destination == TARGET:
                    self.trg_engine.execute(trg_table.__table__.insert(), data_list)
                elif destination == BOTH:
                    self.src_engine.execute(src_table.__table__.insert(), data_list)
                    self.trg_engine.execute(trg_table.__table__.insert(), data_list)
                self.logger.debug(get_commit_msg(start_val))

            print("... Success\n")

            if destination == SOURCE:
                self.logger.info("Source's \"{}\" Table's data generation is completed".format(src_table.__tablename__))
            elif destination == TARGET:
                self.logger.info("Target's \"{}\" Table's data generation is completed".format(trg_table.__tablename__))
            elif destination == BOTH:
                self.logger.info("Source & Target's \"{}\" Table's data generation is completed".format(src_table.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. initializing_data is ended")

    # @staticmethod
    # def get_db_table(dbms_type, table_name):
    #
    #     if table_name == "UPDATE_TEST":
    #
    #         if dbms_type == ConfigManager.ORACLE:
    #             return oracle_mappers.UpdateTest
    #         elif dbms_type == ConfigManager.MYSQL:
    #             return mysql_mappers.UpdateTest
    #         elif dbms_type == ConfigManager.SQLSERVER:
    #             return sqlserver_mappers.UpdateTest
    #         elif dbms_type == ConfigManager.POSTGRESQL:
    #             return postgresql_mappers.UpdateTest
    #
    #     elif table_name == "DELETE_TEST":
    #
    #         if dbms_type == ConfigManager.ORACLE:
    #             return oracle_mappers.DeleteTest
    #         elif dbms_type == ConfigManager.MYSQL:
    #             return mysql_mappers.DeleteTest
    #         elif dbms_type == ConfigManager.SQLSERVER:
    #             return sqlserver_mappers.DeleteTest
    #         elif dbms_type == ConfigManager.POSTGRESQL:
    #             return postgresql_mappers.DeleteTest
