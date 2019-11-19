from commons.constants import *
from commons.funcs_common import get_commit_msg, get_json_data
from commons.mgr_config import ConfigManager
from commons.mgr_connection import ConnectionManager
from commons.mgr_logger import LoggerManager

from sqlalchemy import inspect
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint, UniqueConstraint, AddConstraint, DropConstraint
from sqlalchemy.exc import SAWarning, DatabaseError, CompileError, InvalidRequestError
from datetime import datetime

import random
import os
import warnings
import logging


class InitialFunctions:

    __data_dir = "data"

    def __init__(self):

        self.config = ConfigManager.CONFIG
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()
        self.src_engine = conn_mgr.src_engine
        self.trg_engine = conn_mgr.trg_engine
        self.src_mapper = conn_mgr.get_src_mapper()
        self.trg_mapper = conn_mgr.get_trg_mapper()

        if self.config.source_dbms_type == dialect_driver[SQLSERVER] or \
                self.config.source_dbms_type == dialect_driver[POSTGRESQL]:
            for src_table in self.src_mapper.metadata.sorted_tables:
                src_table.schema = self.config.source_schema_name

        if self.config.target_dbms_type == dialect_driver[SQLSERVER] or \
                self.config.target_dbms_type == dialect_driver[POSTGRESQL]:
            for trg_table in self.trg_mapper.metadata.sorted_tables:
                trg_table.schema = self.config.target_schema_name

    def create(self, destination):

        self.logger.debug("Func. create is started")

        try:
            print("  Create CDCBENCH's objects ", end="", flush=True)

            if destination == SOURCE:
                if self.config.source_dbms_type == dialect_driver[SQLSERVER]:
                    for table in self.src_mapper.metadata.sorted_tables:
                        table.create(bind=self.src_engine)
                else:
                    self.src_mapper.metadata.create_all(bind=self.src_engine)
            elif destination == TARGET:
                if self.config.target_dbms_type == dialect_driver[SQLSERVER]:
                    for table in self.trg_mapper.metadata.sorted_tables:
                        table.create(bind=self.trg_engine)
                else:
                    self.trg_mapper.metadata.create_all(bind=self.trg_engine)
            elif destination == BOTH:
                if self.config.source_dbms_type == dialect_driver[SQLSERVER]:
                    for table in self.src_mapper.metadata.sorted_tables:
                        table.create(bind=self.src_engine)
                else:
                    self.src_mapper.metadata.create_all(bind=self.src_engine)

                if self.config.target_dbms_type == dialect_driver[SQLSERVER]:
                    for table in self.trg_mapper.metadata.sorted_tables:
                        table.create(bind=self.trg_engine)
                else:
                    self.trg_mapper.metadata.create_all(bind=self.trg_engine)

            print("... Success\n")
            self.logger.info("CDCBENCH's objects is created")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
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
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. drop is ended")

    # update_test & delete_test table initialize
    def initializing_data(self, destination, table_name, total_data, commit_unit):
        """
        update_test & delete_test table의 초기 데이터 생성 함수
        
        :param destination: initial 대상을 SOURCE / TARGET / BOTH 로 지정
        :param table_name: 어느 테이블에 데이터를 insert 할 것인지 지정.
        :param total_data: insert 할 데이터의 양을 지정.
        :param commit_unit: Commit 기준을 지정
        """

        self.logger.debug("Func. initializing_data is started")

        file_name = "dml.dat"
        bench_data = get_json_data(os.path.join(self.__data_dir, file_name))
        list_of_product_name = bench_data.get("PRODUCT_NAME")
        list_of_product_date = bench_data.get("PRODUCT_DATE")
        self.logger.debug("Load data file ({})".format(file_name))

        print("  Generate {} Table's data ".format(table_name), end="", flush=True)
        self.logger.info("Start \"{}\" Table's data generation".format(table_name))

        self.logger.info("  Table Name      : " + table_name)
        self.logger.info("  Number of Count : " + str(total_data))
        self.logger.info("  Commit Unit     : " + str(commit_unit))

        if self.config.source_dbms_type == dialect_driver[POSTGRESQL]:
            src_convert_table_name = table_name.lower()
            trg_convert_table_name = table_name
        elif self.config.target_dbms_type == dialect_driver[POSTGRESQL]:
            src_convert_table_name = table_name
            trg_convert_table_name = table_name.lower()
        else:
            src_convert_table_name = table_name
            trg_convert_table_name = table_name

        src_table = self.src_mapper.metadata.tables[src_convert_table_name]
        trg_table = self.trg_mapper.metadata.tables[trg_convert_table_name]

        start_val = 1
        # list_of_row_data = []
        src_list_of_row_data = []
        trg_list_of_row_data = []

        src_column_names = src_table.columns.keys()[:]
        trg_column_names = trg_table.columns.keys()[:]

        try:

            for i in range(1, total_data + 1):

                random_pn = list_of_product_name[random.randrange(0, len(list_of_product_name))]
                random_pd = list_of_product_date[random.randrange(0, len(list_of_product_date))]
                formatted_pd = datetime.strptime(random_pd, "%Y-%m-%d %H:%M:%S")

                if table_name == UPDATE_TEST:
                    product_name = "1"
                else:
                    product_name = random_pn

                src_list_of_row_data.append({
                    src_column_names[1]: product_name,
                    src_column_names[2]: formatted_pd,
                    src_column_names[3]: start_val
                })

                trg_list_of_row_data.append({
                    trg_column_names[1]: product_name,
                    trg_column_names[2]: formatted_pd,
                    trg_column_names[3]: start_val
                })

                if i % commit_unit == 0:
                    if destination == SOURCE:
                        self.src_engine.execute(src_table.insert(), src_list_of_row_data)
                    elif destination == TARGET:
                        self.trg_engine.execute(trg_table.insert(), trg_list_of_row_data)
                    elif destination == BOTH:
                        self.src_engine.execute(src_table.insert(), src_list_of_row_data)
                        self.trg_engine.execute(trg_table.insert(), trg_list_of_row_data)

                    self.logger.debug(get_commit_msg(start_val))
                    start_val += 1
                    src_list_of_row_data.clear()
                    trg_list_of_row_data.clear()

            if total_data % commit_unit != 0:
                if destination == SOURCE:
                    self.src_engine.execute(src_table.insert(), src_list_of_row_data)
                elif destination == TARGET:
                    self.trg_engine.execute(trg_table.insert(), trg_list_of_row_data)
                elif destination == BOTH:
                    self.src_engine.execute(src_table.insert(), src_list_of_row_data)
                    self.trg_engine.execute(trg_table.insert(), trg_list_of_row_data)
                self.logger.debug(get_commit_msg(start_val))

            print("... Success\n")

            if destination == SOURCE:
                self.logger.info("Source's \"{}\" Table's data generation is completed".format(src_table))
            elif destination == TARGET:
                self.logger.info("Target's \"{}\" Table's data generation is completed".format(trg_table))
            elif destination == BOTH:
                self.logger.info("Source & Target's \"{}\" Table's data generation is completed".format(src_table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. initializing_data is ended")

    def run_drop_primary_key(self, engine, mapper, schema_name):

        inspector = inspect(engine)

        all_tables = []
        all_pks = []

        self.logger.info("Gets the Primary key information for each table in the database")
        # for table_name in inspector.get_table_names(schema=schema_name):
        for table_name in mapper.metadata.tables.keys():
            table_pks = []
            pk_name = inspector.get_pk_constraint(table_name, schema=schema_name)["name"]
            table_pks.append(PrimaryKeyConstraint(name=pk_name))

            tab = Table(table_name, mapper.metadata, *table_pks, extend_existing=True)

            all_tables.append(tab)
            all_pks.extend(table_pks)

        self.logger.info("Drop the primary key to each table")
        for pk in all_pks:
            engine.execute(DropConstraint(pk))

    # Drop Primary Keys
    def drop_primary_keys(self, destination):

        self.logger.debug("Func. drop_primary_keys is started")

        try:

            print("    Drop each table's primary key ", end="", flush=True)

            if destination == SOURCE:
                self.logger.info("Start SOURCE side jobs")
                self.run_drop_primary_key(self.src_engine, self.src_mapper, self.config.source_schema_name)
            elif destination == TARGET:
                self.logger.info("Start TARGET side jobs")
                self.run_drop_primary_key(self.trg_engine, self.trg_mapper, self.config.target_schema_name)
            elif destination == BOTH:
                self.logger.info("Start SOURCE side jobs")
                self.run_drop_primary_key(self.src_engine, self.src_mapper, self.config.source_schema_name)

                self.logger.info("Start TARGET side jobs")
                self.run_drop_primary_key(self.trg_engine, self.trg_mapper, self.config.target_schema_name)

            print("... Success\n")
            self.logger.info("The primary key for each table is dropped")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        except CompileError as comperr:
            print("... Fail")
            self.logger.error(comperr.args[0])
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(comperr.args[0])
            raise

        except InvalidRequestError as irerr:
            print("... Fail")
            self.logger.error(irerr.args[0])
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(irerr.args[0])
            raise

        finally:
            self.logger.debug("Func. drop_primary_keys is ended")

    def run_add_unique_constraint(self, engine, mapper, schema_name):

        inspector = inspect(engine)

        all_tables = []
        all_ucs = []

        self.logger.info("Gets the column information for each table in the database")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=SAWarning)

            for table_name in mapper.metadata.tables.keys():
                table_ucs = []
                table_cols = inspector.get_columns(table_name, schema=schema_name)

                if table_name.isupper():
                    uc_name = "{}_UC".format(table_name)
                else:
                    uc_name = "{}_uc".format(table_name)

                table_ucs.append(UniqueConstraint(Column(table_cols[0]["name"]),
                                                  name="{}".format(uc_name)))

                tab = Table(table_name, mapper.metadata, *table_ucs, extend_existing=True)

                all_tables.append(tab)
                all_ucs.extend(table_ucs)

        self.logger.info("Create a unique constraint to each table")
        for uc in all_ucs:
            engine.execute(AddConstraint(uc))

    # Create Unique Constraints
    def add_unqiue_constraints(self, destination):

        self.logger.debug("Func. add_unique_constraints is started")

        try:

            print("    Create each table's Unique Constraint ", end="", flush=True)

            if destination == SOURCE:
                self.logger.info("Start SOURCE side jobs.")
                self.run_add_unique_constraint(self.src_engine, self.src_mapper, self.config.source_schema_name)
            elif destination == TARGET:
                self.logger.info("Start TARGET side jobs")
                self.run_add_unique_constraint(self.trg_engine, self.trg_mapper, self.config.target_schema_name)
            elif destination == BOTH:
                self.logger.info("Start SOURCE side jobs")
                self.run_add_unique_constraint(self.src_engine, self.src_mapper, self.config.source_schema_name)

                self.logger.info("Start TARGET side jobs.")
                self.run_add_unique_constraint(self.trg_engine, self.trg_mapper, self.config.target_schema_name)

            print("... Success\n")
            self.logger.info("A unique constraint for each table is added")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        except CompileError as comperr:
            print("... Fail")
            self.logger.erorr(comperr.args[0])
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(comperr.args[0])
            raise

        except InvalidRequestError as irerr:
            print("... Fail")
            self.logger.error(irerr.args[0])
            if self.config.log_level == logging.DEBUG:
                self.logger.exception(irerr.args[0])
            raise

        finally:
            self.logger.debug("Func. add_unique_constraints is ended")
