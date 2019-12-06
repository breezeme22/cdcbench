from commons.constants import *
from commons.funcs_common import get_object_name
from commons.funcs_datagen import get_file_data, get_separate_col_val, get_sample_table_data, data_file_name
from commons.mgr_logger import LoggerManager

from sqlalchemy import inspect
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint, UniqueConstraint, AddConstraint, DropConstraint
from sqlalchemy.exc import SAWarning, DatabaseError, CompileError, InvalidRequestError
from datetime import datetime

import warnings
import logging


class FuncsInitializer:

    __data_dir = "data"

    def __init__(self, **kwargs):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        if "src_conn" in kwargs and "src_mapper" in kwargs:
            self.src_engine = kwargs["src_conn"].engine
            self.src_mapper = kwargs["src_mapper"]
            self.source_dbms_type = kwargs["src_conn"].connection_info["dbms_type"]
            self.source_schema_name = kwargs["src_conn"].connection_info["schema_name"]

        if "trg_conn" in kwargs and "trg_mapper" in kwargs:
            self.trg_engine = kwargs["trg_conn"].engine
            self.trg_mapper = kwargs["trg_mapper"]
            self.target_dbms_type = kwargs["trg_conn"].connection_info["dbms_type"]
            self.target_schema_name = kwargs["trg_conn"].connection_info["schema_name"]

    def create(self, destination):

        self.logger.debug("Func. create is started")

        try:
            # print("  Create CDCBENCH's objects ", end="", flush=True)
            print("  Create Tables & Sequences ")

            if destination == SOURCE:
                print(_get_src_side_msg(), end="", flush=True)
                if self.source_dbms_type == SQLSERVER:
                    for table in self.src_mapper.metadata.sorted_tables:
                        table.create(bind=self.src_engine)
                else:
                    self.src_mapper.metadata.create_all(bind=self.src_engine)
                print("... Complete\n")
            elif destination == TARGET:
                print(_get_trg_side_msg(), end="", flush=True)
                if self.target_dbms_type == SQLSERVER:
                    for table in self.trg_mapper.metadata.sorted_tables:
                        table.create(bind=self.trg_engine)
                else:
                    self.trg_mapper.metadata.create_all(bind=self.trg_engine)
                print("... Complete\n")
            elif destination == BOTH:
                print(_get_src_side_msg(), end="", flush=True)
                if self.source_dbms_type == SQLSERVER:
                    for table in self.src_mapper.metadata.sorted_tables:
                        table.create(bind=self.src_engine)
                else:
                    self.src_mapper.metadata.create_all(bind=self.src_engine)
                print("... Complete")

                print(_get_trg_side_msg(), end="", flush=True)
                if self.target_dbms_type == SQLSERVER:
                    for table in self.trg_mapper.metadata.sorted_tables:
                        table.create(bind=self.trg_engine)
                else:
                    self.trg_mapper.metadata.create_all(bind=self.trg_engine)
                print("... Complete\n")

            self.logger.info("CDCBENCH's objects is created")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. create is ended")

    def drop(self, destination):

        try:
            print("  Drop Tables & Sequences")

            if destination == SOURCE:
                print(_get_src_side_msg(), end="", flush=True)
                self.src_mapper.metadata.drop_all(bind=self.src_engine)
                print("... Complete\n")
            elif destination == TARGET:
                print(_get_trg_side_msg(), end="", flush=True)
                self.trg_mapper.metadata.drop_all(bind=self.trg_engine)
                print("... Complete\n")
            elif destination == BOTH:
                print(_get_src_side_msg(), end="", flush=True)
                self.src_mapper.metadata.drop_all(bind=self.src_engine)
                print("... Complete")
                print(_get_trg_side_msg(), end="", flush=True)
                self.trg_mapper.metadata.drop_all(bind=self.trg_engine)
                print("... Complete\n")

            self.logger.info("CDCBENCH's objects is dropped")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. drop is ended")

    # initial data insert
    def _run_initial_data_insert(self, engine, mapper, table_name, total_data, commit_unit):

        real_table_name = get_object_name(mapper.metadata.tables.keys(), table_name)
        table = mapper.metadata.tables[real_table_name]
        column_names = table.columns.keys()[:]
        separate_col_val = get_separate_col_val(engine, table, column_names[3])

        file_data = get_file_data(data_file_name[table_name.split("_")[0]].upper())
        list_of_row_data = []

        # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거
        column_names.remove(column_names[0])

        for i in range(1, total_data + 1):

            list_of_row_data.append(get_sample_table_data(file_data, table_name, column_names, separate_col_val))

            if i % commit_unit == 0:
                engine.execute(table.insert(), list_of_row_data)
                separate_col_val += 1
                list_of_row_data.clear()

        if total_data % commit_unit != 0:
            engine.execute(table.insert(), list_of_row_data)

    # update_test & delete_test table initialize
    def initializing_data(self, destination, table_name, total_data, commit_unit):
        """
        update_test & delete_test table의 초기 데이터 생성 함수

        :param destination: initial 대상을 SOURCE / TARGET / BOTH 로 지정
        :param table_name: 어느 테이블에 데이터를 insert 할 것인지 지정.
        :param total_data: insert 할 데이터의 양을 지정.
        :param commit_unit: Commit 기준을 지정
        """

        print("  Generate {} Table's data ".format(table_name))
        self.logger.info("Start \"{}\" Table's data generation".format(table_name))

        self.logger.info("  Table Name      : " + table_name)
        self.logger.info("  Number of Count : " + str(total_data))
        self.logger.info("  Commit Unit     : " + str(commit_unit))

        try:

            if destination == SOURCE:
                print(_get_src_side_msg(), end="", flush=True)
                self._run_initial_data_insert(self.src_engine, self.src_mapper, table_name, total_data, commit_unit)
                print("... Complete\n")
                self.logger.info("Source's \"{}\" Table's data generation is completed".format(table_name))

            elif destination == TARGET:

                print(_get_trg_side_msg(), end="", flush=True)
                self._run_initial_data_insert(self.trg_engine, self.trg_mapper, table_name, total_data, commit_unit)
                print("... Complete\n")
                self.logger.info("Target's \"{}\" Table's data generation is completed".format(table_name))

            elif destination == BOTH:
                print(_get_src_side_msg(), end="", flush=True)
                self._run_initial_data_insert(self.src_engine, self.src_mapper, table_name, total_data, commit_unit)
                print("... Complete")
                self.logger.info("Source's \"{}\" Table's data generation is completed".format(table_name))

                print(_get_trg_side_msg(), end="", flush=True)
                self._run_initial_data_insert(self.trg_engine, self.trg_mapper, table_name, total_data, commit_unit)
                print("... Complete\n")
                self.logger.info("Target's \"{}\" Table's data generation is completed".format(table_name))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func. initializing_data is ended")

    def _run_drop_primary_key(self, engine, mapper, schema_name):

        inspector = inspect(engine)

        all_tables = []
        all_pks = []

        self.logger.info("Gets the Primary key information for each table in the database")
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
                self._run_drop_primary_key(self.src_engine, self.src_mapper, self.source_schema_name)
            elif destination == TARGET:
                self.logger.info("Start TARGET side jobs")
                self._run_drop_primary_key(self.trg_engine, self.trg_mapper, self.target_schema_name)
            elif destination == BOTH:
                self.logger.info("Start SOURCE side jobs")
                self._run_drop_primary_key(self.src_engine, self.src_mapper, self.source_schema_name)
                self.logger.info("Start TARGET side jobs")
                self._run_drop_primary_key(self.trg_engine, self.trg_mapper, self.target_schema_name)

            print("... Success\n")
            self.logger.info("The primary key for each table is dropped")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        except CompileError as comperr:
            print("... Fail")
            self.logger.error(comperr.args[0])
            if self.log_level == logging.DEBUG:
                self.logger.exception(comperr.args[0])
            raise

        except InvalidRequestError as irerr:
            print("... Fail")
            self.logger.error(irerr.args[0])
            if self.log_level == logging.DEBUG:
                self.logger.exception(irerr.args[0])
            raise

        finally:
            self.logger.debug("Func. drop_primary_keys is ended")

    def _run_add_unique_constraint(self, engine, mapper, schema_name):

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
    def add_unique_constraints(self, destination):

        self.logger.debug("Func. add_unique_constraints is started")

        try:

            print("    Create each table's Unique Constraint ", end="", flush=True)

            if destination == SOURCE:
                self.logger.info("Start SOURCE side jobs.")
                self._run_add_unique_constraint(self.src_engine, self.src_mapper, self.source_schema_name)
            elif destination == TARGET:
                self.logger.info("Start TARGET side jobs")
                self._run_add_unique_constraint(self.trg_engine, self.trg_mapper, self.target_schema_name)
            elif destination == BOTH:
                self.logger.info("Start SOURCE side jobs")
                self._run_add_unique_constraint(self.src_engine, self.src_mapper, self.source_schema_name)

                self.logger.info("Start TARGET side jobs.")
                self._run_add_unique_constraint(self.trg_engine, self.trg_mapper, self.target_schema_name)

            print("... Success\n")
            self.logger.info("A unique constraint for each table is added")

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        except CompileError as comperr:
            print("... Fail")
            self.logger.erorr(comperr.args[0])
            if self.log_level == logging.DEBUG:
                self.logger.exception(comperr.args[0])
            raise

        except InvalidRequestError as irerr:
            print("... Fail")
            self.logger.error(irerr.args[0])
            if self.log_level == logging.DEBUG:
                self.logger.exception(irerr.args[0])
            raise

        finally:
            self.logger.debug("Func. add_unique_constraints is ended")


def _get_src_side_msg():
    return "    Source Database "


def _get_trg_side_msg():
    return "    Target Database "
