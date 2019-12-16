from commons.constants import *
from commons.funcs_common import get_object_name, print_complete_msg
from commons.funcs_datagen import get_file_data, get_separate_col_val, get_sample_table_data, data_file_name
from commons.mgr_logger import LoggerManager

from sqlalchemy import inspect
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint, UniqueConstraint, AddConstraint, DropConstraint
from sqlalchemy.exc import SAWarning, DatabaseError, CompileError, InvalidRequestError
from datetime import datetime
from tqdm import tqdm

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

    def create(self, destination, args):

        self.logger.debug("Func. create is started")

        try:

            if args.non_key:
                print("  Create Tables & Sequences (including drop the primary key)")
            elif args.unique:
                print("  Create Tables & Sequences (including drop the primary key and add unique constraints)")
            else:
                print("  Create Tables & Sequences ")

            verbose = {"flag": args.verbose}

            if destination == SOURCE:

                print(_get_src_side_msg(), end="", flush=True)
                for table in tqdm(self.src_mapper.metadata.sorted_tables, disable=verbose["flag"], ncols=70, desc=_get_src_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.create(bind=self.src_engine, checkfirst=True)

                    if args.non_key:
                        self._drop_primary_key(self.src_engine, self.src_mapper, table.name, self.source_schema_name)
                    elif args.unique:
                        self._drop_primary_key(self.src_engine, self.src_mapper, table.name, self.source_schema_name)
                        self._add_unique_constraint(self.src_engine, self.src_mapper, table.name, self.source_schema_name)

                print_complete_msg(args.verbose, "\n")

            elif destination == TARGET:
                print(_get_trg_side_msg(), end="", flush=True)
                for table in tqdm(self.trg_mapper.metadata.sorted_tables, disable=args.verbose, ncols=80, desc=_get_trg_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.create(bind=self.trg_engine, checkfirst=True)

                    if args.non_key:
                        self._drop_primary_key(self.trg_engine, self.trg_mapper, table.name, self.target_schema_name)
                    elif args.unique:
                        self._drop_primary_key(self.trg_engine, self.trg_mapper, table.name, self.target_schema_name)
                        self._add_unique_constraint(self.trg_engine, self.trg_mapper, table.name, self.target_schema_name)

                print_complete_msg(args.verbose, "\n")

            elif destination == BOTH:
                print(_get_src_side_msg(), end="", flush=True)
                for table in tqdm(self.src_mapper.metadata.sorted_tables, disable=args.verbose, ncols=80, desc=_get_src_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.create(bind=self.src_engine, checkfirst=True)

                    if args.non_key:
                        self._drop_primary_key(self.src_engine, self.src_mapper, table.name, self.source_schema_name)
                    elif args.unique:
                        self._drop_primary_key(self.src_engine, self.src_mapper, table.name, self.source_schema_name)
                        self._add_unique_constraint(self.src_engine, self.src_mapper, table.name, self.source_schema_name)

                print_complete_msg(args.verbose, separate=False)

                print(_get_trg_side_msg(), end="", flush=True)
                for table in tqdm(self.trg_mapper.metadata.sorted_tables, disable=args.verbose, ncols=80, desc=_get_trg_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.create(bind=self.trg_engine, checkfirst=True)

                    if args.non_key:
                        self._drop_primary_key(self.trg_engine, self.trg_mapper, table.name, self.target_schema_name)
                    elif args.unique:
                        self._drop_primary_key(self.trg_engine, self.trg_mapper, table.name, self.target_schema_name)
                        self._add_unique_constraint(self.trg_engine, self.trg_mapper, table.name, self.target_schema_name)

                print_complete_msg(args.verbose, "\n")

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

    def _drop_primary_key(self, engine, mapper, table_name, schema_name):

        inspector = inspect(engine)

        self.logger.debug("Gets the Primary key information for table in the database")

        table_pks = []
        pk_name = inspector.get_pk_constraint(table_name, schema=schema_name)["name"]
        self.logger.debug(pk_name)
        table_pks.append(PrimaryKeyConstraint(name=pk_name))

        tab = Table(table_name, mapper.metadata, *table_pks, extend_existing=True)

        self.logger.info("Drop the primary key to table")
        engine.execute(DropConstraint(table_pks[0]))

    def _add_unique_constraint(self, engine, mapper, table_name, schema_name):

        inspector = inspect(engine)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=SAWarning)

            table_ucs = []
            table_cols = inspector.get_columns(table_name, schema=schema_name)

            if table_name.isupper():
                uc_name = "{}_UC".format(table_name)
            else:
                uc_name = "{}_uc".format(table_name)

            table_ucs.append(UniqueConstraint(Column(table_cols[0]["name"]),
                                              name="{}".format(uc_name)))

            tab = Table(table_name, mapper.metadata, *table_ucs, extend_existing=True)

            self.logger.info("Create a unique constraint to table")
            engine.execute(AddConstraint(table_ucs[0]))

    def drop(self, destination, args):

        try:
            print("  Drop Tables & Sequences")

            if destination == SOURCE:
                print(_get_src_side_msg(), end="", flush=True)
                for table in tqdm(self.src_mapper.metadata.sorted_tables, disable=args.verbose, ncols=70, desc=_get_src_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.drop(bind=self.src_engine, checkfirst=True)
                print_complete_msg(args.verbose, "\n")
            elif destination == TARGET:
                print(_get_trg_side_msg(), end="", flush=True)
                for table in tqdm(self.trg_mapper.metadata.sorted_tables, disable=args.verbose, ncols=80, desc=_get_trg_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.drop(bind=self.trg_engine, checkfirst=True)
                print_complete_msg(args.verbose, "\n")
            elif destination == BOTH:
                print(_get_src_side_msg(), end="", flush=True)
                for table in tqdm(self.src_mapper.metadata.sorted_tables, disable=args.verbose, ncols=80, desc=_get_src_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.drop(bind=self.src_engine, checkfirst=True)
                print_complete_msg(args.verbose, separate=False)

                print(_get_trg_side_msg(), end="", flush=True)
                for table in tqdm(self.trg_mapper.metadata.sorted_tables, disable=args.verbose, ncols=80, desc=_get_trg_side_msg(),
                                  bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
                    table.drop(bind=self.trg_engine, checkfirst=True)
                print_complete_msg(args.verbose, "\n")

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
    @staticmethod
    def _run_initial_data_insert(engine, mapper, table_name, total_data, commit_unit, verbose):

        real_table_name = get_object_name(mapper.metadata.tables.keys(), table_name)
        table = mapper.metadata.tables[real_table_name]
        column_names = table.columns.keys()[:]
        separate_col_val = get_separate_col_val(engine, table, column_names[3])

        file_data = get_file_data(data_file_name[table_name.split("_")[0]].upper())
        list_of_row_data = []

        # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거
        column_names.remove(column_names[0])

        for i in tqdm(range(1, total_data + 1), disable=verbose["flag"], ncols=70, desc=verbose["desc"],
                      bar_format="{desc}[{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"):
            list_of_row_data.append(get_sample_table_data(file_data, table_name, column_names, separate_col_val))

            if i % commit_unit == 0:
                engine.execute(table.insert(), list_of_row_data)
                separate_col_val += 1
                list_of_row_data.clear()

        if total_data % commit_unit != 0:
            engine.execute(table.insert(), list_of_row_data)

    # update_test & delete_test table initialize
    def initializing_data(self, destination, table_name, total_data, commit_unit, args):
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

        verbose = {"flag": args.verbose}

        try:

            if destination == SOURCE:
                print(_get_src_side_msg(), end="", flush=True)
                verbose["desc"] = _get_src_side_msg()
                self._run_initial_data_insert(self.src_engine, self.src_mapper, table_name, total_data, commit_unit, verbose)
                print_complete_msg(args.verbose, "\n")
                self.logger.info("Source's \"{}\" Table's data generation is completed".format(table_name))

            elif destination == TARGET:
                print(_get_trg_side_msg(), end="", flush=True)
                verbose["desc"] = _get_trg_side_msg()
                self._run_initial_data_insert(self.trg_engine, self.trg_mapper, table_name, total_data, commit_unit, verbose)
                print_complete_msg(args.verbose, "\n")
                self.logger.info("Target's \"{}\" Table's data generation is completed".format(table_name))

            elif destination == BOTH:
                print(_get_src_side_msg(), end="", flush=True)
                verbose["desc"] = _get_src_side_msg()
                self._run_initial_data_insert(self.src_engine, self.src_mapper, table_name, total_data, commit_unit, verbose)
                print_complete_msg(args.verbose, separate=False)
                self.logger.info("Source's \"{}\" Table's data generation is completed".format(table_name))

                print(_get_trg_side_msg(), end="", flush=True)
                verbose["desc"] = _get_trg_side_msg()
                self._run_initial_data_insert(self.trg_engine, self.trg_mapper, table_name, total_data, commit_unit, verbose)
                print_complete_msg(args.verbose, end="\n")
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


def _get_src_side_msg():
    return "    Source Database "


def _get_trg_side_msg():
    return "    Target Database "


