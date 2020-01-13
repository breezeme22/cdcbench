from src.constants import *
from src.funcs_common import get_object_name, print_complete_msg, exec_database_error
from src.funcs_datagen import get_file_data, get_separate_col_val, data_file_name
from src.mgr_logger import LoggerManager

from datetime import datetime
from sqlalchemy import inspect
from sqlalchemy.exc import SAWarning, DatabaseError
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint, UniqueConstraint, AddConstraint, DropConstraint
from tqdm import tqdm

import random
import warnings


class FuncsInitializer:

    __data_dir = "data"

    def __init__(self, **kwargs):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.dest_info = {}

        if "src_conn" in kwargs and "src_mapper" in kwargs:
            self.dest_info[SOURCE] = {}
            self.dest_info[SOURCE]["engine"] = kwargs["src_conn"].engine
            self.dest_info[SOURCE]["mapper"] = kwargs["src_mapper"]
            self.dest_info[SOURCE]["dbms_type"] = kwargs["src_conn"].connection_info["dbms_type"]
            self.dest_info[SOURCE]["schema_name"] = kwargs["src_conn"].connection_info["schema_name"]
            self.dest_info[SOURCE]["desc"] = "Source Database "

        if "trg_conn" in kwargs and "trg_mapper" in kwargs:
            self.dest_info[TARGET] = {}
            self.dest_info[TARGET]["engine"] = kwargs["trg_conn"].engine
            self.dest_info[TARGET]["mapper"] = kwargs["trg_mapper"]
            self.dest_info[TARGET]["dbms_type"] = kwargs["trg_conn"].connection_info["dbms_type"]
            self.dest_info[TARGET]["schema_name"] = kwargs["trg_conn"].connection_info["schema_name"]
            self.dest_info[TARGET]["desc"] = "Target Database "

    def create(self, dest, args):

        print("  Create Tables & Sequences ")

        try:

            def _drop_primary_key(dest, table):

                table_pks = []
                pk_name = table.primary_key.name
                table_pks.append(PrimaryKeyConstraint(name=pk_name))

                Table(table.name, self.dest_info[dest]["mapper"].metadata, *table_pks, extend_existing=True)

                DropConstraint(table_pks[0])

            def _add_unique_key(dest, table):

                table_uks = []
                table_cols = table.columns

                uk_name = f"{table.name}_UC" if table.name.isupper() else f"{table.name}_uc"

                table_uks.append(UniqueConstraint(Column(table_cols[table.columns.keys()[0]].name), name=uk_name))

                Table(table.name, self.dest_info[dest]["mapper"].metadata, *table_uks, extend_existing=True)

            def _run_create(dest):
                tables = self.dest_info[dest]["mapper"].metadata.sorted_tables
                print(f"    {self.dest_info[dest]['desc']}[{len(tables)}] ", end="", flush=True)
                for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                  desc=f"  {self.dest_info[dest]['desc']}"):
                    
                    if args.non_key:
                        _drop_primary_key(dest, table)
                    elif args.unique:
                        _drop_primary_key(dest, table)
                        
                        # dest가 BOTH로 들어올 경우 Source에서 _add_unique_key 함수를 호출하여 Mapper에 이미 Unique Key가 생성되어 있음
                        # 따라서 Mapper에 Unique Constraint가 이미 추가되어 있는지 검사하여 _add_unique_key 함수를 호출함
                        add_uk_flag = True
                        for constraint in list(table.constraints):
                            if isinstance(constraint, UniqueConstraint):
                                add_uk_flag = False
                        if add_uk_flag:
                            _add_unique_key(dest, table)
                    table.create(bind=self.dest_info[dest]["engine"], checkfirst=True)

            if dest == BOTH:
                _run_create(SOURCE)
                print_complete_msg(False, args.verbose, separate=False)

                _run_create(TARGET)
                print_complete_msg(False, args.verbose, "\n")

            else:
                _run_create(dest)
                print_complete_msg(False, args.verbose, "\n")

            self.logger.info("CDCBENCH's objects is created")

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    def drop(self, dest, args):

        print("  Drop Tables & Sequences")

        try:

            def _run_drop(dest):
                tables = self.dest_info[dest]["mapper"].metadata.sorted_tables
                print(f"    {self.dest_info[dest]['desc']}[{len(tables)}] ", end="", flush=True)
                for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                  desc=f"  {self.dest_info[dest]['desc']}"):
                    table.drop(bind=self.dest_info[dest]["engine"], checkfirst=True)

            if dest == BOTH:
                _run_drop(SOURCE)
                print_complete_msg(False, args.verbose, separate=False)

                _run_drop(TARGET)
                print_complete_msg(False, args.verbose, "\n")

            else:
                _run_drop(dest)
                print_complete_msg(False, args.verbose, "\n")

            self.logger.info("CDCBENCH's objects is dropped")

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

    # update_test & delete_test table initialize
    def initializing_data(self, dest, table_name, total_data, commit_unit, args):
        """
        update_test & delete_test table의 초기 데이터 생성 함수

        :param dest: initial 대상을 SOURCE / TARGET / BOTH 로 지정
        :param table_name: 어느 테이블에 데이터를 insert 할 것인지 지정.
        :param total_data: insert 할 데이터의 양을 지정.
        :param commit_unit: Commit 기준을 지정
        """

        print(f"  Generate {table_name} Table's data ")
        self.logger.info(f"Start \"{table_name}\" Table's data generation")

        self.logger.info(f"  Table Name      : {table_name}")
        self.logger.info(f"  Number of Count : {total_data}")
        self.logger.info(f"  Commit Unit     : {commit_unit}")

        file_data = get_file_data(data_file_name[table_name.split("_")[0]].upper())
        col_names = file_data["COL_NAME"]
        col_dates = file_data["COL_DATE"]

        try:

            def _get_initial_data(column_names, separate_col_val):

                col_name = col_names[random.randrange(len(col_names))] if table_name != UPDATE_TEST else '1'
                col_date = col_dates[random.randrange(len(col_dates))]

                return {
                    column_names[1]: col_name,
                    column_names[2]: col_date,
                    column_names[3]: separate_col_val
                }

            def _get_list_of_row_data(column_names, separate_col_val):

                desc_str = "Make Sample Data"
                print(f"    {desc_str} [{total_data}] ", end="", flush=True)

                list_of_row_data = []
                commit_unit_data = []
                separate_col_val = separate_col_val

                for u in tqdm(range(1, total_data + 1), disable=args.verbose, ncols=tqdm_ncols,
                              desc=f"  {desc_str} ", bar_format=tqdm_bar_format):
                    commit_unit_data.append(_get_initial_data(column_names, separate_col_val))
                    if u % commit_unit == 0:
                        list_of_row_data.append(commit_unit_data[:])
                        separate_col_val += 1
                        commit_unit_data.clear()

                if total_data % commit_unit != 0:
                    list_of_row_data.append(commit_unit_data)

                print_complete_msg(False, args.verbose, separate=False)

                return list_of_row_data

            def _run_insert_init_data(dest, list_of_row_data):

                print(f"    Insert {self.dest_info[dest]['desc']}[{total_data}] ", end="", flush=True)

                t = tqdm(total=total_data, disable=args.verbose, ncols=tqdm_ncols,
                         desc=f"  Insert {self.dest_info[dest]['desc']}", bar_format=tqdm_bar_format)

                for commit_unit_data in list_of_row_data:
                    self.dest_info[dest]["engine"].execute(self.dest_info[dest]["table"].insert(), commit_unit_data)
                    t.update(len(commit_unit_data))

                t.close()

            if dest == BOTH:

                self.dest_info[SOURCE]["table"] = self.dest_info[SOURCE]["mapper"].metadata.tables[
                    get_object_name(table_name, self.dest_info[SOURCE]["mapper"].metadata.tables.keys())
                ]

                self.dest_info[TARGET]["table"] = self.dest_info[TARGET]["mapper"].metadata.tables[
                    get_object_name(table_name, self.dest_info[TARGET]["mapper"].metadata.tables.keys())
                ]

                src_column_names = self.dest_info[SOURCE]["table"].columns.keys()
                trg_column_names = self.dest_info[TARGET]["table"].columns.keys()
                separate_col_val = get_separate_col_val(
                    self.dest_info[SOURCE]["engine"], self.dest_info[SOURCE]["table"], src_column_names[3]
                )

                list_of_row_data = _get_list_of_row_data(src_column_names, separate_col_val)

                _run_insert_init_data(SOURCE, list_of_row_data)
                print_complete_msg(False, args.verbose, separate=False)

                if self.dest_info[SOURCE]["dbms_type"] != self.dest_info[TARGET]["dbms_type"]:
                    for commit_unit_data in list_of_row_data:
                        for row_data in commit_unit_data:
                            for idx, column_name in enumerate(row_data):
                                row_data[trg_column_names[idx + 1]] = row_data.pop(column_name)

                _run_insert_init_data(TARGET, list_of_row_data)
                print_complete_msg(False, args.verbose, "\n")

            else:

                self.dest_info[dest]["table"] = self.dest_info[dest]["mapper"].metadata.tables[
                    get_object_name(table_name, self.dest_info[dest]["mapper"].metadata.tables.keys())
                ]

                column_names = self.dest_info[dest]["table"].columns.keys()[:]
                separate_col_val = get_separate_col_val(
                    self.dest_info[dest]["engine"], self.dest_info[dest]["table"], column_names[3]
                )

                list_of_row_data = _get_list_of_row_data(column_names, separate_col_val)

                _run_insert_init_data(dest, list_of_row_data)

                print_complete_msg(False, args.verbose, "\n")
                self.logger.info(f"{self.dest_info[dest]['desc']}'s \"{table_name}\" Table's "
                                 f"data generation is completed")

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)
