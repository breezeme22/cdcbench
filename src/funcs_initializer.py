from src.constants import *
from src.funcs_common import get_object_name, print_complete_msg, exec_database_error, get_separate_col_val, \
                             pyodbc_exec_database_error
from src.funcs_datamaker import data_file_name, FuncsDataMaker
from src.mgr_logger import LoggerManager

from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import Table, Column, PrimaryKeyConstraint, UniqueConstraint, DropConstraint
from tqdm import tqdm

import random
import re

import os
if os.name == "nt":
    import CUBRIDdb as cubrid
    import pyodbc


class FuncsInitializer:

    __data_dir = "data"

    def __init__(self, **kwargs):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.sql_logger = LoggerManager.get_sql_logger()
        self.log_level = LoggerManager.get_log_level()

        self.dest_info = {}

        if "src_conn" in kwargs and "src_mapper" in kwargs:
            self.dest_info[SOURCE] = {}
            self.dest_info[SOURCE]["conn"] = kwargs["src_conn"]
            self.dest_info[SOURCE]["engine"] = kwargs["src_conn"].engine
            self.dest_info[SOURCE]["mapper"] = kwargs["src_mapper"]
            self.dest_info[SOURCE]["dbms_type"] = kwargs["src_conn"].conn_info["dbms_type"]
            self.dest_info[SOURCE]["user_name"] = kwargs["src_conn"].conn_info["user_id"]
            self.dest_info[SOURCE]["desc"] = "Source Database "

        if "trg_conn" in kwargs and "trg_mapper" in kwargs:
            self.dest_info[TARGET] = {}
            self.dest_info[TARGET]["conn"] = kwargs["trg_conn"]
            self.dest_info[TARGET]["engine"] = kwargs["trg_conn"].engine
            self.dest_info[TARGET]["mapper"] = kwargs["trg_mapper"]
            self.dest_info[TARGET]["dbms_type"] = kwargs["trg_conn"].conn_info["dbms_type"]
            self.dest_info[TARGET]["user_name"] = kwargs["trg_conn"].conn_info["user_id"]
            self.dest_info[TARGET]["desc"] = "Target Database "

    def _table_check_sql(self, dest, table_name):
        """
        SA가 지원하지 않는 DBMS의 경우 SA와 유사하게 동작하기 위해 create/drop 수행 전에 table 존재 여부를 체크하며,
        DBMS에 따라 row level의 sql을 생성
        :param dest: dbms type을 얻기 위한 destination ( SOURCE or TARGET )
        :param table_name: DBMS에서 해당 테비을을 조회하기 위한 table_name (dbms에 따라 대소문자 주의)
        :return: Table Select SQL, binding 변수 (table_name, user_name)
        """

        if self.dest_info[dest]["dbms_type"] == CUBRID:
            table_check_sql = f"SELECT class_name FROM db_class WHERE class_name = ? AND owner_name = ?"
            bindings = (table_name.lower(), self.dest_info[dest]["user_name"].upper())
        else:
            table_check_sql = f"SELECT table_name FROM all_tables WHERE table_name = ? AND owner = ?"
            bindings = (table_name.upper(), self.dest_info[dest]["user_name"].upper())
        self.sql_logger.info(table_check_sql)
        self.sql_logger.info(bindings)

        return table_check_sql, bindings

    def create(self, dest, args):
        """
        Database에 Table 생성 작업을 수행하며, 필요한 내부 함수 및 출력 등을 포함
        :param dest: create를 수행할 Destination ( SOURCE or TARGET )
        :param args: initializer args 객체
        :return: None
        """

        print("  Create Tables & Sequences ")

        try:

            def _columns_nullable_set_false(columns):
                """
                SQLAlchemy에서 Column을 Primary Key로 설정할 경우 자동적으로 Not Null Constraint를 생성함.
                따라서 매개변수로 받은 컬럼의 nullable 값을 True로 변경하여 Not Null Constraint가 생성되지 않도록 함
                :param columns: Column Object
                :return: None
                """
                for column in columns:
                    column.nullable = True

            def _drop_primary_key(dest, table):
                """
                Mapper Class에 있는 Primary Key 속성을 제거
                :param dest: create() 수행 destination ( SOURCE or TARGET )
                :param table: Table Object
                :return: None
                """

                _columns_nullable_set_false(table.primary_key.columns)

                table_pks = []
                pk_name = table.primary_key.name
                table_pks.append(PrimaryKeyConstraint(name=pk_name))

                Table(table.name, self.dest_info[dest]["mapper"].metadata, *table_pks, extend_existing=True)

                DropConstraint(table_pks[0])

            def _add_unique_key(dest, table):
                """
                Mapper Class에 Unique Key 속성을 추가
                :param dest: create()를 수행할 dest ( SOURCE or TARGET )
                :param table: Table Object
                :return: None
                """

                table_uks = []
                table_cols = table.columns

                uk_name = f"{table.name}_UC" if table.name.isupper() else f"{table.name}_uc"

                table_uks.append(UniqueConstraint(Column(table_cols[table.columns.keys()[0]].name), name=uk_name))

                Table(table.name, self.dest_info[dest]["mapper"].metadata, *table_uks, extend_existing=True)

            def _run_create(dest):
                """
                SA가 지원하는 DBMS에 사용하며, 실제 DB 상에 Create 작업을 수행
                :param dest:
                :return:
                """

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
                    else:
                        _columns_nullable_set_false(table.primary_key.columns)

                    table.create(bind=self.dest_info[dest]["engine"], checkfirst=True)

            def _sa_unsupported_dbms_drop_primary_key(table_def):
                """
                SA가 미지원하는 DBMS의 경우 definition file에서 constraint 절을 match하여 공백으로 replace하는 방식으로 primary key를 제거
                :param table_def: definition file
                :return: constraint 절을 제거한 table definition
                """

                reg_expr = re.compile("CONSTRAINT.*?\)")
                re_sub_result = re.sub(reg_expr, "", table_def)
                last_comma = re_sub_result.rfind(",")
                last_rbracket = re_sub_result.rfind(")")
                pk_remove_result = list(re_sub_result)

                for i in range(1, last_rbracket - last_comma):
                    del pk_remove_result[last_comma]

                return "".join(pk_remove_result)
            
            def _sa_unsupported_dbms_add_unique_key(table_name, table_def):
                """
                SA가 미지원하지는 DBMS의 경우 definition file에서 "<Constraint Name> PRIMARY " 부분을 match하여 
                "<table_name>_UC(uc) UNIQUE "로 replace 하는 방식으로 unique key를 생성
                :param table_name: Table Name
                :param table_def: Table Definition String
                :return: 
                """
                
                reg_expr = re.compile("(?<=CONSTRAINT )+.*.+(?=\()")
                uk_name = f"{table_name}_UC" if table_name.isupper() else f"{table_name}_uc"
                add_uk_result = re.sub(reg_expr, f"{uk_name} UNIQUE ", table_def)

                return add_uk_result

            def _run_sa_unsupported_dbms_create(dest):
                """
                SA가 미지원하는 DBMS의 경우 Row Level SQL로 CREATE를 수행
                :param dest: Create를 수행할 Destination ( SOURCE or TARGET )
                :return: None
                """
                tables = self.dest_info[dest]["mapper"].tables
                conn = self.dest_info[dest]["conn"].sa_unsupported_get_connection()

                print(f"    {self.dest_info[dest]['desc']}[{len(tables)}] ", end="", flush=True)
                for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                  desc=f"  {self.dest_info[dest]['desc']}"):
                    cursor = conn.cursor()

                    table_check_sql, bindings = self._table_check_sql(dest, table)

                    cursor.execute(table_check_sql, bindings)

                    if cursor.fetchone() is None:

                        if args.non_key:
                            table_def = _sa_unsupported_dbms_drop_primary_key(tables[table])
                        elif args.unique:
                            table_def = _sa_unsupported_dbms_add_unique_key(table, tables[table])
                        else:
                            table_def = tables[table]
                        create_table_sql = f"\nCREATE TABLE {table_def}\n\n"

                        cursor.execute(create_table_sql)
                        self.sql_logger.info(create_table_sql)
                        self.sql_logger.info("COMMIT")

                    else:
                        continue

                    cursor.close()
                conn.close()

            if dest == BOTH:
                if self.dest_info[SOURCE]["dbms_type"] in sa_unsupported_dbms:
                    _run_sa_unsupported_dbms_create(SOURCE)
                else:
                    _run_create(SOURCE)
                print_complete_msg(False, args.verbose, separate=False)

                if self.dest_info[TARGET]["dbms_type"] in sa_unsupported_dbms:
                    _run_sa_unsupported_dbms_create(TARGET)
                else:
                    _run_create(TARGET)
                print_complete_msg(False, args.verbose, "\n")

            else:
                if self.dest_info[dest]["dbms_type"] in sa_unsupported_dbms:
                    _run_sa_unsupported_dbms_create(dest)
                else:
                    _run_create(dest)
                print_complete_msg(False, args.verbose, "\n")

            self.logger.info("CDCBENCH's objects is created")

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

        except cubrid.DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

        except pyodbc.DatabaseError as dberr:
            pyodbc_exec_database_error(self.logger, self.log_level, dberr)

    def drop(self, dest, args):
        """
        Database의 CDCBENCH Table을 삭제
        :param dest:
        :param args:
        :return:
        """

        print("  Drop Tables & Sequences")

        try:

            def _run_drop(dest):
                tables = self.dest_info[dest]["mapper"].metadata.sorted_tables
                print(f"    {self.dest_info[dest]['desc']}[{len(tables)}] ", end="", flush=True)
                for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                  desc=f"  {self.dest_info[dest]['desc']}"):
                    table.drop(bind=self.dest_info[dest]["engine"], checkfirst=True)

            def _run_sa_unsupported_dbms_drop(dest):
                tables = self.dest_info[dest]["mapper"].tables
                conn = self.dest_info[dest]["conn"].sa_unsupported_get_connection()

                print(f"    {self.dest_info[dest]['desc']}[{len(tables)}] ", end="", flush=True)
                for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                                  desc=f"  {self.dest_info[dest]['desc']}"):
                    cursor = conn.cursor()

                    table_check_sql, bindings = self._table_check_sql(dest, table)

                    cursor.execute(table_check_sql, bindings)

                    if cursor.fetchone() is not None:
                        drop_table_sql = f"\nDROP TABLE {table.upper()}"
                        cursor.execute(drop_table_sql)
                        self.sql_logger.info(drop_table_sql)
                        self.sql_logger.info("COMMIT")

                    else:
                        continue

                    cursor.close()
                conn.close()

            if dest == BOTH:
                if self.dest_info[SOURCE]["dbms_type"] in sa_unsupported_dbms:
                    _run_sa_unsupported_dbms_drop(SOURCE)
                else:
                    _run_drop(SOURCE)
                print_complete_msg(False, args.verbose, separate=False)

                if self.dest_info[TARGET]["dbms_type"] in sa_unsupported_dbms:
                    _run_sa_unsupported_dbms_drop(TARGET)
                else:
                    _run_drop(TARGET)
                print_complete_msg(False, args.verbose, "\n")

            else:
                if self.dest_info[dest]["dbms_type"] in sa_unsupported_dbms:
                    _run_sa_unsupported_dbms_drop(dest)
                else:
                    _run_drop(dest)
                print_complete_msg(False, args.verbose, "\n")

            self.logger.info("CDCBENCH's objects is dropped")

        except DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

        except cubrid.DatabaseError as dberr:
            exec_database_error(self.logger, self.log_level, dberr)

        except pyodbc.DatabaseError as dberr:
            pyodbc_exec_database_error(self.logger, self.log_level, dberr)

    # update_test & delete_test table initialize
    def initializing_data(self, dest, table_name, total_data, commit_unit, args):
        """
        update_test & delete_test table의 초기 데이터 생성

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

        file_data = FuncsDataMaker(data_file_name[table_name.split("_")[0]].upper()).get_file_data()
        col_names = file_data["COL_NAME"]
        col_dates = file_data["COL_DATE"]

        try:

            def _get_initial_data(column_names, separate_col_val):
                """
                Random Row Data를 생성
                :param column_names: Table Column Names
                :param separate_col_val: Row의 SEPARATE_COL 컬럼 값
                :return: Row Data
                """

                col_name = col_names[random.randrange(len(col_names))] if table_name != UPDATE_TEST else '1'
                col_date = col_dates[random.randrange(len(col_dates))]

                return {
                    column_names[1]: col_name,
                    column_names[2]: col_date,
                    column_names[3]: separate_col_val
                }

            def _get_list_of_row_data(column_names, separate_col_val):
                """
                Insert할 전체 데이터를 생성
                :param column_names: Table Column Names
                :param separate_col_val: SEPARATE_COL 컬럼 시작값
                :return: Insert할 전체 데이터 List
                """
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
