
import argparse
import logging
import jaydebeapi
import jpype
import random
import re

from typing import List, Dict, NoReturn

from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import Table, PrimaryKeyConstraint, UniqueConstraint, DropConstraint
from tqdm import tqdm

from lib.globals import *
from lib.common import proc_database_error, DatabaseMetaData
from lib.config import InitialDataConfig


def create_objects(db_meta: DatabaseMetaData, args: argparse.Namespace) -> NoReturn:

    def _enable_column_nullable(columns) -> NoReturn:
        """
        SQLAlchemy에서 Column을 Primary Key로 설정할 경우 자동적으로 Not Null Constraint를 생성함.
        따라서 매개변수로 받은 컬럼의 nullable 값을 True로 변경하여 Not Null Constraint가 생성되지 않도록 함
        """
        for column in columns:
            column.nullable = True

    def _drop_primary_key(performed_table: Table) -> NoReturn:
        """ Declarative Class에 있는 Primary Key 속성을 제거 """

        _enable_column_nullable(performed_table.primary_key.columns)

        table_pks = []
        pk_name = performed_table.primary_key.name
        table_pks.append(PrimaryKeyConstraint(name=pk_name))

        Table(performed_table.name, db_meta.decl_base.metadata, *table_pks, extend_existing=True)

        DropConstraint(table_pks[0])

    def _add_unique(performed_table: Table) -> NoReturn:
        """ Declarative Class에 Unique Key 속성을 추가 """

        table_uks = []
        key_column_names = (column.name for column in performed_table.primary_key.columns)
        uk_name = performed_table.primary_key.name
        table_uks.append(UniqueConstraint(*key_column_names, name=uk_name))

        Table(performed_table.name, db_meta.decl_base.metadata, *table_uks, extend_existing=True)

    tables: List[Table] = db_meta.decl_base.metadata.sorted_tables
    print(f"    {db_meta.description} [{len(tables)}] ... ", end="", flush=True)
    for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                      desc=f"  {db_meta.description}"):

        if table.custom_attrs.constraint_type == NON_KEY:
            _drop_primary_key(table)
        elif table.custom_attrs.constraint_type == UNIQUE:
            _drop_primary_key(table)

            # dest가 BOTH로 들어올 경우 Source에서 _add_unique_key 함수를 호출하여 Mapper에 이미 Unique Key가 생성되어 있음
            # 따라서 Mapper에 Unique Constraint가 이미 추가되어 있는지 검사하여 _add_unique_key 함수를 호출함
            add_uk: bool = False
            for constraint in list(table.constraints):
                if isinstance(constraint, UniqueConstraint):
                    add_uk = True
                    break
            if not add_uk:
                _add_unique(table)
        else:
            _enable_column_nullable(table.primary_key.columns)

        try:
            table.create(bind=db_meta.engine, checkfirst=True)
        except DatabaseError as DE:
            proc_database_error(DE)


def drop_objects(db_meta: DatabaseMetaData, args: argparse.Namespace) -> NoReturn:

    tables: List[Table] = db_meta.decl_base.metadata.sorted_tables
    print(f"    {db_meta.description} [{len(tables)}] ... ", end="", flush=True)
    for table in tqdm(tables, disable=args.verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format,
                      desc=f"  {db_meta.description}"):
        try:
            table.drop(bind=db_meta.engine, checkfirst=True)
        except DatabaseError as DE:
            proc_database_error(DE)


def generate_initial_data(db_meta: DatabaseMetaData, args: argparse.Namespace,
                          initial_data_conf: Dict[str, InitialDataConfig], logger: logging.Logger) -> NoReturn:

    def _proc_execute_insert(table_name: str):
        print(f"    {table_name} [{initial_data_conf[table_name].record_count}] ... ", end="", flush=True)
        t = tqdm(total=initial_data_conf[table_name].record_count, disable=args.verbose, ncols=tqdm_ncols,
                 bar_format=tqdm_bar_format, desc=f"  {table_name} ")
        logger.info(f"{table_name} [{initial_data_conf[table_name].record_count}]")

        remaining_record = initial_data_conf[table_name].record_count
        commit_count = initial_data_conf[table_name].commit_count

        logger.debug(f"Record count: {remaining_record}, Commit count: {commit_count}")

        while remaining_record > 0:
            if remaining_record < commit_count:
                commit_count = remaining_record

            # TODO. 데이터 입력 구현

    for table_name in initial_data_conf:
        pass

