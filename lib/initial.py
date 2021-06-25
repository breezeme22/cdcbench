
import argparse
import logging
import jaydebeapi
import jpype
import random
import re

from typing import List, Dict, NoReturn

from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import Table, PrimaryKeyConstraint, UniqueConstraint, DropConstraint
from yaspin import yaspin
from yaspin.spinners import Spinners

from lib.globals import *
from lib.common import proc_database_error, DBWorkToolBox
from lib.config import ConfigModel, InitialDataConfig
from lib.sql import DML, execute_tcl


def create_objects(tool_box: DBWorkToolBox) -> NoReturn:

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

        Table(performed_table.name, tool_box.decl_base.metadata, *table_pks, extend_existing=True)

        DropConstraint(table_pks[0])

    def _add_unique(performed_table: Table) -> NoReturn:
        """ Declarative Class에 Unique Key 속성을 추가 """

        table_uks = []
        key_column_names = (column.name for column in performed_table.primary_key.columns)
        uk_name = performed_table.primary_key.name
        table_uks.append(UniqueConstraint(*key_column_names, name=uk_name))

        Table(performed_table.name, tool_box.decl_base.metadata, *table_uks, extend_existing=True)

    tables: List[Table] = list(tool_box.tables.values())
    with yaspin(Spinners.line, text=f"    {tool_box.description} [{len(tables)}] ...", side="right") as sp:
        for table in tables:

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
                table.create(bind=tool_box.engine, checkfirst=True)
            except DatabaseError as DE:
                proc_database_error(DE)

        sp.ok(COMPLETE)


def drop_objects(tool_box: DBWorkToolBox) -> NoReturn:

    tables: List[Table] = list(tool_box.tables.values())
    with yaspin(Spinners.line, text=f"    {tool_box.description} [{len(tables)}] ...", side="right") as sp:
        for table in tables:
            try:
                table.drop(bind=tool_box.engine, checkfirst=True)
            except DatabaseError as DE:
                proc_database_error(DE)

        sp.ok(COMPLETE)


def generate_initial_data(args: argparse.Namespace, config: ConfigModel,
                          tool_boxes: Dict[str, DBWorkToolBox]) -> NoReturn:

    dmls: [str, DML] = {db_key: DML(args, config, tool_boxes[db_key]) for db_key in tool_boxes}

    first_tool_box = list(tool_boxes.values())[0]
    first_db_dbms = first_tool_box.conn_info.dbms

    init_data_conf = config.initial_data

    for table_name in init_data_conf:
        list_row_data = []
        table_data = first_tool_box.table_data[table_name]
        table_columns = first_tool_box.table_columns[table_name]

        try:
            with yaspin(Spinners.line, text=f"    {table_name} [{init_data_conf[table_name].record_count}] ...",
                        side="right") as sp:
                for i in range(1, init_data_conf[table_name].record_count + 1):

                    list_row_data.append(table_data.get_row_data(table_columns, first_db_dbms))

                    if i % init_data_conf[table_name].commit_count != 0:
                        for db_key in dmls:
                            dmls[db_key].execute_multi_dml(INSERT, dmls[db_key].tables[table_name].insert(),
                                                           list_row_data, table_name)
                            execute_tcl(dmls[db_key].conn, False, dmls[db_key].summary)
                        list_row_data.clear()
                    elif len(list_row_data) >= config.settings.dml_array_size:
                        for db_key in dmls:
                            dmls[db_key].execute_multi_dml(INSERT, dmls[db_key].tables[table_name].insert(),
                                                           list_row_data, table_name)
                        list_row_data.clear()

                if init_data_conf[table_name].record_count % init_data_conf[table_name].commit_count != 0:
                    for db_key in dmls:
                        dmls[db_key].execute_multi_dml(INSERT, dmls[db_key].tables[table_name].insert(), list_row_data,
                                                       table_name)
                        execute_tcl(dmls[db_key].conn, False, dmls[db_key].summary)

                sp.ok(COMPLETE)

        except DatabaseError as DE:
            proc_database_error(DE)
