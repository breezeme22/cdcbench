from sqlalchemy import Column, Sequence, PrimaryKeyConstraint
from sqlalchemy.types import NCHAR
from sqlalchemy.dialects.oracle import \
        CHAR, LONG, NVARCHAR2, VARCHAR2, \
        BINARY_DOUBLE, BINARY_FLOAT, FLOAT, NUMBER, \
        DATE, INTERVAL, TIMESTAMP, \
        RAW, \
        BFILE, BLOB, CLOB, NCLOB, \
        ROWID

from commons.constants import *
from commons.funcs_common import get_json_data, get_rowid_data, get_commit_msg, get_elapsed_time_msg, chunker
from commons.mgr_config import ConfigManager
from commons.mgr_logger import LoggerManager
from mappers.oracle_mappers import OracleMapperBase
from mappers.oracle_custom_types import VARCHAR2BYTE, LONGRAW, INTERVALYEARMONTH

from datetime import datetime, timedelta
from pyparsing import Word, delimitedList, Optional, Group, alphas, nums, alphanums, OneOrMore, \
                      Keyword, Suppress, oneOf, ParseException

import random
import os

__data_dir = "data"
__lob_data_dir = "lob_files"
__mapper_dir = "mappers"


def _user_defined_table_parser(table_name):

    file_name = "{}.tab".format(table_name)
    with open(os.path.join(__mapper_dir, file_name), "r", encoding="utf-8") as f:
        table_definition = f.read()

    try:

        lbracket, rbracket = map(Suppress, "()")
        object_name = Word(alphas, alphanums + "-_\"$?")

        # String Category
        oracle_char = Keyword(ORACLE_CHAR).setResultsName("data_type") \
                      + Optional(lbracket + Word(nums).setResultsName("data_length") + rbracket)

        oracle_nchar = Keyword(ORACLE_NCHAR).setResultsName("data_type") \
                       + Optional(lbracket + Word(nums).setResultsName("data_length") + rbracket)

        oracle_varchar2 = Keyword(ORACLE_VARCHAR2).setResultsName("data_type") \
                          + lbracket + Word(nums).setResultsName("data_length") \
                          + Optional(oneOf("BYTE CHAR", asKeyword=True), default="BYTE").setResultsName("varchar2_type") \
                          + rbracket

        oracle_nvarchar2 = Keyword(ORACLE_NVARCHAR2).setResultsName("data_type") \
                           + lbracket + Word(nums).setResultsName("data_length") + rbracket

        oracle_long = Keyword(ORACLE_LONG).setResultsName("data_type")

        # Numeric Category
        oracle_number = Keyword(ORACLE_NUMBER).setResultsName("data_type") \
                        + Optional(lbracket + delimitedList(Word(nums)).setResultsName("data_length") + rbracket)

        oracle_binary_float = Keyword(ORACLE_BINARY_FLOAT).setResultsName("data_type")

        oracle_binary_double = Keyword(ORACLE_BINARY_DOUBLE).setResultsName("data_type")

        oracle_float = Keyword(ORACLE_FLOAT).setResultsName("data_type")

        # Date & Time Category
        oracle_date = Keyword(ORACLE_DATE).setResultsName("data_type")

        oracle_timestamp = Keyword(ORACLE_TIMESTAMP).setResultsName("data_type")

        oracle_interval = Keyword(ORACLE_INTERVAL).setResultsName("data_type") \
                          + oneOf("YEAR DAY", asKeyword=True).setResultsName("year_or_day") \
                          + Optional(lbracket + Word(nums).setResultsName("year_or_day_precision") + rbracket) \
                          + Keyword("TO") \
                          + oneOf("MONTH SECOND", asKeyword=True).setResultsName("month_or_second") \
                          + Optional(lbracket + Word(nums).setResultsName("second_precision") + rbracket)

        # Binary Category
        oracle_raw = Keyword(ORACLE_RAW).setResultsName("data_type") \
                     + lbracket + Word(nums).setResultsName("data_length") + rbracket

        oracle_long_raw = Keyword(ORACLE_LONG_RAW).setResultsName("data_type")

        # LOB Category
        oracle_clob = Keyword(ORACLE_CLOB).setResultsName("data_type")

        oracle_nclob = Keyword(ORACLE_NCLOB).setResultsName("data_type")

        oracle_blob = Keyword(ORACLE_BLOB).setResultsName("data_type")

        # etc. Category
        oracle_rowid = Keyword(ORACLE_ROWID).setResultsName("data_type")

        column_name_def = object_name.setResultsName("column_name")
        # data_type_def = (ora_four_word | ora_two_word | ora_one_word)
        data_type_def = oracle_long_raw | oracle_char | oracle_nchar | oracle_varchar2 | oracle_nvarchar2 | oracle_long \
                        | oracle_number | oracle_binary_float | oracle_binary_double | oracle_float \
                        | oracle_date | oracle_timestamp | oracle_interval \
                        | oracle_raw  \
                        | oracle_clob | oracle_nclob | oracle_blob \
                        | oracle_rowid
        # LONG RAW 타입은 LONG 보다 먼저 Matching 되어야 해서 제일 앞으로 보냄

        nullable_def = Optional(Keyword("NOT NULL") | Keyword("NULL"), default="NULL").setResultsName("nullable")

        column_def = OneOrMore(Group(column_name_def + data_type_def + nullable_def))

        column_list_def = delimitedList(column_def).setResultsName("columns")

        primary_key_def = Keyword("PRIMARY KEY") + lbracket + delimitedList(object_name).setResultsName("primary_key_column") + rbracket

        constraint_def = Group(Keyword("CONSTRAINT") + object_name.setResultsName("constraint_name") +
                               primary_key_def).setResultsName("constraint")

        table_name_def = object_name.setResultsName("table_name")

        table_def = Group(table_name_def + lbracket + column_list_def + Suppress(",") +
                          constraint_def + rbracket + Suppress(";")).setResultsName("table")

        table_metadata = OneOrMore(table_def).parseString(table_definition)

        return table_metadata

    except ParseException as pe:
        raise


def _gen_oracle_types(column):

    if column.data_type == ORACLE_CHAR:
        return CHAR(int(column.data_length))
    elif column.data_type == ORACLE_NCHAR:
        return NCHAR(int(column.data_length))
    elif column.data_type == ORACLE_VARCHAR2:
        if column.varchar2_type == "BYTE":
            return VARCHAR2BYTE(int(column.data_length))
        elif column.varchar2_type == "CHAR":
            return VARCHAR2(int(column.data_length))
    elif column.data_type == ORACLE_NVARCHAR2:
        return NVARCHAR2(int(column.data_length))
    elif column.data_type == ORACLE_LONG:
        return LONG
    elif column.data_type == ORACLE_NUMBER:
        if len(column.data_length) == 1:
            return NUMBER(int(column.data_length[0]))
        elif len(column.data_length) == 2:
            return NUMBER(int(column.data_length[0]), int(column.data_length[1]))
        else:
            return NUMBER
    elif column.data_type == ORACLE_BINARY_FLOAT:
        return BINARY_FLOAT
    elif column.data_type == ORACLE_BINARY_DOUBLE:
        return BINARY_DOUBLE
    elif column.data_type == ORACLE_FLOAT:
        return FLOAT
    elif column.data_type == ORACLE_DATE:
        return DATE
    elif column.data_type == ORACLE_TIMESTAMP:
        return TIMESTAMP
    elif column.data_type == ORACLE_INTERVAL:
        if column.year_or_day == "YEAR":
            if column.year_or_day_precision != '':
                return INTERVALYEARMONTH(int(column.year_or_day_precision))
            else:
                return INTERVALYEARMONTH
        elif column.year_or_day == "DAY":
            if column.year_or_day_precision == '' and column.second_precision == '':
                return INTERVAL
            elif column.year_or_day_precision != '' and column.second_precision == '':
                return INTERVAL(day_precision=int(column.year_or_day_precision))
            elif column.year_or_day_precision == '' and column.second_precision != '':
                return INTERVAL(second_precision=int(column.second_precision))
            elif column.year_or_day_precision != '' and column.second_precision != '':
                return INTERVAL(day_precision=int(column.year_or_day_precision),
                                second_precision=int(column.second_precision))
    elif column.data_type == ORACLE_RAW:
        return RAW(int(column.data_length))
    elif column.data_type == ORACLE_LONG_RAW:
        return LONGRAW
    elif column.data_type == ORACLE_CLOB:
        return CLOB
    elif column.data_type == ORACLE_NCLOB:
        return NCLOB
    elif column.data_type == ORACLE_BLOB:
        return BLOB
    elif column.data_type == ORACLE_ROWID:
        return ROWID


def gen_user_defined_table(table_name):

    table_metadata = _user_defined_table_parser(table_name)[0]

    print(table_metadata.dump())
    
    # Table Name 생성
    mapper_attr = {"__tablename__": table_metadata.table_name}
    
    # Column 정보 생성
    for idx, column in enumerate(table_metadata.columns):
        nullable = True
        if column.nullable == "NOT NULL":
            nullable = False

        if idx == 0:
            mapper_attr[column.column_name] = Column(column.column_name, _gen_oracle_types(column),
                                                     Sequence("{}_SEQ".format(table_metadata.table_name)),
                                                     nullable=nullable)
        else:
            mapper_attr[column.column_name] = Column(column.column_name, _gen_oracle_types(column), nullable=nullable)

    # Primary Key Constraint 정보 생성
    mapper_attr["__table_args__"] = (PrimaryKeyConstraint(*table_metadata.constraint.primary_key_column,
                                                          name=table_metadata.constraint.constraint_name),)

    UserDefinedTable = type("UserDefinedTable", (OracleMapperBase,), mapper_attr)

    return UserDefinedTable


def gen_sample_table_data(source_dbms_type, file_data, table_name, column_names):

    row_data = {}

    # STRING_TEST 테이블 데이터 처리
    if table_name == STRING_TEST:
        for key in column_names:

            sample_data_count = len(file_data[key.upper()])

            if sample_data_count > 0:
                # COL_TEXT 컬럼 데이터 처리
                if key.upper() == "COL_TEXT":
                    text_file_name = file_data[key.upper()][random.randrange(sample_data_count)]
                    with open(os.path.join(__data_dir, __lob_data_dir, text_file_name), "r",
                              encoding="utf-8") as f:
                        column_data = f.read()
                else:
                    column_data = file_data[key.upper()][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # NUMERIC_TEST 테이블 데이터 처리
    elif table_name == NUMERIC_TEST:
        for key in column_names:

            sample_data_count = len(file_data[key.upper()])

            if sample_data_count > 0:
                column_data = file_data[key.upper()][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # DATETIME_TEST 테이블 데이터 처리
    elif table_name == DATETIME_TEST:
        for key in column_names:

            sample_data_count = len(file_data[key.upper()])
            formatted_data = None

            if sample_data_count > 0:
                column_data = file_data[key.upper()][random.randrange(sample_data_count)]

                if key.upper() == "COL_DATETIME":
                    formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S")
                elif key.upper() == "COL_TIMESTAMP":
                    formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S.%f")
                elif key.upper() == "COL_TIMESTAMP2":
                    formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S.%f")
                elif key.upper() == "COL_INTER_YEAR_MONTH":
                    formatted_data = "{}-{}".format(column_data[0], column_data[1])
                elif key.upper() == "COL_INTER_DAY_SEC":
                    if source_dbms_type == dialect_driver[ORACLE]:
                        formatted_data = timedelta(days=column_data[0], hours=column_data[1],
                                                   minutes=column_data[2], seconds=column_data[3],
                                                   microseconds=column_data[4])
                    else:
                        formatted_data = "{} {:02d}:{:02d}:{:02d}.{:06d}" \
                            .format(column_data[0], column_data[1], column_data[2],
                                    column_data[3], column_data[4])

            row_data[key] = formatted_data

    # BINARY_TEST 테이블 데이터 처리
    elif table_name == BINARY_TEST:
        col_binary = os.urandom(random.randrange(1, 1001))
        col_varbinary = os.urandom(random.randrange(1, 1001))
        col_long_binary = os.urandom(random.randrange(1, 2001))

        row_data = {
            column_names[0]: col_binary,
            column_names[1]: col_varbinary,
            column_names[2]: col_long_binary
        }

    # LOB_TEST 테이블 데이터 처리
    elif table_name == LOB_TEST:
        for pair in chunker(column_names, 2):

            key = pair[0].rpartition("_")[0].upper()
            sample_data_count = len(file_data[key])

            if sample_data_count > 0:
                lob_file_name = file_data[key][random.randrange(sample_data_count)]
                file_extension = lob_file_name.split(".")[1]

                row_data[pair[0]] = lob_file_name

                if file_extension == "txt":
                    with open(os.path.join(__data_dir, __lob_data_dir, lob_file_name), "r",
                              encoding="utf-8") as f:
                        row_data[pair[1]] = f.read()
                else:
                    with open(os.path.join(__data_dir, __lob_data_dir, lob_file_name), "rb") as f:
                        row_data[pair[1]] = f.read()
            else:
                row_data[pair[0]] = None
                row_data[pair[1]] = None

    # ORACLE_TEST 테이블 데이터 처리
    elif table_name == ORACLE_TEST:
        row_data["COL_ROWID"] = get_rowid_data()

        for key in file_data.keys():
            sample_data_count = len(file_data[key])

            if sample_data_count > 0:
                column_data = file_data[key][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # SQLSERVER_TEST 테이블 데이터 처리
    elif table_name == SQLSERVER_TEST:
        for key in column_names:
            sample_data_count = len(file_data[key])

            if sample_data_count > 0:
                column_data = file_data[key][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    return row_data


def gen_user_table_data():
    pass
