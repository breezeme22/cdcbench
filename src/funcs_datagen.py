from src.constants import *
from src.funcs_common import print_error_msg

from datetime import datetime, timedelta
from sqlalchemy.sql import select, func

import random
import os
import yaml

__data_dir = "data"
__lob_data_dir = "lob_files"

data_file_name = {
    "INSERT": "dml.dat",
    "UPDATE": "dml.dat",
    "DELETE": "dml.dat",
    "STRING": "string.dat",
    "NUMERIC": "numeric.dat",
    "DATETIME": "datetime.dat",
    "BINARY": "binary.dat",
    "LOB": "lob.dat",
    "ORACLE": "oracle.dat",
    "SQLSERVER": "sqlserver.dat"
}


def get_separate_col_val(engine, table, column):
    sql = select([func.max(table.columns[column]).label("MAX_SEPARATE_COL")])
    result = engine.execute(sql).scalar()
    if result is None:
        return 1
    else:
        return result + 1


# data file read
def get_file_data(file_name):

    try:
        with open(os.path.join(__data_dir, file_name), "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return data

    except FileNotFoundError:
        print_error_msg(f"Data file ({file_name}) does not exist.")

    except yaml.YAMLError as yerr:
        print_error_msg(f"Invalid YAML format of data file. [ {yerr.args[1].name} ] "
                        f"line {yerr.args[1].line+1}, column {yerr.args[1].column+1}")


def _read_file(file_name):

    file_extension = file_name.split(".")[1]

    try:
        if file_extension == "txt":
            with open(os.path.join(__data_dir, __lob_data_dir, file_name), "r", encoding="utf-8") as f:
                return f.read()
        else:
            with open(os.path.join(__data_dir, __lob_data_dir, file_name), "rb") as f:
                return f.read()

    except UnicodeDecodeError as unierr:
        print("... Fail")
        print_error_msg(f"'{unierr.encoding}' codec can't decode file [ {file_name} ]. \n"
                        "  * Note. The LOB test file with string must be UTF-8 (without BOM) encoding.")


def get_sample_table_data(file_data, table_name, column_names, separate_col_val=None, dbms_type=None):

    row_data = {}

    # INSERT_TEST, UPDATE_TEST, DELETE_TEST 테이블 데이터 생성
    if table_name.upper() in [INSERT_TEST, UPDATE_TEST]:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            if key_upper == "SEPARATE_COL":
                column_data = separate_col_val
            else:
                sample_data_count = len(file_data[key_upper])
                if sample_data_count > 0:
                    column_data = file_data[key_upper][random.randrange(sample_data_count)]
                else:
                    column_data = None

            row_data[key] = column_data

    # STRING_TEST 테이블 데이터 생성
    elif table_name.upper() == STRING_TEST:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            sample_data_count = len(file_data[key_upper])

            if sample_data_count > 0:
                # COL_TEXT 컬럼 데이터 처리
                if key_upper == "COL_TEXT":
                    long_file_name = file_data[key_upper][random.randrange(sample_data_count)]
                    column_data = _read_file(long_file_name)
                else:
                    column_data = file_data[key_upper][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # NUMERIC_TEST 테이블 데이터 생성
    elif table_name.upper() == NUMERIC_TEST:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            sample_data_count = len(file_data[key_upper])

            if sample_data_count > 0:
                column_data = file_data[key_upper][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # DATETIME_TEST 테이블 데이터 생성
    elif table_name.upper() == DATETIME_TEST:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            sample_data_count = len(file_data[key_upper])

            if sample_data_count > 0:
                tmp_data = file_data[key_upper][random.randrange(sample_data_count)]
                if key_upper == "COL_INTER_YEAR_MONTH":
                    column_data = f"{tmp_data[0]}-{tmp_data[1]}"
                elif key_upper == "COL_INTER_DAY_SEC":
                    if dbms_type == SQLSERVER:
                        column_data = f"{tmp_data[0]} {tmp_data[1]:02d}:{tmp_data[2]:02d}:" \
                                          f"{tmp_data[3]:02d}.{tmp_data[4]:06d}"
                    else:
                        column_data = timedelta(days=tmp_data[0], hours=tmp_data[1], minutes=tmp_data[2],
                                                seconds=tmp_data[3], microseconds=tmp_data[4])
                else:
                    column_data = tmp_data
            else:
                column_data = None

            row_data[key] = column_data

    # BINARY_TEST 테이블 데이터 생성
    elif table_name.upper() == BINARY_TEST:

        col_binary = os.urandom(random.randrange(1, 1001))
        col_varbinary = os.urandom(random.randrange(1, 1001))
        col_long_binary = os.urandom(random.randrange(1, 2001))

        row_data = {
            column_names[0]: col_binary,
            column_names[1]: col_varbinary,
            column_names[2]: col_long_binary
        }

    # LOB_TEST 테이블 데이터 생성
    elif table_name.upper() == LOB_TEST:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            sample_data_count = len(file_data[key_upper])

            if sample_data_count > 0:
                lob_file_name = file_data[key_upper][random.randrange(sample_data_count)]
                column_data = _read_file(lob_file_name)
            else:
                column_data = None

            row_data[key] = column_data

    # ORACLE_TEST 테이블 데이터 생성
    elif table_name.upper() == ORACLE_TEST:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            sample_data_count = len(file_data[key_upper])

            if sample_data_count > 0:
                column_data = file_data[key_upper][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # SQLSERVER_TEST 테이블 데이터 생성
    elif table_name.upper() == SQLSERVER_TEST:

        for key in column_names:

            key_upper = key.upper()
            if key_upper == "T_ID":
                continue

            sample_data_count = len(file_data[key_upper])

            if sample_data_count > 0:
                column_data = file_data[key_upper][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    return row_data
