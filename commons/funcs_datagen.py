from commons.constants import *

from sqlalchemy.sql import select, func

from datetime import datetime, timedelta

import random
import os
import json

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
    "LOB": "lob.dat"
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
            data = json.load(f)

        return data

    except FileNotFoundError:
        raise FileNotFoundError("Data file ({}) does not exist.".format(file_name))


def get_sample_table_data(file_data, table_name, column_names, separate_col_val=None, dbms_type=None, update=False):

    row_data = {}

    # INSERT_TEST, UPDATE_TEST, DELETE_TEST 테이블 데이터 생성
    if table_name in [INSERT_TEST, UPDATE_TEST, DELETE_TEST]:

        product_names = file_data["PRODUCT_NAME"]
        product_dates = file_data["PRODUCT_DATE"]

        if table_name == UPDATE_TEST and update is False:
            product_name = "1"
        else:
            product_name = product_names[random.randrange(len(product_names))]

        product_date = datetime.strptime(product_dates[random.randrange(len(product_dates))], "%Y-%m-%d %H:%M:%S")

        row_data = {
            column_names[0]: product_name,
            column_names[1]: product_date,
            column_names[2]: separate_col_val
        }

    # STRING_TEST 테이블 데이터 생성
    elif table_name == STRING_TEST:

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

    # NUMERIC_TEST 테이블 데이터 생성
    elif table_name == NUMERIC_TEST:

        for key in column_names:

            sample_data_count = len(file_data[key.upper()])

            if sample_data_count > 0:
                column_data = file_data[key.upper()][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # DATETIME_TEST 테이블 데이터 생성
    elif table_name == DATETIME_TEST:

        for key in column_names:

            sample_data_count = len(file_data[key.upper()])

            if sample_data_count > 0:
                tmp_data = file_data[key.upper()][random.randrange(sample_data_count)]

                if key.upper() == "COL_DATETIME":
                    column_data = datetime.strptime(tmp_data, "%Y-%m-%d %H:%M:%S")
                elif key.upper() == "COL_TIMESTAMP":
                    column_data = datetime.strptime(tmp_data, "%Y-%m-%d %H:%M:%S.%f")
                elif key.upper() == "COL_TIMESTAMP2":
                    column_data = datetime.strptime(tmp_data, "%Y-%m-%d %H:%M:%S.%f")
                elif key.upper() == "COL_INTER_YEAR_MONTH":
                    column_data = "{}-{}".format(tmp_data[0], tmp_data[1])
                elif key.upper() == "COL_INTER_DAY_SEC":
                    if dbms_type == ORACLE:
                        column_data = timedelta(days=tmp_data[0], hours=tmp_data[1], minutes=tmp_data[2],
                                                seconds=tmp_data[3], microseconds=tmp_data[4])
                    else:
                        column_data = "{} {:02d}:{:02d}:{:02d}.{:06d}".format(tmp_data[0], tmp_data[1], tmp_data[2],
                                                                              tmp_data[3], tmp_data[4])
                else:
                    column_data = None
            else:
                column_data = None

            row_data[key] = column_data

    # BINARY_TEST 테이블 데이터 생성
    elif table_name == BINARY_TEST:
        col_binary = os.urandom(random.randrange(1, 1001))
        col_varbinary = os.urandom(random.randrange(1, 1001))
        col_long_binary = os.urandom(random.randrange(1, 2001))

        row_data = {
            column_names[0]: col_binary,
            column_names[1]: col_varbinary,
            column_names[2]: col_long_binary
        }

    # LOB_TEST 테이블 데이터 생성
    elif table_name == LOB_TEST:

        for pair in _chunker(column_names, 2):

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

    # ORACLE_TEST 테이블 데이터 생성
    elif table_name == ORACLE_TEST:

        for key in column_names:
            sample_data_count = len(file_data[key])

            if sample_data_count > 0:
                column_data = file_data[key][random.randrange(sample_data_count)]
            else:
                column_data = None

            row_data[key] = column_data

    # SQLSERVER_TEST 테이블 데이터 생성
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


def _chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))