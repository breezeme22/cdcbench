from commons.constants import *
from commons.funcs_common import get_rowid_data, chunker

from datetime import datetime, timedelta

import random
import os

__data_dir = "data"
__lob_data_dir = "lob_files"


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
                    if source_dbms_type == ORACLE:
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
