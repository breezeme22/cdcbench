from commons.constants import *
from commons.funcs_common import print_error_msg
from commons.mgr_mappers import TYPE

from datetime import timedelta

import random
import os
import yaml

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
    "SQLSERVER": "sqlserver.dat",
    "USER": "user.dat"
}


class FuncsDataMaker:

    __data_dir = "data"
    __lob_data_dir = "lob_files"

    def __init__(self, file_name):

        try:
            with open(os.path.join(self.__data_dir, file_name), "r", encoding="utf-8") as f:
                self.file_data = yaml.safe_load(f)

        except FileNotFoundError:
            print_error_msg(f"Data file [ {file_name} ] does not exist.")

        except yaml.YAMLError as yerr:
            print_error_msg(f"Invalid YAML format of data file [ {yerr.args[1].name} ]."
                            f"line {yerr.args[1].line+1}, column {yerr.args[1].column+1}")

    def get_file_data(self):
        return self.file_data

    @classmethod
    def _read_lob_file(cls, file_name):
        """
        File을 Open하여 내용을 읽음
        :param file_name: File Name
        :return: File Content
        """

        try:
            file_extension = file_name.split(".")[1]

            # File 확장자에 따라 읽는 방식을 구분
            if file_extension == "txt":
                with open(os.path.join(cls.__data_dir, cls.__lob_data_dir, file_name), "r", encoding="utf-8") as f:
                    return f.read()
            else:
                with open(os.path.join(cls.__data_dir, cls.__lob_data_dir, file_name), "rb") as f:
                    return f.read()

        except IndexError:
            print_error_msg(f"Invalid LOB file name [ {file_name} ]. Check file name in data file.")

        except FileNotFoundError as ferr:
            print_error_msg(f"LOB file [ {file_name} ] does not exist.")

        except UnicodeDecodeError as unierr:
            print("... Fail")
            print_error_msg(f"'{unierr.encoding}' codec can't decode file [ {file_name} ]. \n"
                            "  * Note. The LOB test file with string must be UTF-8 (without BOM) encoding.")

    def _basic_data_select(self, key):

        sample_data_count = len(self.file_data[key])

        if sample_data_count > 0:
            return self.file_data[key][random.randrange(sample_data_count)]

        else:
            return None

    def _lob_data_select(self, key):

        sample_data_count = len(self.file_data[key])

        if sample_data_count > 0:
            lob_file_name = self.file_data[key][random.randrange(sample_data_count)]
            return self._read_lob_file(lob_file_name)
        else:
            return None

    def get_sample_table_data(self, table_name, columns, separate_col_val=None, dbms_type=None):
        """
        Sample table의 Row 단위 Sample data를 생성
        :param table_name: Table name
        :param columns: 작업 대상 column list
        :param separate_col_val: INSERT_TEST 테이블의 경우 row separate_col 값
        :param dbms_type: DBMS type
        :return: Row data
        """

        row_data = {}

        # INSERT_TEST, UPDATE_TEST, DELETE_TEST 테이블 데이터 생성
        if table_name.upper() in [INSERT_TEST, UPDATE_TEST]:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                if column_name_upper == "SEPARATE_COL":
                    column_data = separate_col_val
                else:
                    sample_data_count = len(self.file_data[column_name_upper])
                    if sample_data_count > 0:
                        column_data = self.file_data[column_name_upper][random.randrange(sample_data_count)]
                    else:
                        column_data = None

                row_data[column.name] = column_data

        # STRING_TEST 테이블 데이터 생성
        elif table_name.upper() == STRING_TEST:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                # COL_TEXT 컬럼 데이터 처리
                if column_name_upper == "COL_TEXT":
                    column_data = self._lob_data_select(column_name_upper)
                else:
                    column_data = self._basic_data_select(column_name_upper)

                row_data[column.name] = column_data

        # NUMERIC_TEST 테이블 데이터 생성
        elif table_name.upper() == NUMERIC_TEST:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                row_data[column.name] = self._basic_data_select(column_name_upper)

        # DATETIME_TEST 테이블 데이터 생성
        elif table_name.upper() == DATETIME_TEST:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                sample_data_count = len(self.file_data[column_name_upper])

                if sample_data_count > 0:
                    tmp_data = self.file_data[column_name_upper][random.randrange(sample_data_count)]
                    if column_name_upper == "COL_INTER_YEAR_MONTH":
                        column_data = f"{tmp_data[0]}-{tmp_data[1]}"
                    elif column_name_upper == "COL_INTER_DAY_SEC":
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

                row_data[column.name] = column_data

        # BINARY_TEST 테이블 데이터 생성
        elif table_name.upper() == BINARY_TEST:

            col_binary = os.urandom(random.randrange(1, 1001))
            col_varbinary = os.urandom(random.randrange(1, 1001))
            col_long_binary = os.urandom(random.randrange(1, 2001))

            row_data = {
                columns[0].name: col_binary,
                columns[1].name: col_varbinary,
                columns[2].name: col_long_binary
            }

        # LOB_TEST 테이블 데이터 생성
        elif table_name.upper() == LOB_TEST:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                row_data[column.name] = self._lob_data_select(column_name_upper)

        # ORACLE_TEST 테이블 데이터 생성
        elif table_name.upper() == ORACLE_TEST:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                row_data[column.name] = self._basic_data_select(column_name_upper)

        # SQLSERVER_TEST 테이블 데이터 생성
        elif table_name.upper() == SQLSERVER_TEST:

            for column in columns:

                column_name_upper = column.name.upper()
                if column_name_upper == "T_ID":
                    continue

                row_data[column.name] = self._basic_data_select(column_name_upper)

        return row_data

    def get_user_table_random_data(self, columns, dbms_type):
        """
        사용자 정의 테이블의 Row 단위 sample data 생성
        :param columns: 작업 대상 column list
        :param dbms_type: DBMS type
        :return: row data
        """

        row_data = {}

        def group(data_type):
            return f"GROUP.{data_type}"

        class GROUP:

            CHAR = group("CHAR")
            VARCHAR = group("VARCHAR")

            NUMBER = group("NUMBER")
            BIT = group("BIT")
            TINYINT = group("TINYINT")
            SMALLINT = group("SMALLINT")
            MEDIUMINT = group("MEDIUMINT")
            INT = group("INT")
            BIGINT = group("BIGINT")
            DECIMAL = group("DECIMAL")
            FLOAT = group("FLOAT")
            DOUBLE = group("DOUBLE")
            MONEY = group("MONEY")

            TIME = group("TIME")
            DATE = group("DATE")
            DATETIME = group("DATETIME")
            TIMESTAMP = group("TIMESTAMP")
            INTERVAL_YEAR_MONTH = group("INTERVAL_YEAR_MONTH")
            INTERVAL_DAY_SECOND = group("INTERVAL_DAY_SECOND")
            DATETIMEOFFSET = group("DATETIMEOFFSET")

            CLOB = group("CLOB")
            BLOB = group("BLOB")

            ROWID = group("ROWID")

        for column in columns:

            if column.default is not None:
                continue

            data_type = column.type
            data_type_name = data_type.__class__.__name__

            if dbms_type == ORACLE:
                # GROUP.CHAR
                if data_type_name in [TYPE.CHAR, TYPE.NCHAR]:
                    column_data = self._basic_data_select(GROUP.CHAR)

                # GROUP.VARCHAR
                elif data_type_name in [TYPE.VARCHAR2, TYPE.NVARCHAR]:
                    column_data = self._basic_data_select(GROUP.VARCHAR)

                # GROUP.NUMBER
                elif data_type_name == TYPE.NUMBER:
                    column_data = self._basic_data_select(GROUP.NUMBER)

                # GROUP.FLOAT
                elif data_type_name in [TYPE.BINARY_FLOAT, TYPE.FLOAT]:
                    column_data = self._basic_data_select(GROUP.FLOAT)

                # GROUP.DOUBLE
                elif data_type_name == TYPE.BINARY_DOUBLE:
                    column_data = self._basic_data_select(GROUP.DOUBLE)

                # GROUP.DATETIME
                elif data_type_name == TYPE.DATE:
                    column_data = self._basic_data_select(GROUP.DATETIME)

                # GROUP.TIMESTAMP
                elif data_type_name == TYPE.TIMESTAMP:
                    column_data = self._basic_data_select(GROUP.TIMESTAMP)

                # GROUP.INTERVAL_YEAR_MONTH
                elif data_type_name == TYPE.INTERVAL:
                    if "year_precision" in data_type.__dict__:
                        tmp_data = self._basic_data_select(GROUP.INTERVAL_YEAR_MONTH)
                        column_data = f"{tmp_data[0]}-{tmp_data[1]}"
                    else:
                        tmp_data = self._basic_data_select(GROUP.INTERVAL_DAY_SECOND)
                        column_data = timedelta(days=tmp_data[0], hours=tmp_data[1], minutes=tmp_data[2],
                                                seconds=tmp_data[3], microseconds=tmp_data[4])

                # TYPE.RAW
                elif data_type_name == TYPE.RAW:
                    column_data = os.urandom(random.randrange(int(data_type.length)))

                # TYPE.LONG_RAW
                elif data_type_name == TYPE.LONG_RAW:
                    column_data = os.urandom(random.randrange(2000))

                # GROUP.CLOB
                elif data_type_name in [TYPE.CLOB, TYPE.NCLOB, TYPE.LONG]:
                    column_data = self._lob_data_select(GROUP.CLOB)

                # GROUP.BLOB
                elif data_type_name == TYPE.BLOB:
                    column_data = self._lob_data_select(GROUP.BLOB)

                # GROUP.ROWID
                elif data_type_name == TYPE.ROWID:
                    column_data = self._basic_data_select(GROUP.ROWID)

                # Unsupported Data Type
                else:
                    column_data = None

            elif dbms_type == MYSQL:
                # GROUP.CHAR
                if data_type_name in [TYPE.CHAR, TYPE.NCHAR, TYPE.TINYTEXT]:
                    column_data = self._basic_data_select(GROUP.CHAR)

                # GROUP.VARCHAR
                elif data_type_name in [TYPE.VARCHAR, TYPE.NVARCHAR, TYPE.TEXT]:
                    column_data = self._basic_data_select(GROUP.VARCHAR)

                # GROUP.TINYINT
                elif data_type_name == TYPE.TINYINT:
                    column_data = self._basic_data_select(GROUP.TINYINT)

                # GROUP.SMALLINT
                elif data_type_name == TYPE.SMALLINT:
                    column_data = self._basic_data_select(GROUP.SMALLINT)

                # GROUP.MEDIUMINT
                elif data_type_name == TYPE.MEDIUMINT:
                    column_data = self._basic_data_select(GROUP.MEDIUMINT)

                # GROUP.INT
                elif data_type_name in [TYPE.INT, TYPE.INTEGER]:
                    column_data = self._basic_data_select(GROUP.INT)

                # GROUP.BIGINT
                elif data_type_name == TYPE.BIGINT:
                    column_data = self._basic_data_select(GROUP.BIGINT)

                # GROUP.DECIMAL
                elif data_type_name in [TYPE.DECIMAL, TYPE.NUMERIC]:
                    column_data = self._basic_data_select(GROUP.DECIMAL)

                # GROUP.FLOAT
                elif data_type_name == TYPE.FLOAT:
                    column_data = self._basic_data_select(GROUP.FLOAT)

                # GROUP.DOUBLE
                elif data_type_name == TYPE.DOUBLE:
                    column_data = self._basic_data_select(GROUP.DOUBLE)

                # GROUP.TIME
                elif data_type_name == TYPE.TIME:
                    column_data = self._basic_data_select(GROUP.TIME)

                # GROUP.DATE
                elif data_type_name in [TYPE.DATE, TYPE.YEAR]:
                    tmp_data = self._basic_data_select(GROUP.DATE)
                    if data_type_name == TYPE.YEAR:
                        column_data = tmp_data.year
                    else:
                        column_data = tmp_data

                # GROUP.DATETIME
                elif data_type_name == TYPE.DATETIME:
                    column_data = self._basic_data_select(GROUP.DATETIME)

                # GROUP.TIMESTAMP
                elif data_type_name == TYPE.TIMESTAMP:
                    column_data = self._basic_data_select(GROUP.TIMESTAMP)

                # TYPE.BINARY
                elif data_type_name == TYPE.BINARY:
                    if data_type.length is None:
                        data_type.length = 1
                    column_data = os.urandom(random.randrange(data_type.length))

                # TYPE.VARBINARY
                elif data_type_name == TYPE.VARBINARY:
                    column_data = os.urandom(random.randrange(data_type.length))

                # TYPE.TINYBLOB
                elif data_type_name == TYPE.TINYBLOB:
                    column_data = os.urandom(random.randrange(255))

                # TYPE.BLOB
                elif data_type_name == TYPE.BLOB:
                    if data_type.length is None:
                        data_type.length = 65535
                    column_data = os.urandom(random.randrange(data_type.length))

                # GROUP.CLOB
                elif data_type_name in [TYPE.MEDIUMTEXT, TYPE.LONGTEXT]:
                    column_data = self._lob_data_select(GROUP.CLOB)

                # GROUP.BLOB
                elif data_type_name in [TYPE.MEDIUMBLOB, TYPE.LONGBLOB]:
                    column_data = self._lob_data_select(GROUP.BLOB)

                # Unsupported Data Type
                else:
                    column_data = None

            elif dbms_type == SQLSERVER:
                # GROUP.CHAR
                if data_type_name in [TYPE.CHAR, TYPE.NCHAR]:
                    column_data = self._basic_data_select(GROUP.CHAR)

                # GROUP.VARCHAR / GROUP.CLOB
                elif data_type_name in [TYPE.VARCHAR, TYPE.NVARCHAR]:
                    # GROUP.CLOB
                    if data_type.length == "MAX":
                        column_data = self._lob_data_select(GROUP.CLOB)
                    # GROUP.VARCHAR
                    else:
                        column_data = self._basic_data_select(GROUP.VARCHAR)

                # GROUP.BIT
                elif data_type_name == TYPE.BIT:
                    column_data = self._basic_data_select(GROUP.BIT)

                # GROUP.TINYINT
                elif data_type_name == TYPE.TINYINT:
                    column_data = self._basic_data_select(GROUP.TINYINT)

                # GROUP.SMALLINT
                elif data_type_name == TYPE.SMALLINT:
                    column_data = self._basic_data_select(GROUP.SMALLINT)

                # GROUP.INT
                elif data_type_name in [TYPE.INT, TYPE.INTEGER]:
                    column_data = self._basic_data_select(GROUP.INT)

                # GROUP.BIGINT
                elif data_type_name == TYPE.BIGINT:
                    column_data = self._basic_data_select(GROUP.BIGINT)

                # GROUP.DECIMAL
                elif data_type_name in [TYPE.DECIMAL, TYPE.NUMERIC]:
                    column_data = self._basic_data_select(GROUP.DECIMAL)

                # GROUP.FLOAT
                elif data_type_name == TYPE.REAL:
                    column_data = self._basic_data_select(GROUP.FLOAT)

                # GROUP.DOUBLE
                elif data_type_name == TYPE.FLOAT:
                    column_data = self._basic_data_select(GROUP.DOUBLE)

                # GROUP.MONEY
                elif data_type_name in [TYPE.SMALLMONEY, TYPE.MONEY]:
                    column_data = self._basic_data_select(GROUP.MONEY)

                # GROUP.TIME
                elif data_type_name == TYPE.TIME:
                    column_data = self._basic_data_select(GROUP.TIME)

                # GROUP.DATE
                elif data_type_name == TYPE.DATE:
                    column_data = self._basic_data_select(GROUP.DATE)

                # GROUP.DATETIME
                elif data_type_name == TYPE.SMALLDATETIME:
                    column_data = self._basic_data_select(GROUP.DATETIME)

                # GROUP.TIMESTAMP
                elif data_type_name in [TYPE.DATETIME, TYPE.DATETIME2]:
                    column_data = self._basic_data_select(GROUP.TIMESTAMP)

                # GROUP.DATETIMEOFFSET:
                elif data_type_name == TYPE.DATETIMEOFFSET:
                    column_data = self._basic_data_select(GROUP.DATETIMEOFFSET)

                # TYPE.BINARY
                elif data_type_name == TYPE.BINARY:
                    if data_type.length is None:
                        data_type.length = 1
                    column_data = os.urandom(random.randrange(data_type.length))

                # TYPE.VARBINARY / GROUP.BLOB
                elif data_type_name == TYPE.VARBINARY:
                    # GROUP.BLOB
                    if data_type.length == "MAX":
                        column_data = self._lob_data_select(GROUP.BLOB)
                    else:
                        if data_type.length is None:
                            data_type.length = 1
                        column_data = os.urandom(random.randrange(data_type.length))

                # Unsupported Data Type
                else:
                    column_data = None

            else:   # POSTGRESQL

                # GROUP.CHAR
                if data_type_name == TYPE.CHAR:
                    column_data = self._basic_data_select(GROUP.CHAR)

                # GROUP.VARCHAR
                elif data_type_name == TYPE.VARCHAR:
                    column_data = self._basic_data_select(GROUP.VARCHAR)

                # GROUP.SMALLINT
                elif data_type_name == TYPE.SMALLINT:
                    column_data = self._basic_data_select(GROUP.SMALLINT)

                # GROUP.INT
                elif data_type_name in [TYPE.INT, TYPE.INTEGER]:
                    column_data = self._basic_data_select(GROUP.INT)

                # GROUP.BIGINT
                elif data_type_name == TYPE.BIGINT:
                    column_data = self._basic_data_select(GROUP.BIGINT)

                # GROUP.DECIMAL
                elif data_type_name in [TYPE.DECIMAL, TYPE.NUMERIC]:
                    column_data = self._basic_data_select(GROUP.DECIMAL)

                # GROUP.FLOAT
                elif data_type_name == TYPE.REAL:
                    column_data = self._basic_data_select(GROUP.FLOAT)

                # GROUP.DOUBLE
                elif data_type_name == TYPE.DOUBLE_PRECISION_:
                    column_data = self._basic_data_select(GROUP.DOUBLE)

                # GROUP.MONEY
                elif data_type_name == TYPE.MONEY:
                    column_data = self._basic_data_select(GROUP.MONEY)

                # GROUP.TIME
                elif data_type_name == TYPE.TIME:
                    column_data = self._basic_data_select(GROUP.TIME)

                # GROUP.DATE
                elif data_type_name == TYPE.DATE:
                    column_data = self._basic_data_select(GROUP.DATE)

                # GROUP.TIMESTAMP
                elif data_type_name == TYPE.TIMESTAMP:
                    column_data = self._basic_data_select(GROUP.TIMESTAMP)

                # GROUP.INTERVAL
                elif data_type_name == TYPE.INTERVAL:

                    if data_type.fields is None:
                        data_type.fields = TYPE.interval_fields[random.randrange(len(TYPE.interval_fields))]

                    if data_type.fields in ["YEAR", "MONTH", "YEAR TO MONTH"]:
                        tmp_data = self._basic_data_select(GROUP.INTERVAL_YEAR_MONTH)
                    else:
                        tmp_data = self._basic_data_select(GROUP.INTERVAL_DAY_SECOND)

                    if data_type.fields == "YEAR":
                        column_data = f"{tmp_data[0]}"
                    elif data_type.fields == "MONTH":
                        column_data = f"{tmp_data[1]}"
                    elif data_type.fields == "DAY":
                        column_data = timedelta(days=tmp_data[0])
                    elif data_type.fields == "HOUR":
                        column_data = timedelta(hours=tmp_data[1])
                    elif data_type.fields == "MINUTE":
                        column_data = timedelta(minutes=tmp_data[2])
                    elif data_type.fields == "SECOND":
                        column_data = timedelta(seconds=tmp_data[3])
                    elif data_type.fields == "YEAR TO MONTH":
                        column_data = f"{tmp_data[0]}-{tmp_data[1]}"
                    elif data_type.fields == "DAY TO HOUR":
                        column_data = timedelta(days=tmp_data[0], hours=tmp_data[1])
                    elif data_type.fields == "DAY TO MINUTE":
                        column_data = timedelta(days=tmp_data[0], minutes=tmp_data[2])
                    elif data_type.fields == "DAY TO SECOND":
                        column_data = timedelta(days=tmp_data[0], seconds=tmp_data[3])
                    elif data_type.fields == "HOUR TO MINUTE":
                        column_data = timedelta(hours=tmp_data[1], minutes=tmp_data[2])
                    elif data_type.fields == "HOUR TO SECOND":
                        column_data = timedelta(hours=tmp_data[1], seconds=tmp_data[3])
                    elif data_type.fields == "MINUTE TO SECOND":
                        column_data = timedelta(minutes=tmp_data[2], seconds=tmp_data[3])
                    else:
                        column_data = None

                # GROUP.CLOB
                elif data_type_name == TYPE.TEXT:
                    column_data = self._lob_data_select(GROUP.CLOB)

                # GROUP.BLOB
                elif data_type_name == TYPE.BYTEA:
                    column_data = self._lob_data_select(GROUP.BLOB)

                # Unsupported Data Type
                else:
                    column_data = None

            row_data[column.name] = column_data

        return row_data

    def get_user_table_user_defined_data(self, columns, dbms_type):
        """
        사용자 정의 테이블의 Row 단위 Sample Data를 사용자가 정의한 데이터 파일에서 읽어 생성한다.
        :param columns: 작업 대상 Column List
        :param dbms_type: DBMS Type
        :return: Row Data
        """

        def get_binary_column_dict_value(column_name, key):

            try:
                self.file_data[column_name]    # Column 검증을 위해 대입없이 사용
            except KeyError:
                raise

            try:
                return self.file_data[column_name][key]
            except KeyError:
                print_error_msg(f"Invalid keyword in Column [ {column_name} ]. Expected 'MIN' or 'MAX'. ")

        row_data = {}

        for column in columns:

            if column.default is not None:
                continue

            column_name_upper = column.name.upper()

            data_type = column.type
            data_type_name = data_type.__class__.__name__

            try:
                if dbms_type == ORACLE:

                    if data_type_name == TYPE.INTERVAL:
                        if "year_precision" in data_type.__dict__:
                            tmp_data = self._basic_data_select(column_name_upper)
                            column_data = f"{tmp_data[0]}-{tmp_data[1]}"
                        else:
                            tmp_data = self._basic_data_select(column_name_upper)
                            column_data = timedelta(days=tmp_data[0], hours=tmp_data[1], minutes=tmp_data[2],
                                                    seconds=tmp_data[3], microseconds=tmp_data[4])

                    elif data_type_name in [TYPE.RAW, TYPE.LONG_RAW]:
                        min_length = get_binary_column_dict_value(column_name_upper, "MIN")
                        max_length = get_binary_column_dict_value(column_name_upper, "MAX")
                        column_data = os.urandom(random.randrange(min_length, max_length+1))

                    elif data_type_name in [TYPE.CLOB, TYPE.NCLOB, TYPE.BLOB, TYPE.LONG]:
                        column_data = self._lob_data_select(column_name_upper)

                    else:
                        column_data = self._basic_data_select(column_name_upper)

                elif dbms_type == MYSQL:

                    if data_type_name in [TYPE.BINARY, TYPE.VARBINARY, TYPE.TINYBLOB, TYPE.BLOB]:
                        min_length = get_binary_column_dict_value(column_name_upper, "MIN")
                        max_length = get_binary_column_dict_value(column_name_upper, "MAX")
                        column_data = os.urandom(random.randrange(min_length, max_length + 1))

                    elif data_type_name in [TYPE.MEDIUMTEXT, TYPE.LONGTEXT, TYPE.MEDIUMBLOB, TYPE.LONGBLOB]:
                        column_data = self._lob_data_select(column_name_upper)

                    else:
                        column_data = self._basic_data_select(column_name_upper)

                elif dbms_type == SQLSERVER:

                    if data_type_name in [TYPE.VARCHAR, TYPE.NVARCHAR]:
                        if data_type.length == "MAX":
                            column_data = self._lob_data_select(column_name_upper)
                        else:
                            column_data = self._basic_data_select(column_name_upper)

                    elif data_type_name in [TYPE.BINARY, TYPE.VARBINARY]:
                        if data_type_name == TYPE.VARBINARY and data_type.length == "MAX":
                            column_data = self._lob_data_select(column_name_upper)
                        else:
                            column_data = self._basic_data_select(column_name_upper)

                    else:
                        column_data = self._basic_data_select(column_name_upper)

                else:   # PostgreSQL

                    if data_type_name in [TYPE.TEXT, TYPE.BYTEA]:
                        column_data = self._lob_data_select(column_name_upper)

                    else:
                        column_data = self._basic_data_select(column_name_upper)

            except KeyError:
                print_error_msg(f"Not found Column [ {column.name} ] in data file.")

            row_data[column.name] = column_data

        return row_data

