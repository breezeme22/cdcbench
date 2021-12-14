
import logging
import random
import re
import os
import yaml
import yaml.scanner

from typing import Dict, Any, List, NoReturn
from sqlalchemy.schema import Column

from lib.common import print_error
from lib.definition import (OracleDataType as oracle, MySqlDataType as mysql,
                            SqlServerDataType as sqlserver, PostgresqlDataType as postgresql)
from lib.globals import *


_DATA_DIRECTORY = "data"
_LOB_DATA_DIRECTORY = "lob_files"
_DATA_FILE_EXT = ".yaml"
_DEFAULT_DATA_FILE_NAME = "sample_data.yaml"


class DataManager:

    def __init__(self, table_name: str, custom_data: bool):

        self.logger = logging.getLogger(CDCBENCH)
        self.custom_data = custom_data

        if custom_data:
            self.data_file_name = f"{table_name.lower()}{_DATA_FILE_EXT}"
        else:
            self.data_file_name = _DEFAULT_DATA_FILE_NAME

        self.logger.debug(f"Data file: {self.data_file_name}")

        try:
            with open(os.path.join(_DATA_DIRECTORY, self.data_file_name), "r", encoding="utf-8") as f:
                self.file_content: Dict = yaml.load(f.read(), yaml.SafeLoader)

        except FileNotFoundError:
            print_error(f"Data file [ {self.data_file_name} ] does not exist.")

        except yaml.scanner.ScannerError as SE:
            print_error(f"Invalid YAML format of data file [ {self.data_file_name} ] \n"
                        f"  * problem: {SE.problem} \n"
                        f"  * line {SE.problem_mark.line + 1}, column {SE.problem_mark.column + 1}")

        except yaml.YAMLError as YE:
            print_error(f"Invalid YAML format of data file [ {YE.args[1].name} ] \n"
                        f"  * line {YE.args[1].line + 1}, column {YE.args[1].column + 1}")

    def _get_scalar_data(self, key: str, nullable: bool) -> Any:
        try:
            if key and len(self.file_content[key]) > 0:
                chosen_data = random.choice(self.file_content[key])
                if not nullable:
                    while chosen_data is None:
                        chosen_data = random.choice(self.file_content[key])
                return chosen_data
            else:
                return None
        except KeyError:
            print_error(f"Invalid key(column) name [ {key} ] in data file [ {self.data_file_name} ]", True)

    def _get_lob_data(self, key: str) -> Any:

        def _read_lob_file(file_name: str):

            if not file_name:
                return None

            try:
                file_path = os.path.join(_DATA_DIRECTORY, _LOB_DATA_DIRECTORY, file_name)

                # File 확장자에 따라 File Read 방식 구분
                if file_name.split(".")[1] == "txt":
                    with open(file_path, "r", encoding="utf-8") as f:
                        return f.read()
                else:
                    with open(file_path, "rb") as f:
                        return f.read()

            except FileNotFoundError:
                print_error(f"LOB data file [ {file_name} ] does not exist.", True)

            except UnicodeDecodeError as UDE:
                # print("... Fail")
                print_error(f"'{UDE.encoding}' codec can't decode file [ {file_name} ]. \n"
                            f"  * Note. The LOB test file with string must be UTF-8 (without BOM) encoding.", True)

        try:
            if key and len(self.file_content[key]) > 0:
                lob_file_name = random.choice(self.file_content[key])
                return _read_lob_file(lob_file_name)
            else:
                return None
        except KeyError:
            print_error(f"Invalid key(column) name [ {key} ] in data file [ {self.data_file_name} ]", True)

    def _get_binary_data(self, key: str, nullable: bool) -> Any:
        if key:
            try:
                binary_data = self.file_content[key]
                if isinstance(binary_data, list):
                    return self._get_scalar_data(key, nullable)
                elif isinstance(binary_data, dict):
                    try:
                        len_min = binary_data["MIN"]
                        len_max = binary_data["MAX"]
                        return os.urandom(random.randrange(len_min, len_max+1))
                    except KeyError:
                        print_error(f"Invalid key name in binary column [ {key} ]. Expected 'MIN' or 'MAX'. ", True)
                else:
                    print_error("Binary data supports only the following types of YAML format: List, Dictionary", True)
            except KeyError:
                print_error(f"Invalid key(column) name [ {key} ] in data file [ {self.data_file_name} ]", True)
        else:
            return None

    def _get_interval_data(self, key: str, nullable: bool, interval_filed: str) -> Any:
        interval_data = self._get_scalar_data(key, nullable)

        if interval_data:
            if interval_filed in ("YEAR", "MONTH", "YEAR TO MONTH"):
                interval_expr = re.compile("-?[0-9]{1,9}-[0-9]{1,2}$")
            else:
                interval_expr = re.compile("^-?[0-9]{1,9} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}\\.?[0-9]{0,9}?$")

            if interval_expr.match(interval_data):
                return interval_data
            else:
                print_error(f"Invalid interval data format of [ {key} ] in data file. \n"
                            f"  * Data: {interval_data}", True)

    def column_name_based_get_data(self, columns: List[Column], dbms: str) -> Dict[str, Any]:

        row_data = {}

        for column in columns:

            if column.default is not None:
                continue

            data_type = column.type
            data_type_name = data_type.__class__.__name__
            dialect_data_type_name = f"{dbms}.{data_type_name}"
            column_name_upper = column.name.upper()

            # SQLServer VARCHAR/VARBINARY는 MAX 옵션에 따라 데이터 처리가 달라지므로 예외처리
            if dialect_data_type_name in (f"{SQLSERVER}.{sqlserver.VARCHAR}", f"{SQLSERVER}.{sqlserver.VARBINARY}"):
                if data_type.length == "MAX":
                    column_data = self._get_lob_data(column_name_upper)
                elif data_type_name == sqlserver.VARBINARY:
                    column_data = self._get_binary_data(column_name_upper, column.nullable)
                else:
                    column_data = self._get_scalar_data(column_name_upper, column.nullable)

            # PostgreSQL Binary 데이터는 BYTEA 타입 하나로 모두 처리되어 예외처리
            elif dialect_data_type_name == f"{POSTGRESQL}.{postgresql.BYTEA}":
                tmp_data = self._get_binary_data(column_name_upper, column.nullable)
                if isinstance(tmp_data, bytes):
                    column_data = tmp_data
                else:
                    column_data = self._get_lob_data(column_name_upper)

            # Oracle Interval Data type 예외처리
            elif dialect_data_type_name == f"{ORACLE}.{oracle.INTERVAL}":
                if "year_precision" in data_type.__dict__:
                    column_data = self._get_interval_data(column_name_upper, column.nullable, "YEAR TO MONTH")
                else:
                    column_data = self._get_interval_data(column_name_upper, column.nullable, "DAY TO SECOND")

            # PostgreSQL Interval Data type 예외처리
            elif dialect_data_type_name == f"{POSTGRESQL}.{postgresql.INTERVAL}":
                column_data = self._get_interval_data(column_name_upper, column.nullable, data_type.fields)

            # Binary data type 예외처리
            elif data_type_name in (oracle.RAW, mysql.BINARY, mysql.VARBINARY, mysql.TINYBLOB, mysql.BLOB,
                                    sqlserver.BINARY):
                column_data = self._get_binary_data(column_name_upper, column.nullable)

            # LOB data type 예외처리
            elif data_type_name in (oracle.CLOB, oracle.NCLOB, oracle.LONG, oracle.BLOB, oracle.LONG_RAW,
                                    mysql.MEDIUMTEXT, mysql.LONGTEXT, mysql.MEDIUMBLOB, mysql.LONGBLOB):
                column_data = self._get_lob_data(column_name_upper)

            else:
                column_data = self._get_scalar_data(column_name_upper, column.nullable)

            row_data[column.name] = column_data

        return row_data

    def data_type_based_get_data(self, columns: List[Column], dbms: str) -> Dict[str, Any]:

        row_data = {}

        for column in columns:

            if column.default is not None:
                continue

            data_type = column.type
            data_type_name = data_type.__class__.__name__

            if dbms == ORACLE:

                if data_type_name == oracle.NCHAR:
                    column_data = self._get_scalar_data(GROUP.CHAR, column.nullable)

                elif data_type_name in (oracle.VARCHAR2, oracle.NVARCHAR):
                    column_data = self._get_scalar_data(GROUP.VARCHAR, column.nullable)

                elif data_type_name == oracle.NUMBER:
                    column_data = self._get_scalar_data(GROUP.NUMBER, column.nullable)

                elif data_type_name == oracle.BINARY_FLOAT:
                    column_data = self._get_scalar_data(GROUP.FLOAT, column.nullable)

                elif data_type_name == oracle.BINARY_DOUBLE:
                    column_data = self._get_scalar_data(GROUP.DOUBLE, column.nullable)

                elif data_type_name == oracle.DATE:
                    column_data = self._get_scalar_data(GROUP.DATETIME, column.nullable)

                elif data_type_name == oracle.TIMESTAMP:
                    column_data = self._get_scalar_data(GROUP.TIMESTAMP, column.nullable)

                elif data_type_name == oracle.INTERVAL:
                    if "year_precision" in data_type.__dict__:
                        column_data = self._get_interval_data(GROUP.INTERVAL_YEAR_MONTH, column.nullable,
                                                              "YEAR TO MONTH")
                    else:
                        column_data = self._get_interval_data(GROUP.INTERVAL_DAY_SECOND, column.nullable,
                                                              "DAY TO SECOND")

                elif data_type_name in (oracle.RAW, oracle.LONG_RAW, oracle.LONG_RAW_):
                    column_data = self._get_binary_data(GROUP.BINARY, column.nullable)

                elif data_type_name in (oracle.CLOB, oracle.NCLOB, oracle.LONG):
                    column_data = self._get_lob_data(GROUP.CLOB)

                elif data_type_name == oracle.BLOB:
                    column_data = self._get_lob_data(GROUP.BLOB)

                else:
                    column_data = self._get_scalar_data(getattr(GROUP, data_type_name), column.nullable)

            elif dbms in MYSQL:

                if data_type_name in (mysql.NCHAR, mysql.TINYTEXT):
                    column_data = self._get_scalar_data(GROUP.CHAR, column.nullable)

                elif data_type_name in (mysql.NVARCHAR, mysql.TEXT):
                    column_data = self._get_scalar_data(GROUP.VARCHAR, column.nullable)

                elif data_type_name == mysql.YEAR:
                    column_data = self._get_scalar_data(GROUP.DATE, column.nullable).YEAR

                elif data_type_name in (mysql.BINARY, mysql.VARBINARY, mysql.TINYBLOB, mysql.BLOB):
                    column_data = self._get_binary_data(GROUP.BINARY, column.nullable)

                elif data_type_name in (mysql.MEDIUMTEXT, mysql.LONGTEXT):
                    column_data = self._get_lob_data(GROUP.CLOB)

                elif data_type_name in (mysql.MEDIUMBLOB, mysql.LONGBLOB):
                    column_data = self._get_lob_data(GROUP.BLOB)

                else:
                    column_data = self._get_scalar_data(getattr(GROUP, data_type_name), column.nullable)

            elif dbms in SQLSERVER:

                if data_type_name == sqlserver.NCHAR:
                    column_data = self._get_scalar_data(GROUP.CHAR, column.nullable)

                elif data_type_name in (sqlserver.VARCHAR, sqlserver.NVARCHAR):
                    if data_type.length == "MAX":
                        column_data = self._get_lob_data(GROUP.CLOB)
                    else:
                        column_data = self._get_scalar_data(GROUP.VARCHAR, column.nullable)

                elif data_type_name == sqlserver.REAL:
                    column_data = self._get_scalar_data(GROUP.FLOAT, column.nullable)

                elif data_type_name == sqlserver.FLOAT:
                    column_data = self._get_scalar_data(GROUP.DOUBLE, column.nullable)

                elif data_type_name == sqlserver.SMALLMONEY:
                    column_data = self._get_scalar_data(GROUP.MONEY, column.nullable)

                elif data_type_name == sqlserver.SMALLDATETIME:
                    column_data = self._get_scalar_data(GROUP.DATETIME, column.nullable)

                elif data_type_name in (sqlserver.DATETIME, sqlserver.DATETIME2):
                    column_data = self._get_scalar_data(GROUP.TIMESTAMP, column.nullable)

                elif data_type_name == sqlserver.BINARY:
                    column_data = self._get_binary_data(GROUP.BINARY, column.nullable)

                elif data_type_name == sqlserver.VARBINARY:
                    if data_type.length == "MAX":
                        column_data = self._get_lob_data(GROUP.BLOB)
                    else:
                        column_data = self._get_binary_data(GROUP.BINARY, column.nullable)

                else:
                    column_data = self._get_scalar_data(getattr(GROUP, data_type_name), column.nullable)

            else:   # PostgreSQL

                if data_type_name == postgresql.REAL:
                    column_data = self._get_scalar_data(GROUP.FLOAT, column.nullable)

                elif data_type_name == postgresql.DOUBLE_PRECISION_:
                    column_data = self._get_scalar_data(GROUP.DOUBLE, column.nullable)

                elif data_type_name == postgresql.INTERVAL:
                    if data_type.fields is None:
                        data_type.fields = postgresql.interval_fields[random.randrange(len(postgresql.interval_fields))]

                    if data_type.fields in ("YEAR", "MONTH", "YEAR TO MONTH"):
                        column_data = self._get_interval_data(GROUP.INTERVAL_YEAR_MONTH, column.nullable,
                                                              data_type.fields)
                    else:
                        column_data = self._get_interval_data(GROUP.INTERVAL_DAY_SECOND, column.nullable,
                                                              data_type.fields)

                elif data_type_name == postgresql.TEXT:
                    column_data = self._get_lob_data(GROUP.CLOB)

                elif data_type_name == postgresql.BYTEA:
                    column_data = self._get_lob_data(GROUP.BLOB)

                else:
                    column_data = self._get_scalar_data(getattr(GROUP, data_type_name), column.nullable)

            row_data[column.name] = column_data

        return row_data

    def get_row_data(self, columns: List[Column], dbms: str) -> Dict:
        if self.custom_data:
            return self.column_name_based_get_data(columns, dbms)
        else:
            return self.data_type_based_get_data(columns, dbms)

    def get_list_row_data(self, column: List[Column], dbms: str, record_count: int) -> List:
        if self.custom_data:
            return [self.column_name_based_get_data(column, dbms) for _ in range(record_count)]
        else:
            return [self.data_type_based_get_data(column, dbms) for _ in range(record_count)]


def Group(group_name: str) -> str:
    return f"GROUP.{group_name}"


class GROUP:
    """Data Type Group"""

    CHAR = Group("CHAR")
    VARCHAR = Group("VARCHAR")

    NUMBER = Group("NUMBER")
    BIT = Group("BIT")
    TINYINT = Group("TINYINT")
    SMALLINT = Group("SMALLINT")
    MEDIUMINT = Group("MEDIUMINT")
    INT = Group("INT")
    INTEGER = INT
    BIGINT = Group("BIGINT")
    DECIMAL = Group("DECIMAL")
    NUMERIC = DECIMAL
    FLOAT = Group("FLOAT")
    DOUBLE = Group("DOUBLE")
    MONEY = Group("MONEY")

    TIME = Group("TIME")
    DATE = Group("DATE")
    DATETIME = Group("DATETIME")
    TIMESTAMP = Group("TIMESTAMP")
    INTERVAL_YEAR_MONTH = Group("INTERVAL_YEAR_MONTH")
    INTERVAL_DAY_SECOND = Group("INTERVAL_DAY_SECOND")
    DATETIMEOFFSET = Group("DATETIMEOFFSET")

    BINARY = Group("BINARY")

    CLOB = Group("CLOB")
    BLOB = Group("BLOB")

    ROWID = Group("ROWID")
