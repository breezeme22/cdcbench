
import os

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from pyparsing import (Word, delimitedList, Optional, Group, alphas, nums, alphanums, OneOrMore, CaselessKeyword,
                       Suppress, ParseException, tokenMap, MatchFirst, ParseResults)
from sqlalchemy import Column, Sequence, PrimaryKeyConstraint, String, LargeBinary, types as sql_types
from sqlalchemy.dialects import oracle, mysql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import as_declarative
from typing import List, Dict, Any, Type, NoReturn, Union

from lib.common import print_error
from lib.connection import ConnectionManager
from lib.globals import *
from lib.logger import LoggerManager


DEFINITION_DIR = "definition"


@dataclass
class TableCustomAttributes:
    constraint_type: str
    identity_column: str


@as_declarative()
class OracleBase:
    pass


@as_declarative()
class MysqlBase:
    pass


@as_declarative()
class SqlserverBase:
    pass


@as_declarative()
class PostgresqlBase:
    pass


class CubridBase:
    tables = {}


@as_declarative()
class TiberoBase:
    tables = {}


# Parsing support keyword
LBRACKET = Suppress("(").setName("LBRACKET")
RBRACKET = Suppress(")").setName("RBRACKET")
OPT_LBRACKET = Optional(Suppress("(")).setName("LBRACKET")
OPT_RBRACKET = Optional(Suppress(")")).setName("RBRACKET")

DATA_TYPE = "data_type"
DATA_LENGTH = "data_length"

SEQUENCE = "SEQUENCE"
IDENTITY = "IDENTITY"

# Database reserved word (keyword)
UNSIGNED = "UNSIGNED"
SIGNED = "SIGNED"
ZEROFILL = "ZEROFILL"
NULL = "NULL"
NOT_NULL = "NOT NULL"


def parse_definition_file(dbms: str, file_path: str) -> ParseResults:
    kw_constraint = CaselessKeyword("CONSTRAINT")
    kw_constraint.setName("CONSTRAINT")

    # CONSTRAINT 키워드가 구조상으로 Object name과 동일하므로, 해당 키워드는 Object name 파싱에서 제외
    object_name = ~kw_constraint + Word(alphas, alphanums + "-_\"$?")
    object_name.setParseAction(lambda toks: str(toks[0]))

    column_name = object_name.setResultsName("column_name")
    column_name.setName("column_name")

    data_type: MatchFirst or None = None
    if dbms == ORACLE:
        data_type = OracleDataType.get_data_type_parser()
    elif dbms == MYSQL:
        data_type = MySQLDataType.get_data_type_parser()
    data_type.setName("data_type")

    nullable = Optional(CaselessKeyword(NOT_NULL) | CaselessKeyword(NULL), default=NULL).setResultsName("nullable")
    nullable.setName("nullable")
    nullable.setParseAction(lambda toks: True if toks.nullable == NULL else False)

    sequence = Optional(CaselessKeyword(SEQUENCE)).setResultsName("sequence")
    sequence.setName("sequence")
    sequence.setParseAction(lambda toks: True if toks.sequence != "" else False)

    identity = Optional(CaselessKeyword(IDENTITY)).setResultsName("identity")
    identity.setName("identity")
    identity.setParseAction(lambda toks: True if toks.identity != "" else False)

    column = Group(column_name + data_type + (nullable & sequence & identity)) + Suppress(Optional(","))
    column.setName("column")

    columns = OneOrMore(column).setResultsName("columns")
    columns.setName("columns")

    kw_primary_key = CaselessKeyword(PRIMARY_KEY)
    kw_primary_key.setName(PRIMARY_KEY)
    kw_unique = CaselessKeyword(UNIQUE)
    kw_unique.setName(UNIQUE)
    kw_non_key = CaselessKeyword(NON_KEY)
    kw_non_key.setName(NON_KEY)

    constraint_detail = ((kw_primary_key | kw_unique | kw_non_key).setResultsName("constraint_type")
                         + LBRACKET
                         + delimitedList(object_name).setResultsName("constraint_columns")
                         + RBRACKET)
    constraint_detail.setName("constraint_detail")

    constraint = Optional(Group(kw_constraint + object_name.setResultsName("constraint_name") + constraint_detail)
                          .setResultsName("constraint"), default=None)
    constraint.setName("constraint")

    table_name = object_name.setResultsName("table_name")
    table_name.setName("table_name")

    # Non key의 경우 마지막 RBRACKET을 columns에서 체크하고 넘어가버려서 OPT_RBRACKET 으로 사용 (RBRACKET 사용시 파싱 에러)
    table = Group(table_name + LBRACKET + columns + constraint + RBRACKET).setResultsName("table")
    table.setName("table")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            table_definition = f.read()

        return table.parseString(table_definition)

    except FileNotFoundError:
        print_error(f"Definition file [ {file_path} ] does not exist.")

    except ParseException as PE:
        PE.file_name = file_path
        print_error(
            f"You have an error in Table Definition syntax. An error exists in the following: \n"
            f"    definition file: {PE.file_name} \n"
            f"    message: {PE.msg} \n"
            f"    line: {PE.lineno}, col: {PE.col} (position {PE.loc}) \n"
            f"    near error point: {PE.line} \n"
        )


# Common Parser
data_length = (LBRACKET
               + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH)
               + RBRACKET)

opt_data_length = (OPT_LBRACKET
                   + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH)
                   + OPT_RBRACKET)

opt_dual_data_length = (OPT_LBRACKET
                        + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH)
                        + OPT_RBRACKET)


class DataType(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def get_data_type_parser(cls) -> MatchFirst:
        pass

    @classmethod
    @abstractmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:
        pass


class OracleDataType(DataType):

    CHAR = "CHAR"
    NCHAR = "NCHAR"
    VARCHAR = "VARCHAR"
    VARCHAR2 = "VARCHAR2"
    NVARCHAR2 = "NVARCHAR2"
    LONG = "LONG"

    NUMBER = "NUMBER"
    BINARY_FLOAT = "BINARY_FLOAT"
    BINARY_DOUBLE = "BINARY_DOUBLE"
    FLOAT = "FLOAT"

    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"

    RAW = "RAW"
    LONG_RAW = "LONG RAW"

    CLOB = "CLOB"
    NCLOB = "NCLOB"
    BLOB = "BLOB"

    ROWID = "ROWID"

    class CustomTypes:

        class CHAR(String):
            """크기 기준을 CHAR로 생성하는 CHAR (sqlalchemy.dialects의 CHAR 타입의 경우 크기 기준이 BYTE로만 설정 가능)"""

            def __init__(self, length):
                super().__init__(length)
                self.__class__.__name__ = "CHAR"

        @staticmethod
        @compiles(CHAR)
        def compile_char_len_char(type_, compiler, **kw):
            return f"CHAR({type_.length} CHAR)"

        class VARCHAR2(String):
            """
            크기 기준을 BYTE로 생성하는 VARCHAR2 (sqlalchemy.dialects의 VARCHAR2 타입의 경우 크기 기준이 CHAR로만 설정 가능)
            """

            def __init__(self, length):
                super().__init__(length)
                self.__class__.__name__ = "VARCHAR2"

        @staticmethod
        @compiles(VARCHAR2)
        def compile_varchar2_len_byte(type_, compiler, **kw):
            return f"VARCHAR2({type_.length} BYTE)"

        class INTERVAL(sql_types.TypeEngine):
            def __init__(self, year_precision=None):
                """Construct an INTERVAL.

                    Note that only DAY TO SECOND intervals are currently supported.
                    This is due to a lack of support for YEAR TO MONTH intervals
                    within available DBAPIs (cx_oracle and zxjdbc).

                    :param year_precision: the day precision value.  this is the number of
                      digits to store for the day field.  Defaults to "2"

                """
                self.year_precision = year_precision
                self.__class__.__name__ = "INTERVAL"

        @staticmethod
        @compiles(INTERVAL)
        def compile_interval_year_month(type_, compiler, **kw):
            return "INTERVAL YEAR{} TO MONTH".format(
                type_.year_precision is not None and "(%d)" % type_.year_precision or ""
            )

        class LONG_RAW(LargeBinary):
            def __init__(self):
                super().__init__()
                self.__class__.__name__ = "LONG RAW"

        @staticmethod
        @compiles(LONG_RAW)
        def compile_long_raw(type_, complier, **kw):
            return "LONG RAW"

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:

        char = (CaselessKeyword(cls.CHAR).setResultsName(DATA_TYPE)
                + LBRACKET
                + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH)
                + (Optional(CaselessKeyword("BYTE") | CaselessKeyword("CHAR"), default="BYTE")
                   .setResultsName("length_semantics"))
                + RBRACKET)
        char.setName(cls.CHAR)

        nchar = CaselessKeyword(cls.NCHAR).setResultsName(DATA_TYPE) + data_length
        nchar.setName(cls.NCHAR)

        varchar2 = ((CaselessKeyword(cls.VARCHAR2) | CaselessKeyword(cls.VARCHAR)).setResultsName(DATA_TYPE)
                    + LBRACKET
                    + Word(nums).setParseAction().setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH)
                    + (Optional(CaselessKeyword("BYTE") | CaselessKeyword("CHAR"), default="BYTE")
                       .setResultsName("length_semantics"))
                    + RBRACKET)
        varchar2.setName(cls.VARCHAR2)

        nvarchar2 = CaselessKeyword(cls.NVARCHAR2).setResultsName(DATA_TYPE) + data_length
        nvarchar2.setName(cls.NVARCHAR2)

        long = CaselessKeyword(cls.LONG).setResultsName(DATA_TYPE)
        long.setName(cls.LONG)

        number = CaselessKeyword(cls.NUMBER).setResultsName(DATA_TYPE) + opt_dual_data_length
        number.setName(cls.NUMBER)

        binary_float = CaselessKeyword(cls.BINARY_FLOAT).setResultsName(DATA_TYPE)
        binary_float.setName(cls.BINARY_FLOAT)

        binary_double = CaselessKeyword(cls.BINARY_DOUBLE).setResultsName(DATA_TYPE)
        binary_double.setName(cls.BINARY_DOUBLE)

        m_float = CaselessKeyword(cls.FLOAT).setResultsName(DATA_TYPE) + opt_data_length
        m_float.setName(cls.FLOAT)

        date = CaselessKeyword(cls.DATE).setResultsName(DATA_TYPE)
        date.setName(cls.DATE)

        timestamp = CaselessKeyword(cls.TIMESTAMP).setResultsName(DATA_TYPE) + opt_data_length
        timestamp.setName(cls.TIMESTAMP)

        interval_year_month = (CaselessKeyword(cls.INTERVAL).setResultsName(DATA_TYPE)
                               + CaselessKeyword("YEAR").setResultsName("type")
                               + OPT_LBRACKET
                               + (Optional(Word(nums).setParseAction(tokenMap(int)), default=None)
                                  .setResultsName("year_precision"))
                               + CaselessKeyword("TO MONTH"))
        interval_year_month.setName("INTERVAL YEAR TO MONTH")

        interval_day_second = (CaselessKeyword(cls.INTERVAL).setResultsName(DATA_TYPE)
                               + CaselessKeyword("DAY").setResultsName("type")
                               + (Optional(Word(nums).setParseAction(tokenMap(int)), default=None)
                                  .setResultsName("day_precision"))
                               + CaselessKeyword("TO SECOND")
                               + OPT_LBRACKET
                               + (Optional(Word(nums).setParseAction(tokenMap(int)), default=None)
                                  .setResultsName("second_precision"))
                               + OPT_RBRACKET)
        interval_day_second.setName("INTERVAL DAY TO SECOND")

        raw = CaselessKeyword(cls.RAW).setResultsName(DATA_TYPE) + data_length
        raw.setName(cls.RAW)

        long_raw = CaselessKeyword(cls.LONG_RAW).setResultsName(DATA_TYPE)
        long_raw.setName(cls.LONG_RAW)

        clob = CaselessKeyword(cls.CLOB).setResultsName(DATA_TYPE)
        clob.setName(cls.CLOB)

        nclob = CaselessKeyword(cls.NCLOB).setResultsName(DATA_TYPE)
        nclob.setName(cls.NCLOB)

        blob = CaselessKeyword(cls.BLOB).setResultsName(DATA_TYPE)
        blob.setName(cls.BLOB)

        rowid = CaselessKeyword(cls.ROWID).setResultsName(DATA_TYPE)
        rowid.setName(cls.ROWID)

        return (long_raw | char | nchar | varchar2 | nvarchar2 | long | number | binary_float | binary_double | m_float
                | date | timestamp | interval_year_month | interval_day_second | raw | clob | nclob | blob | rowid)

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:

        def oracle_character_length_semantic_type():
            column.data_type = (OracleDataType.VARCHAR2
                                if column.data_type == OracleDataType.VARCHAR else column.data_type)
            if column.length_semantics == "CHAR":
                data_type = getattr(cls.CustomTypes, column.data_type)
            else:
                data_type = getattr(oracle, column.data_type)
            return data_type(column.data_length)

        def oracle_number_type():
            if len(column.data_length) == 1:
                return oracle.NUMBER(column.data_length[0])
            elif len(column.data_length) == 2:
                return oracle.NUMBER(column.data_length[0], column.data_length[1])
            else:
                return oracle.NUMBER

        def oracle_interval_type():
            if column.type == "YEAR":
                return cls.CustomTypes.INTERVAL(column.year_precision)
            else:   # DAY
                if column.day_precision is None and column.second_precision is None:
                    return oracle.INTERVAL
                elif column.day_precision is not None and column.second_precision is None:
                    return oracle.INTERVAL(day_precision=column.year_day_precision)
                elif column.day_precision is None and column.second_precision is not None:
                    return oracle.INTERVAL(second_precision=column.second_precision)
                else:
                    return oracle.INTERVAL(day_precision=column.year_day_precision,
                                           second_precision=column.second_precision)

        data_type_objects = {
            cls.CHAR: oracle_character_length_semantic_type,
            cls.NCHAR: oracle.NCHAR(column.data_length),
            cls.VARCHAR2: oracle_character_length_semantic_type,
            cls.VARCHAR: oracle_character_length_semantic_type,
            cls.NVARCHAR2: oracle.NVARCHAR2(column.data_length),
            cls.LONG: oracle.LONG,
            cls.NUMBER: oracle_number_type,
            cls.BINARY_FLOAT: oracle.BINARY_FLOAT,
            cls.BINARY_DOUBLE: oracle.BINARY_DOUBLE,
            cls.FLOAT: oracle.FLOAT,
            cls.DATE: oracle.DATE,
            cls.TIMESTAMP: oracle.TIMESTAMP,
            cls.INTERVAL: oracle_interval_type,
            cls.RAW: oracle.RAW(column.data_length),
            cls.LONG_RAW: cls.CustomTypes.LONG_RAW,
            cls.CLOB: oracle.CLOB,
            cls.NCLOB: oracle.NCLOB,
            cls.BLOB: oracle.BLOB,
            cls.ROWID: oracle.ROWID
        }

        result = data_type_objects[column.data_type]
        return result() if callable(result) else result


class MySQLDataType(DataType):

    CHAR = "CHAR"
    NCHAR = "NCHAR"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    TINYTEXT = "TINYTEXT"
    TEXT = "TEXT"
    MEDIUMTEXT = "MEDIUMTEXT"
    LONGTEXT = "LONGTEXT"

    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    TINYBLOB = "TINYBLOB"
    BLOB = "BLOB"
    MEDIUMBLOB = "MEDIUMBLOB"
    LONGBLOB = "LONGBLOB"

    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    MEDIUMINT = "MEDIUMINT"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"

    TIME = "TIME"
    DATE = "DATE"
    YEAR = "YEAR"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:

        char = CaselessKeyword(cls.CHAR).setResultsName(DATA_TYPE) + opt_data_length
        char.setName(cls.CHAR)

        nchar = CaselessKeyword(cls.NCHAR).setResultsName(DATA_TYPE) + opt_data_length
        nchar.setName(cls.NCHAR)
        varchar = CaselessKeyword(cls.VARCHAR).setResultsName(DATA_TYPE) + data_length
        varchar.setName(cls.VARCHAR)

        nvarchar = CaselessKeyword(cls.NVARCHAR).setResultsName(DATA_TYPE) + data_length
        nvarchar.setName(cls.NVARCHAR)

        tinytext = CaselessKeyword(cls.TINYTEXT).setResultsName(DATA_TYPE)
        tinytext.setName(cls.TINYTEXT)

        text = CaselessKeyword(cls.TEXT).setResultsName(DATA_TYPE) + opt_data_length
        text.setName(cls.TEXT)

        mediumtext = CaselessKeyword(cls.MEDIUMTEXT).setResultsName(DATA_TYPE)
        mediumtext.setName(cls.MEDIUMTEXT)

        longtext = CaselessKeyword(cls.LONGTEXT).setResultsName(DATA_TYPE)
        longtext.setName(cls.LONGTEXT)

        binary = (CaselessKeyword(cls.BINARY).setResultsName(DATA_TYPE) + opt_data_length)
        binary.setName(cls.BINARY)

        varbinary = CaselessKeyword(cls.VARBINARY).setResultsName(DATA_TYPE) + data_length
        varbinary.setName(cls.VARBINARY)

        tinyblob = CaselessKeyword(cls.TINYBLOB).setResultsName(DATA_TYPE)
        tinyblob.setName(cls.TINYBLOB)

        blob = CaselessKeyword(cls.BLOB).setResultsName(DATA_TYPE) + opt_data_length
        blob.setName(cls.BLOB)

        mediumblob = CaselessKeyword(cls.MEDIUMBLOB).setResultsName(DATA_TYPE)
        mediumblob.setName(cls.MEDIUMBLOB)

        longblob = CaselessKeyword(cls.LONGBLOB).setResultsName(DATA_TYPE)
        longblob.setName(cls.LONGBLOB)

        def _signed_replace_bool(toks):
            return True if toks.unsigned == UNSIGNED else False

        def _zerofill_replace_bool(toks):
            return True if toks.zerofill != "" else False

        signed_opt = (Optional(CaselessKeyword(UNSIGNED) | CaselessKeyword(SIGNED), default=SIGNED)
                      .setResultsName(UNSIGNED.lower()).setParseAction(_signed_replace_bool))
        signed_opt.setName("SIGNED | UNSIGNED")

        zerofill_opt = (Optional(CaselessKeyword(ZEROFILL))
                        .setResultsName(ZEROFILL.lower()).setParseAction(_zerofill_replace_bool))
        zerofill_opt.setName(ZEROFILL)

        tinyint = (CaselessKeyword(cls.TINYINT).setResultsName(DATA_TYPE) + opt_data_length
                   + (signed_opt & zerofill_opt))
        tinyint.setName(cls.TINYINT)

        smallint = (CaselessKeyword(cls.SMALLINT).setResultsName(DATA_TYPE) + opt_data_length
                    + (signed_opt & zerofill_opt))
        smallint.setName(cls.SMALLINT)

        mediumint = (CaselessKeyword(cls.MEDIUMINT).setResultsName(DATA_TYPE) + opt_data_length
                     + (signed_opt & zerofill_opt))
        mediumint.setName(cls.MEDIUMINT)

        integer = ((CaselessKeyword(cls.INTEGER) | CaselessKeyword(cls.INT)).setResultsName(DATA_TYPE)
                   + opt_data_length + (signed_opt & zerofill_opt))
        integer.setName(cls.INTEGER)

        bigint = (CaselessKeyword(cls.BIGINT).setResultsName(DATA_TYPE) + opt_data_length
                  + (signed_opt & zerofill_opt))
        bigint.setName(cls.BIGINT)

        decimal = ((CaselessKeyword(cls.DECIMAL) | CaselessKeyword(cls.NUMERIC)).setResultsName(DATA_TYPE)
                   + opt_dual_data_length + (signed_opt & zerofill_opt))
        decimal.setName(cls.DECIMAL)

        m_float = (CaselessKeyword(cls.FLOAT).setResultsName(DATA_TYPE) + opt_dual_data_length
                   + (signed_opt & zerofill_opt))
        m_float.setName(cls.FLOAT)

        double = (CaselessKeyword(cls.DOUBLE).setResultsName(DATA_TYPE) + opt_dual_data_length
                  + (signed_opt & zerofill_opt))
        double.setName(cls.DOUBLE)

        time = CaselessKeyword(cls.TIME).setResultsName(DATA_TYPE) + opt_data_length
        time.setName(cls.TIME)

        date = CaselessKeyword(cls.DATE).setResultsName(DATA_TYPE)
        date.setName(cls.DATE)

        year = CaselessKeyword(cls.YEAR).setResultsName(DATA_TYPE) + opt_data_length
        year.setName(cls.YEAR)

        datetime = CaselessKeyword(cls.DATETIME).setResultsName(DATA_TYPE) + opt_data_length
        datetime.setName(cls.DATETIME)

        timestamp = CaselessKeyword(cls.TIMESTAMP).setResultsName(DATA_TYPE) + opt_data_length
        timestamp.setName(cls.TIMESTAMP)

        return (char | nchar | varchar | nvarchar | tinytext | text | mediumtext | longtext
                | binary | varbinary | tinyblob | blob | mediumblob | longblob
                | tinyint | smallint | mediumint | integer | bigint | decimal | m_float | double
                | time | date | year | datetime | timestamp)

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:

        def mysql_integer_type():
            column.data_type = MySQLDataType.INTEGER if column.data_type == MySQLDataType.INT else column.data_type
            data_type = getattr(mysql, column.data_type)
            return data_type(column.data_length, unsigned=column.unsigned, zerofill=column.zerofill)

        def mysql_point_type():
            data_type = getattr(mysql, column.data_type)
            if len(column.data_length) == 1:
                return data_type(precision=column.data_length[0], unsigned=column.unsigned, zerofill=column.zerofill)
            elif len(column.data_length) == 2:
                return data_type(precision=column.data_length[0], scale=column.data_length[1],
                                 unsigned=column.unsigned, zerofill=column.zerofill)
            else:
                return data_type(unsigned=column.unsigned, zerofill=column.zerofill)

        data_type_objects = {
            cls.CHAR: mysql.CHAR(column.data_length),
            cls.NCHAR: mysql.NCHAR(column.data_length),
            cls.VARCHAR: mysql.VARCHAR(column.data_length),
            cls.NVARCHAR: mysql.NVARCHAR(column.data_length),
            cls.TINYTEXT: mysql.TINYTEXT,
            cls.TEXT: mysql.TEXT(column.data_length),
            cls.MEDIUMTEXT: mysql.MEDIUMTEXT,
            cls.LONGTEXT: mysql.LONGTEXT,
            cls.BINARY: mysql.BINARY(column.data_length),
            cls.VARBINARY: mysql.VARBINARY(column.data_length),
            cls.TINYBLOB: mysql.TINYBLOB,
            cls.BLOB: mysql.BLOB(column.data_length),
            cls.MEDIUMBLOB: mysql.MEDIUMBLOB,
            cls.LONGBLOB: mysql.LONGBLOB,
            cls.TINYINT: mysql_integer_type,
            cls.SMALLINT: mysql_integer_type,
            cls.MEDIUMINT: mysql_integer_type,
            cls.INT: mysql_integer_type,
            cls.INTEGER: mysql_integer_type,
            cls.BIGINT: mysql_integer_type,
            cls.DECIMAL: mysql_point_type,
            cls.NUMERIC: mysql_point_type,
            cls.FLOAT: mysql_point_type,
            cls.DOUBLE: mysql_point_type,
            cls.TIME: mysql.TIME(fsp=column.data_length),
            cls.DATE: mysql.DATE,
            cls.YEAR: mysql.YEAR(column.data_length),
            cls.DATETIME: mysql.DATETIME(fsp=column.data_length),
            cls.TIMESTAMP: mysql.TIMESTAMP(fsp=column.data_length)
        }

        result = data_type_objects[column.data_type]
        return result() if callable(result) else result


class SqlServerDataType(DataType):

    CHAR = "CHAR"
    NCHAR = "NCHAR"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"

    BIT = "BIT"
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    FLOAT = "FLOAT"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    REAL = "REAL"
    SMALLMONEY = "SMALLMONEY"
    MONEY = "MONEY"

    TIME = "TIME"
    DATE = "DATE"
    DATETIME = "DATETIME"
    DATETIME2 = "DATETIME2"
    SMALLDATETIME = "SMALLDATETIME"
    DATETIMEOFFSET = "DATETIMEOFFSET"

    BINARY = "BINARY"
    VARBINARY = "VARBINARY"

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:
        pass

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:
        pass


class PostgresqlDataType(DataType):

    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"

    SMALLINT = "SMALLINT"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    MONEY = "MONEY"

    TIME = "TIME"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    interval_fields = ["YEAR TO MONTH", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND",
                       "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND",
                       "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]

    BYTEA = "BYTEA"

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:
        pass

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:
        pass
