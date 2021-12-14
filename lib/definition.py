
import logging
import os

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from pyparsing import (Word, delimitedList, Optional, Group, alphas, nums, alphanums, OneOrMore, CaselessKeyword,
                       Suppress, ParseException, tokenMap, MatchFirst, ParseResults)
from sqlalchemy import Column, Sequence, PrimaryKeyConstraint, String, LargeBinary, types as sql_types
from sqlalchemy.dialects import oracle, mysql, mssql as sqlserver, postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import as_declarative
from typing import List, Dict, Any, Type, NoReturn, Union

from lib.common import print_error
from lib.config import DatabaseConfig
from lib.globals import *


DEFINITION_DIRECTORY = "definition"
_DEFINITION_FILE_EXT = ".def"


@dataclass
class TableCustomAttributes:
    constraint_type: str
    identifier_column: str


@as_declarative()
class OracleDeclBase:
    pass


@as_declarative()
class MysqlDeclBase:
    pass


@as_declarative()
class SqlServerDeclBase:
    pass


@as_declarative()
class PostgresqlDeclBase:
    pass


class CubridStructBase:
    tables = {}


@as_declarative()
class TiberoStructBase:
    tables = {}


TYPE_DBMS_DECL_BASE = Type[Union[OracleDeclBase, MysqlDeclBase, SqlServerDeclBase, PostgresqlDeclBase,
                                 TiberoStructBase, CubridStructBase]]


class SADeclarativeManager:

    def __init__(self, conn_info: DatabaseConfig, table_names: List[str] = None):

        self.logger = logging.getLogger(CDCBENCH)
        self.dbms = conn_info.dbms
        self.schema = conn_info.v_schema

        self.definition_file_path = os.path.join(DEFINITION_DIRECTORY, self.dbms.lower())
        self.definition_file_names: List[str]
        definition_file_path_exist_file_names = (fn for fn in os.listdir(self.definition_file_path)
                                                 if fn.endswith(_DEFINITION_FILE_EXT))
        if table_names is not None:
            self.definition_file_names = [f"{table_name.lower()}{_DEFINITION_FILE_EXT}" for table_name in table_names]
            set_specific_def_fns = set(self.definition_file_names)
            set_exist_def_fns = set(definition_file_path_exist_file_names)
            specific_def_fns_diff_exist_def_fns = set_specific_def_fns.difference(set_exist_def_fns)
            if len(specific_def_fns_diff_exist_def_fns) >= 1:
                print_error(f"[ {', '.join(specific_def_fns_diff_exist_def_fns)} ] is"
                            f" Definition file that does not exist.")
        else:
            self.definition_file_names = definition_file_path_exist_file_names

        self.custom_attrs: Dict[TableCustomAttributes] = {}

        self.set_declarative_base()

    def set_declarative_base(self) -> NoReturn:

        dbms_decl_base = {
            ORACLE: OracleDeclBase,
            MYSQL: MysqlDeclBase,
            SQLSERVER: SqlServerDeclBase,
            POSTGRESQL: PostgresqlDeclBase
        }
        dbms_data_type_classes = {
            ORACLE: OracleDataType,
            MYSQL: MySqlDataType,
            SQLSERVER: SqlServerDataType,
            POSTGRESQL: PostgresqlDataType
        }

        for def_fn in self.definition_file_names:

            self.logger.debug(f"definition file name: [ {def_fn} ]")
            table_info = parse_definition_file(self.dbms, os.path.join(self.definition_file_path, def_fn))[0]
            self.logger.debug(f"table_info: {table_info.dump()}")

            decl_base: Type[Union[OracleDeclBase, MysqlDeclBase, SqlServerDeclBase, PostgresqlDeclBase]]
            decl_base = dbms_decl_base[self.dbms]

            declarative_attr = {"__tablename__": table_info.table_name}

            identifier_cols = []
            for column in table_info.columns:

                data_type: Any = dbms_data_type_classes[self.dbms].get_data_type_object(column)

                if column.identifier:
                    if column.data_type not in dbms_data_type_classes[self.dbms].identifier_data_types:
                        print_error(f"Table [ {table_info.table_name} ] Identifier [ {column.column_name} ] "
                                    f"column's data type is not "
                                    f"{', '.join(dbms_data_type_classes[self.dbms].identifier_data_types)}.")
                    identifier_cols.append(column.column_name)

                if column.sequence:
                    if column.data_type not in dbms_data_type_classes[self.dbms].sequence_data_types:
                        print_error(f"Table [ {table_info.table_name} ] Sequence [ {column.column_name} ] "
                                    f"column's data type is not "
                                    f"{', '.join(dbms_data_type_classes[self.dbms].identifier_data_types)}.")
                    declarative_attr[column.column_name] = Column(column.column_name, data_type,
                                                                  Sequence(f"{table_info.table_name}_SEQ"),
                                                                  nullable=column.nullable,
                                                                  autoincrement=column.sequence)
                else:
                    declarative_attr[column.column_name] = Column(column.column_name, data_type,
                                                                  nullable=column.nullable)

            if len(identifier_cols) > 1:
                print_error(f"Table [ {table_info.table_name} ] has two or more identifier attribute")

            declarative_attr["__table_args__"] = (PrimaryKeyConstraint(*table_info.constraint.constraint_columns,
                                                                       name=table_info.constraint.constraint_name),)
            self.custom_attrs[table_info.table_name.upper()] = TableCustomAttributes(
                constraint_type=table_info.constraint.constraint_type, identifier_column=identifier_cols[0]
            )

            # self.logger.debug(f"Declarative attr: {declarative_attr}")
            type(table_info.table_name, (decl_base,), declarative_attr)
            self.logger.info(f"Create declarative class [ {table_info.table_name} ]")

    def set_structure_base(self) -> NoReturn:

        structure_base: Type[Union[TiberoStructBase, CubridStructBase]]
        if self.dbms == TIBERO:
            structure_base = TiberoStructBase
        else:   # CUBRID
            structure_base = CubridStructBase

        for def_fn in self.definition_file_names:
            with open(os.path.join(self.definition_file_path, def_fn), "r", encoding="utf-8") as f:
                table_info = f.read()
                table_name = table_info[:table_info.find("(")].strip()
                structure_base.tables[table_name] = table_info

    def get_dbms_base(self) -> TYPE_DBMS_DECL_BASE:

        if self.dbms not in sa_unsupported_dbms:
            decl_base: TYPE_DBMS_DECL_BASE
            if self.dbms == ORACLE:
                decl_base = OracleDeclBase

            elif self.dbms == MYSQL:
                decl_base = MysqlDeclBase

            elif self.dbms == SQLSERVER:
                decl_base = SqlServerDeclBase

            else:   # PostgreSQL
                if self.schema:
                    for table in PostgresqlDeclBase.metadata.sorted_tables:
                        table.schema = self.schema
                decl_base = PostgresqlDeclBase

            for table in decl_base.metadata.sorted_tables:
                table.custom_attrs = self.custom_attrs[table.name.upper()]

            return decl_base

        else:
            if self.dbms == TIBERO:
                return TiberoStructBase

            else:   # CUBRID
                return CubridStructBase


# Parsing support keyword
LBRACKET = Suppress("(").setName("LBRACKET")
RBRACKET = Suppress(")").setName("RBRACKET")
OPT_LBRACKET = Optional(Suppress("(")).setName("LBRACKET")
OPT_RBRACKET = Optional(Suppress(")")).setName("RBRACKET")

DATA_TYPE = "data_type"
DATA_LENGTH = "data_length"
INTERVAL_TYPE = "interval_type"

SEQUENCE = "SEQUENCE"
IDENTIFIER = "IDENTIFIER"

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

    data_type: MatchFirst
    if dbms == ORACLE:
        data_type = OracleDataType.get_data_type_parser()
    elif dbms == MYSQL:
        data_type = MySqlDataType.get_data_type_parser()
    elif dbms == SQLSERVER:
        data_type = SqlServerDataType.get_data_type_parser()
    else:   # PostgreSQL
        data_type = PostgresqlDataType.get_data_type_parser()
    data_type.setName("data_type")

    nullable = Optional(CaselessKeyword(NOT_NULL) | CaselessKeyword(NULL), default=NULL).setResultsName("nullable")
    nullable.setName("nullable")
    nullable.setParseAction(lambda toks: True if toks.nullable == NULL else False)

    sequence = Optional(CaselessKeyword(SEQUENCE)).setResultsName("sequence")
    sequence.setName("sequence")
    sequence.setParseAction(lambda toks: True if toks.sequence != "" else False)

    identity = Optional(CaselessKeyword(IDENTIFIER)).setResultsName("identifier")
    identity.setName("identifier")
    identity.setParseAction(lambda toks: True if toks.identifier != "" else False)

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
REQ_DATA_LENGTH = (LBRACKET
                   + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH)
                   + RBRACKET)

OPT_DATA_LENGTH = (OPT_LBRACKET
                   + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH)
                   + OPT_RBRACKET)

OPT_DUAL_DATA_LENGTH = (OPT_LBRACKET
                        + (Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int)))
                              .setResultsName(DATA_LENGTH))
                        + OPT_RBRACKET)


def CaselessDataType(*data_type):
    if len(data_type) == 2:
        return (CaselessKeyword(data_type[0]) | CaselessKeyword(data_type[1])).setResultsName(DATA_TYPE)
    else:
        return CaselessKeyword(data_type[0]).setResultsName(DATA_TYPE)


class DataType(metaclass=ABCMeta):

    identifier_data_types: List[str]
    sequence_data_types: List[str]

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
    NVARCHAR = "NVARCHAR"
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
    LONG_RAW_ = "LONG_RAW"

    CLOB = "CLOB"
    NCLOB = "NCLOB"
    BLOB = "BLOB"

    ROWID = "ROWID"

    identifier_data_types = [NUMBER]
    sequence_data_types = [NUMBER]

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
            return f"INTERVAL YEAR{type_.year_precision is not None and '(%d)' % type_.year_precision or ''} TO MONTH"

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

        REQ_DATA_LENGTH_WITH_SEMANTIC = (LBRACKET
                                         + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH)
                                         + (Optional(CaselessKeyword("BYTE") | CaselessKeyword("CHAR"), default="BYTE")
                                            .setResultsName("length_semantics"))
                                         + RBRACKET)

        char = CaselessDataType(cls.CHAR) + REQ_DATA_LENGTH_WITH_SEMANTIC
        char.setName(cls.CHAR)

        nchar = CaselessDataType(cls.NCHAR) + REQ_DATA_LENGTH
        nchar.setName(cls.NCHAR)

        varchar2 = CaselessDataType(cls.VARCHAR2, cls.VARCHAR) + REQ_DATA_LENGTH_WITH_SEMANTIC
        varchar2.setName(cls.VARCHAR2)

        nvarchar2 = CaselessDataType(cls.NVARCHAR2) + REQ_DATA_LENGTH
        nvarchar2.setName(cls.NVARCHAR2)

        long = CaselessDataType(cls.LONG)
        long.setName(cls.LONG)

        number = CaselessDataType(cls.NUMBER) + OPT_DUAL_DATA_LENGTH
        number.setName(cls.NUMBER)

        binary_float = CaselessDataType(cls.BINARY_FLOAT)
        binary_float.setName(cls.BINARY_FLOAT)

        binary_double = CaselessDataType(cls.BINARY_DOUBLE)
        binary_double.setName(cls.BINARY_DOUBLE)

        float_ = CaselessDataType(cls.FLOAT) + OPT_DATA_LENGTH
        float_.setName(cls.FLOAT)

        date = CaselessDataType(cls.DATE)
        date.setName(cls.DATE)

        timestamp = CaselessDataType(cls.TIMESTAMP) + OPT_DATA_LENGTH
        timestamp.setName(cls.TIMESTAMP)

        interval_year_month = (CaselessDataType(cls.INTERVAL)
                               + CaselessKeyword("YEAR").setResultsName(INTERVAL_TYPE)
                               + OPT_LBRACKET
                               + (Optional(Word(nums).setParseAction(tokenMap(int)), default=None)
                                  .setResultsName("year_precision"))
                               + OPT_RBRACKET
                               + CaselessKeyword("TO MONTH"))
        interval_year_month.setName("INTERVAL YEAR TO MONTH")

        interval_day_second = (CaselessDataType(cls.INTERVAL)
                               + CaselessKeyword("DAY").setResultsName(INTERVAL_TYPE)
                               + OPT_LBRACKET
                               + (Optional(Word(nums).setParseAction(tokenMap(int)), default=None)
                                  .setResultsName("day_precision"))
                               + OPT_RBRACKET
                               + CaselessKeyword("TO SECOND")
                               + OPT_LBRACKET
                               + (Optional(Word(nums).setParseAction(tokenMap(int)), default=None)
                                  .setResultsName("second_precision"))
                               + OPT_RBRACKET)
        interval_day_second.setName("INTERVAL DAY TO SECOND")

        raw = CaselessDataType(cls.RAW) + REQ_DATA_LENGTH
        raw.setName(cls.RAW)

        long_raw = CaselessDataType(cls.LONG_RAW)
        long_raw.setName(cls.LONG_RAW)

        clob = CaselessDataType(cls.CLOB)
        clob.setName(cls.CLOB)

        nclob = CaselessDataType(cls.NCLOB)
        nclob.setName(cls.NCLOB)

        blob = CaselessDataType(cls.BLOB)
        blob.setName(cls.BLOB)

        rowid = CaselessDataType(cls.ROWID)
        rowid.setName(cls.ROWID)

        return (long_raw | char | nchar | varchar2 | nvarchar2 | long | number | binary_float | binary_double | float_
                | date | timestamp | interval_year_month | interval_day_second | raw | clob | nclob | blob | rowid)

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:

        def oracle_character_length_semantic_type():
            column["data_type"] = (OracleDataType.VARCHAR2
                                   if column.data_type == OracleDataType.VARCHAR else column.data_type)
            if column.length_semantics == "CHAR":
                data_type = getattr(oracle, column.data_type)
            else:
                data_type = getattr(cls.CustomTypes, column.data_type)
            return data_type(column.data_length)

        def oracle_number_type():
            if len(column.data_length) == 1:
                return oracle.NUMBER(column.data_length[0])
            elif len(column.data_length) == 2:
                return oracle.NUMBER(column.data_length[0], column.data_length[1])
            else:
                return oracle.NUMBER

        def oracle_interval_type():
            if column.interval_type == "YEAR":
                return cls.CustomTypes.INTERVAL(column.year_precision)
            else:   # DAY
                if column.day_precision is None and column.second_precision is None:
                    return oracle.INTERVAL
                elif column.day_precision is not None and column.second_precision is None:
                    return oracle.INTERVAL(day_precision=column.day_precision)
                elif column.day_precision is None and column.second_precision is not None:
                    return oracle.INTERVAL(second_precision=column.second_precision)
                else:
                    return oracle.INTERVAL(day_precision=column.day_precision,
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


class MySqlDataType(DataType):

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

    identifier_data_types = [INT, INTEGER, BIGINT]
    sequence_data_types = [INT, INTEGER, BIGINT]

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:

        char = CaselessDataType(cls.CHAR) + OPT_DATA_LENGTH
        char.setName(cls.CHAR)

        nchar = CaselessDataType(cls.NCHAR) + OPT_DATA_LENGTH
        nchar.setName(cls.NCHAR)

        varchar = CaselessDataType(cls.VARCHAR) + REQ_DATA_LENGTH
        varchar.setName(cls.VARCHAR)

        nvarchar = CaselessDataType(cls.NVARCHAR) + REQ_DATA_LENGTH
        nvarchar.setName(cls.NVARCHAR)

        tinytext = CaselessDataType(cls.TINYTEXT)
        tinytext.setName(cls.TINYTEXT)

        text = CaselessDataType(cls.TEXT) + OPT_DATA_LENGTH
        text.setName(cls.TEXT)

        mediumtext = CaselessDataType(cls.MEDIUMTEXT)
        mediumtext.setName(cls.MEDIUMTEXT)

        longtext = CaselessDataType(cls.LONGTEXT)
        longtext.setName(cls.LONGTEXT)

        binary = CaselessDataType(cls.BINARY) + OPT_DATA_LENGTH
        binary.setName(cls.BINARY)

        varbinary = CaselessDataType(cls.VARBINARY) + REQ_DATA_LENGTH
        varbinary.setName(cls.VARBINARY)

        tinyblob = CaselessDataType(cls.TINYBLOB)
        tinyblob.setName(cls.TINYBLOB)

        blob = CaselessDataType(cls.BLOB) + OPT_DATA_LENGTH
        blob.setName(cls.BLOB)

        mediumblob = CaselessDataType(cls.MEDIUMBLOB)
        mediumblob.setName(cls.MEDIUMBLOB)

        longblob = CaselessDataType(cls.LONGBLOB)
        longblob.setName(cls.LONGBLOB)

        def _signed_replace_bool(toks):
            return True if toks.unsigned == UNSIGNED else False

        def _zerofill_replace_bool(toks):
            return True if toks.zerofill != "" else False

        OPT_SIGNED = (Optional(CaselessKeyword(UNSIGNED) | CaselessKeyword(SIGNED), default=SIGNED)
                      .setResultsName(UNSIGNED.lower()).setParseAction(_signed_replace_bool))
        OPT_SIGNED.setName("SIGNED | UNSIGNED")

        OPT_ZEROFILL = (Optional(CaselessKeyword(ZEROFILL))
                        .setResultsName(ZEROFILL.lower()).setParseAction(_zerofill_replace_bool))
        OPT_ZEROFILL.setName(ZEROFILL)

        tinyint = CaselessDataType(cls.TINYINT) + OPT_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        tinyint.setName(cls.TINYINT)

        smallint = CaselessDataType(cls.SMALLINT) + OPT_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        smallint.setName(cls.SMALLINT)

        mediumint = CaselessDataType(cls.MEDIUMINT) + OPT_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        mediumint.setName(cls.MEDIUMINT)

        integer = CaselessDataType(cls.INTEGER, cls.INT) + OPT_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        integer.setName(cls.INTEGER)

        bigint = CaselessDataType(cls.BIGINT) + OPT_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        bigint.setName(cls.BIGINT)

        decimal = CaselessDataType(cls.DECIMAL, cls.NUMERIC) + OPT_DUAL_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        decimal.setName(cls.DECIMAL)

        float_ = CaselessDataType(cls.FLOAT) + OPT_DUAL_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        float_.setName(cls.FLOAT)

        double = CaselessDataType(cls.DOUBLE) + OPT_DUAL_DATA_LENGTH + (OPT_SIGNED & OPT_ZEROFILL)
        double.setName(cls.DOUBLE)

        time = CaselessDataType(cls.TIME) + OPT_DATA_LENGTH
        time.setName(cls.TIME)

        date = CaselessDataType(cls.DATE)
        date.setName(cls.DATE)

        year = CaselessDataType(cls.YEAR) + OPT_DATA_LENGTH
        year.setName(cls.YEAR)

        datetime = CaselessDataType(cls.DATETIME) + OPT_DATA_LENGTH
        datetime.setName(cls.DATETIME)

        timestamp = CaselessDataType(cls.TIMESTAMP) + OPT_DATA_LENGTH
        timestamp.setName(cls.TIMESTAMP)

        return (char | nchar | varchar | nvarchar | tinytext | text | mediumtext | longtext
                | binary | varbinary | tinyblob | blob | mediumblob | longblob
                | tinyint | smallint | mediumint | integer | bigint | decimal | float_ | double
                | time | date | year | datetime | timestamp)

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:

        def mysql_integer_type():
            column["data_type"] = MySqlDataType.INTEGER if column.data_type == MySqlDataType.INT else column.data_type
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

    identifier_data_types = [INT, INTEGER, BIGINT]
    sequence_data_types = [INT, INTEGER, BIGINT]

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:

        char = CaselessDataType(cls.CHAR) + OPT_DATA_LENGTH
        char.setName(cls.CHAR)

        nchar = CaselessDataType(cls.NCHAR) + OPT_DATA_LENGTH
        nchar.setName(cls.NCHAR)

        OPT_DATA_LENGTH_WITH_MAX = (OPT_LBRACKET
                                    + (Optional(Word(nums).setParseAction(tokenMap(int)) | CaselessKeyword("MAX"),
                                                default=None)
                                       .setResultsName(DATA_LENGTH))
                                    + OPT_RBRACKET)

        varchar = CaselessDataType(cls.VARCHAR) + OPT_DATA_LENGTH_WITH_MAX
        varchar.setName(cls.VARCHAR)

        nvarchar = CaselessDataType(cls.NVARCHAR) + OPT_DATA_LENGTH_WITH_MAX
        nvarchar.setName(cls.NVARCHAR)

        bit = CaselessDataType(cls.BIT)
        bit.setName(cls.BIT)

        tinyint = CaselessDataType(cls.TINYINT)
        tinyint.setName(cls.TINYINT)

        smallint = CaselessDataType(cls.SMALLINT)
        smallint.setName(cls.SMALLINT)

        integer = CaselessDataType(cls.INT, cls.INTEGER)
        integer.setName(cls.INT)

        bigint = CaselessDataType(cls.BIGINT)
        bigint.setName(cls.BIGINT)

        decimal = CaselessDataType(cls.DECIMAL, cls.NUMERIC) + OPT_DUAL_DATA_LENGTH
        decimal.setName(cls.DECIMAL)

        float_ = CaselessDataType(cls.FLOAT, cls.DOUBLE_PRECISION) + OPT_DATA_LENGTH
        float_.setName(cls.FLOAT)

        real = CaselessDataType(cls.REAL)
        real.setName(cls.REAL)

        smallmoney = CaselessDataType(cls.SMALLMONEY)
        smallmoney.setName(cls.SMALLMONEY)

        money = CaselessDataType(cls.MONEY)
        money.setName(cls.MONEY)

        time = CaselessDataType(cls.TIME) + OPT_DATA_LENGTH
        time.setName(cls.TIME)

        date = CaselessDataType(cls.DATE)
        date.setName(cls.DATE)

        datetime = CaselessDataType(cls.DATETIME)
        datetime.setName(cls.DATETIME)

        datetime2 = CaselessDataType(cls.DATETIME2) + OPT_DATA_LENGTH
        datetime2.setName(cls.DATETIME2)

        smalldatetime = CaselessDataType(cls.SMALLDATETIME)
        smalldatetime.setName(cls.SMALLDATETIME)

        datetimeoffset = CaselessDataType(cls.DATETIMEOFFSET)
        datetimeoffset.setName(cls.DATETIMEOFFSET)

        binary = CaselessDataType(cls.BINARY) + OPT_DATA_LENGTH
        binary.setName(cls.BINARY)

        varbinary = CaselessDataType(cls.VARBINARY) + OPT_DATA_LENGTH_WITH_MAX
        varbinary.setName(cls.VARBINARY)

        return (char | nchar | varchar | nvarchar | bit | tinyint | smallint | integer | bigint | decimal | float_ |
                real | smallmoney | money | datetimeoffset | datetime2 | datetime | smalldatetime | date | time |
                binary | varbinary)

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:

        def sqlserver_point_type():
            if len(column.data_length) == 1:
                return sqlserver.DECIMAL(precision=column.data_length[0])
            elif len(column.data_length) == 2:
                return sqlserver.DECIMAL(precision=column.data_length[0], scale=column.data_length[1])
            else:
                return sqlserver.DECIMAL

        data_type_objects = {
            cls.CHAR: sqlserver.CHAR(column.data_length),
            cls.NCHAR: sqlserver.NCHAR(column.data_length),
            cls.VARCHAR: sqlserver.VARCHAR(column.data_length),
            cls.NVARCHAR: sqlserver.NVARCHAR(column.data_length),
            cls.BIT: sqlserver.BIT,
            cls.TINYINT: sqlserver.TINYINT,
            cls.SMALLINT: sqlserver.SMALLINT,
            cls.INT: sqlserver.INTEGER,
            cls.INTEGER: sqlserver.INTEGER,
            cls.BIGINT: sqlserver.BIGINT,
            cls.DECIMAL: sqlserver_point_type,
            cls.NUMERIC: sqlserver_point_type,
            cls.FLOAT: sqlserver.FLOAT,
            cls.DOUBLE_PRECISION: sqlserver.FLOAT,
            cls.REAL: sqlserver.REAL,
            cls.SMALLMONEY: sqlserver.SMALLMONEY,
            cls.MONEY: sqlserver.MONEY,
            cls.TIME: sqlserver.TIME,
            cls.DATE: sqlserver.DATE,
            cls.DATETIME: sqlserver.DATETIME,
            cls.DATETIME2: sqlserver.DATETIME2,
            cls.SMALLDATETIME: sqlserver.SMALLDATETIME,
            cls.DATETIMEOFFSET: sqlserver.DATETIMEOFFSET,
            cls.BINARY: sqlserver.BINARY,
            cls.VARBINARY: sqlserver.VARBINARY
        }

        result = data_type_objects[column.data_type]
        return result() if callable(result) else result


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

    identifier_data_types = [INT, INTEGER, BIGINT]
    sequence_data_types = [INT, INTEGER, BIGINT]

    @classmethod
    def get_data_type_parser(cls) -> MatchFirst:

        char = CaselessDataType(cls.CHAR) + OPT_DATA_LENGTH
        char.setName(cls.CHAR)

        varchar = CaselessDataType(cls.VARCHAR) + OPT_DATA_LENGTH
        varchar.setName(cls.VARCHAR)

        text = CaselessDataType(cls.TEXT)
        text.setName(cls.TEXT)

        smallint = CaselessDataType(cls.SMALLINT)
        smallint.setName(cls.SMALLINT)

        integer = CaselessDataType(cls.INTEGER, cls.INT)
        integer.setName(cls.INTEGER)

        bigint = CaselessDataType(cls.BIGINT)
        bigint.setName(cls.BIGINT)

        numeric = CaselessDataType(cls.NUMERIC, cls.DECIMAL) + OPT_DUAL_DATA_LENGTH
        numeric.setName(cls.NUMERIC)

        real = CaselessDataType(cls.REAL)
        real.setName(cls.REAL)

        double_precision = CaselessDataType(cls.DOUBLE_PRECISION)
        double_precision.setName(cls.DOUBLE_PRECISION)

        money = CaselessDataType(cls.MONEY)
        money.setName(cls.MONEY)

        time = CaselessDataType(cls.TIME) + OPT_DATA_LENGTH
        time.setName(cls.TIME)

        date = CaselessDataType(cls.DATE)
        date.setName(cls.DATE)

        timestamp = CaselessDataType(cls.TIMESTAMP) + OPT_DATA_LENGTH
        timestamp.setName(cls.TIMESTAMP)

        interval_fields = list(map(CaselessKeyword, cls.interval_fields))
        interval = (CaselessDataType(cls.INTERVAL)
                    + Optional(MatchFirst(interval_fields), default=None).setResultsName(INTERVAL_TYPE)
                    + OPT_DATA_LENGTH)
        interval.setName(cls.INTERVAL)

        bytea = CaselessDataType(cls.BYTEA)
        bytea.setName(cls.BYTEA)

        return (char | varchar | text | smallint | integer | bigint | numeric | real | double_precision |
                money | timestamp | date | time | interval | bytea)

    @classmethod
    def get_data_type_object(cls, column: ParseResults) -> Any:

        def postgresql_point_type():
            if len(column.data_length) == 1:
                return postgresql.NUMERIC(precision=column.data_length[0])
            elif len(column.data_length) == 2:
                return postgresql.NUMERIC(precision=column.data_length[0], scale=column.data_length[1])
            else:
                return postgresql.NUMERIC

        data_type_objects = {
            cls.CHAR: postgresql.CHAR(column.data_length),
            cls.VARCHAR: postgresql.VARCHAR(column.data_length),
            cls.TEXT: postgresql.TEXT,
            cls.SMALLINT: postgresql.SMALLINT,
            cls.INT: postgresql.INTEGER,
            cls.INTEGER: postgresql.INTEGER,
            cls.BIGINT: postgresql.BIGINT,
            cls.NUMERIC: postgresql_point_type,
            cls.DECIMAL: postgresql_point_type,
            cls.REAL: postgresql.REAL,
            cls.DOUBLE_PRECISION: postgresql.DOUBLE_PRECISION,
            cls.MONEY: postgresql.MONEY,
            cls.TIME: postgresql.TIME(precision=column.data_length),
            cls.DATE: postgresql.DATE,
            cls.TIMESTAMP: postgresql.TIMESTAMP(precision=column.data_length),
            cls.INTERVAL: postgresql.INTERVAL(fields=column.interval_type, precision=column.data_length),
            cls.BYTEA: postgresql.BYTEA
        }

        result = data_type_objects[column.data_type]
        return result() if callable(result) else result
