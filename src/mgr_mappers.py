from sqlalchemy import Column, Sequence, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.dialects import oracle, mysql, mssql as sqlserver, postgresql

from src.constants import *
from src.oracle_custom_types import CHARLENCHAR, VARCHAR2LENBYTE, LONGRAW, INTERVALYEARMONTH
from src.mgr_logger import LoggerManager

from pyparsing import Word, delimitedList, Optional, Group, alphas, nums, alphanums, OneOrMore, \
    CaselessKeyword, Suppress, Forward, ParseFatalException, ParseBaseException, tokenMap, MatchFirst

import os


class MapperManager:

    __definition_dir = "definitions"

    def __init__(self, connection, table_name=None):

        self.logger = LoggerManager.get_logger(__name__)
        self.dbms_type = connection.connection_info["dbms_type"]
        self.schema_name = connection.connection_info["schema_name"]
        self.db_session = connection.db_session

        def_file_path = os.path.join(os.path.join(self.__definition_dir, self.dbms_type.lower()))
        if table_name is not None:
            def_files = ["{}.def".format(table_name.lower())]
        else:
            def_files = os.listdir(def_file_path)

        for def_file in def_files:

            table_metadata = _table_definition_parser(self.dbms_type, os.path.join(def_file_path, def_file))[0]

            mapper_base = None
            if self.dbms_type == ORACLE:
                mapper_base = OracleMapperBase
            elif self.dbms_type == MYSQL:
                mapper_base = MysqlMapperBase
            elif self.dbms_type == SQLSERVER:
                mapper_base = SqlserverMapperBase
            elif self.dbms_type == POSTGRESQL:
                mapper_base = PostgresqlMapperBase

            # Table Name 생성
            mapper_attr = {"__tablename__": table_metadata.table_name}

            self.logger.debug(table_metadata.table_name)

            # Column 정보 생성
            for idx, column in enumerate(table_metadata.columns):

                # DBMS별 Data Type 생성
                data_type = None
                if self.dbms_type == ORACLE:
                    data_type = _get_oracle_data_type(column)
                elif self.dbms_type == MYSQL:
                    data_type = _get_mysql_data_type(column)
                elif self.dbms_type == SQLSERVER:
                    data_type = _get_sqlserver_data_type(column)
                elif self.dbms_type == POSTGRESQL:
                    data_type = _get_postgresql_data_type(column)

                # 첫 번째 컬럼은 Sequence 함께 생성
                if idx == 0:
                    if self.dbms_type == ORACLE:
                        mapper_attr[column.column_name] = Column(column.column_name, data_type,
                                                                 Sequence("{}_SEQ".format(table_metadata.table_name)))
                    else:
                        mapper_attr[column.column_name] = Column(column.column_name, data_type)

                else:
                    mapper_attr[column.column_name] = Column(column.column_name, data_type, nullable=column.nullable)

            # Primary Key Constraint 정보 생성
            mapper_attr["__table_args__"] = (PrimaryKeyConstraint(*table_metadata.constraint.key_column,
                                                                  name=table_metadata.constraint.constraint_name),)

            # Table Mapper를 DBMS별 Mapper Base에 등록
            type("{}".format(table_metadata.table_name), (mapper_base,), mapper_attr)

    def get_mappers(self):

        if self.dbms_type == ORACLE:
            OracleMapperBase.query = self.db_session.query_property()
            return OracleMapperBase

        elif self.dbms_type == MYSQL:
            MysqlMapperBase.query = self.db_session.query_property()
            return MysqlMapperBase

        elif self.dbms_type == SQLSERVER:
            SqlserverMapperBase.query = self.db_session.query_property()

            # SQL Server의 경우 Table명 앞에 Schema명 붙임
            for table in SqlserverMapperBase.metadata.sorted_tables:
                table.schema = self.schema_name

            return SqlserverMapperBase

        elif self.dbms_type == POSTGRESQL:
            PostgresqlMapperBase.query = self.db_session.query_property()

            # PostgreSQL의 경우 Table명 앞에 Schema명 붙임
            for table in PostgresqlMapperBase.metadata.sorted_tables:
                table.schema = self.schema_name

            return PostgresqlMapperBase


@as_declarative()
class OracleMapperBase:
    pass


@as_declarative()
class MysqlMapperBase:
    pass


@as_declarative()
class SqlserverMapperBase:
    pass


@as_declarative()
class PostgresqlMapperBase:
    pass


class MissingKeywordException(ParseFatalException):
    def __init__(self, s, loc, msg):
        super(MissingKeywordException, self).__init__(
            s, loc, msg="Missing Keyword"
        )


class UnknownDataTypeException(ParseFatalException):
    def __init__(self, s, loc, msg):
        super(UnknownDataTypeException, self).__init__(
            s, loc, msg="Unknown Data Type"
        )


def _error(exception_class):
    def raise_exception(s, loc, toks):
        raise exception_class(s, loc, toks)
    return Word(alphanums).setParseAction(raise_exception)


LBRACKET = Suppress("(").setName("LBRACKET")
RBRACKET = Suppress(")").setName("RBRACKET")
OPT_LBRACKET = Optional(Suppress("(")).setName("LBRACKET")
OPT_RBRACKET = Optional(Suppress(")")).setName("RBRACKET")


def _nullable_replace_bool(toks):
    if toks.nullable == "NULL":
        return True
    else:
        return False


def _table_definition_parser(dbms_type, file_abs_path):

    with open(file_abs_path, "r", encoding="utf-8") as f:
        table_definition = f.read()

    object_name = Word(alphas, alphanums + "-_\"$?")

    column_name_expr = object_name.setResultsName("column_name")
    column_name_expr.setName("column_name")

    # DBMS별 Data Type Parsing 구조 생성
    data_type_expr = None

    if dbms_type == ORACLE:
        data_type_expr = _oracle_data_type_parser()
    elif dbms_type == MYSQL:
        data_type_expr = _mysql_data_type_parser()
    elif dbms_type == SQLSERVER:
        data_type_expr = _sqlserver_data_type_parser()
    elif dbms_type == POSTGRESQL:
        data_type_expr = _postgresql_data_type_parser()

    data_type_expr = data_type_expr | _error(UnknownDataTypeException)
    data_type_expr.setName("data_type_expr")

    nullable_expr = Optional(CaselessKeyword(W_NOT_NULL) | CaselessKeyword(W_NULL) | _error(MissingKeywordException),
                             default=W_NULL).setResultsName("nullable")
    nullable_expr.setName("nullable_expr")
    nullable_expr.setParseAction(_nullable_replace_bool)

    column_expr = Group(column_name_expr + data_type_expr + nullable_expr) + Suppress(",")
    column_expr.setName("column_expr")

    column_list_expr = Forward()
    column_list_expr.setName("column_list_expr")

    PRIMARY_KEY = CaselessKeyword(W_PRIMARY_KEY) | _error(MissingKeywordException)
    PRIMARY_KEY.setName(W_PRIMARY_KEY)

    key_expr = PRIMARY_KEY.setResultsName("key_type") \
               + LBRACKET + delimitedList(object_name).setResultsName("key_column") + RBRACKET

    CONSTRAINT = CaselessKeyword(W_CONSTRAINT) | _error(MissingKeywordException)
    CONSTRAINT.setName(W_CONSTRAINT)

    constraint_expr = Group(CONSTRAINT + object_name.setResultsName("constraint_name") + key_expr).setResultsName("constraint")
    constraint_expr.setName("constraint_expr")

    # column_list_def Expression을 constraint 절 전에 멈춤
    column_list_expr <<= OneOrMore(column_expr, stopOn=constraint_expr).setResultsName("columns")

    table_name_expr = object_name.setResultsName("table_name")
    table_name_expr.setName("table_name_expr")

    table_expr = Group(table_name_expr + LBRACKET + column_list_expr +
                       Optional(constraint_expr, default=None) + RBRACKET + Suppress(";")).setResultsName("table")

    try:

        table_metadata = table_expr.parseString(table_definition, parseAll=True)

        return table_metadata

    except ParseBaseException as pbe:
        pbe.file_name = file_abs_path
        raise pbe


# Metadata key name
DATA_TYPE = "data_type"
DATA_LENGTH = "data_length"

# Reserved Word (Keyword)
W_NULL = "NULL"
W_NOT_NULL = "NOT NULL"
W_CONSTRAINT = "CONSTRAINT"
W_PRIMARY_KEY = "PRIMARY KEY"
W_UNSIGNED = "UNSIGNED"
W_SIGNED = "SIGNED"
W_ZEROFILL = "ZEROFILL"


class TYPE:
    # String Category
    CHAR = "CHAR"
    NCHAR = "NCHAR"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    VARCHAR2 = "VARCHAR2"
    NVARCHAR2 = "NVARCHAR2"

    # Numeric Category
    NUMBER = "NUMBER"
    BIT = "BIT"
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    MEDIUMINT = "MEDIUMINT"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    BINARY_FLOAT = "BINARY_FLOAT"
    BINARY_DOUBLE = "BINARY_DOUBLE"

    # Date & Time Category
    TIME = "TIME"
    DATE = "DATE"
    YEAR = "YEAR"
    SMALLDATETIME = "SMALLDATETIME"
    DATETIME = "DATETIME"
    DATETIME2 = "DATETIME2"
    DATETIMEOFFSET = "DATETIMEOFFSET"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"

    # Binary Category
    RAW = "RAW"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BYTEA = "BYTEA"

    # Large Object Category
    LONG = "LONG"
    CLOB = "CLOB"
    NCLOB = "NCLOB"
    TINYTEXT = "TINYTEXT"
    TEXT = "TEXT"
    MEDIUMTEXT = "MEDIUMTEXT"
    LONGTEXT = "LONGTEXT"
    LONG_RAW = "LONG RAW"
    TINYBLOB = "TINYBLOB"
    BLOB = "BLOB"
    MEDIUMBLOB = "MEDIUMBLOB"
    LONGBLOB = "LONGBLOB"

    # etc. Category
    ROWID = "ROWID"
    SMALLMONEY = "SMALLMONEY"
    MONEY = "MONEY"


def _oracle_data_type_parser():

    # String Category
    oracle_char = CaselessKeyword(TYPE.CHAR).setResultsName(DATA_TYPE) \
                  + LBRACKET \
                  + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) \
                  + Optional(CaselessKeyword("BYTE") | CaselessKeyword("CHAR"), default="BYTE").setResultsName("length_semantics") \
                  + RBRACKET
    oracle_char.setName(TYPE.CHAR)

    oracle_nchar = CaselessKeyword(TYPE.NCHAR).setResultsName(DATA_TYPE) \
                   + LBRACKET + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_nchar.setName(TYPE.NCHAR)

    oracle_varchar2 = CaselessKeyword(TYPE.VARCHAR2).setResultsName(DATA_TYPE) \
                      + LBRACKET \
                      + Word(nums).setParseAction().setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) \
                      + Optional(CaselessKeyword("BYTE") | CaselessKeyword("CHAR"), default="BYTE").setResultsName("length_semantics") \
                      + RBRACKET
    oracle_varchar2.setName(TYPE.VARCHAR2)

    oracle_nvarchar2 = CaselessKeyword(TYPE.NVARCHAR2).setResultsName(DATA_TYPE) \
                       + LBRACKET + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_nvarchar2.setName(TYPE.NVARCHAR2)

    # Number Category
    oracle_number = CaselessKeyword(TYPE.NUMBER).setResultsName(DATA_TYPE) \
                    + OPT_LBRACKET \
                    + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                    + OPT_RBRACKET
    oracle_number.setName(TYPE.NUMBER)

    oracle_binary_float = CaselessKeyword(TYPE.BINARY_FLOAT).setResultsName(DATA_TYPE)
    oracle_binary_float.setName(TYPE.BINARY_FLOAT)

    oracle_binary_double = CaselessKeyword(TYPE.BINARY_DOUBLE).setResultsName(DATA_TYPE)
    oracle_binary_double.setName(TYPE.BINARY_DOUBLE)

    oracle_float = CaselessKeyword(TYPE.FLOAT).setResultsName(DATA_TYPE) \
                   + OPT_LBRACKET \
                   + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                   + OPT_RBRACKET
    oracle_float.setName(TYPE.FLOAT)

    # Date & Time Category
    oracle_date = CaselessKeyword(TYPE.DATE).setResultsName(DATA_TYPE)
    oracle_date.setName(TYPE.DATE)

    oracle_timestamp = CaselessKeyword(TYPE.TIMESTAMP).setResultsName(DATA_TYPE) \
                       + OPT_LBRACKET \
                       + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                       + OPT_RBRACKET
    oracle_timestamp.setName(TYPE.TIMESTAMP)

    oracle_interval = CaselessKeyword(TYPE.INTERVAL).setResultsName(DATA_TYPE) \
                      + (CaselessKeyword("YEAR") | CaselessKeyword("DAY")).setResultsName("interval_type") \
                      + OPT_LBRACKET \
                      + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName("year_or_day_precision") \
                      + OPT_RBRACKET \
                      + CaselessKeyword("TO") \
                      + (CaselessKeyword("MONTH") | CaselessKeyword("SECOND")) \
                      + OPT_LBRACKET \
                      + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName("second_precision") \
                      + OPT_RBRACKET
    oracle_interval.setName(TYPE.INTERVAL)

    # Long & Raw Category
    oracle_long = CaselessKeyword(TYPE.LONG).setResultsName(DATA_TYPE)
    oracle_long.setName(TYPE.LONG)

    oracle_long_raw = CaselessKeyword(TYPE.LONG_RAW).setResultsName(DATA_TYPE)
    oracle_long_raw.setName(TYPE.LONG_RAW)

    oracle_raw = CaselessKeyword(TYPE.RAW).setResultsName(DATA_TYPE) \
                 + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_raw.setName(TYPE.RAW)

    # LOB Category
    oracle_clob = CaselessKeyword(TYPE.CLOB).setResultsName(DATA_TYPE)
    oracle_clob.setName(TYPE.CLOB)

    oracle_nclob = CaselessKeyword(TYPE.NCLOB).setResultsName(DATA_TYPE)
    oracle_nclob.setName(TYPE.NCLOB)

    oracle_blob = CaselessKeyword(TYPE.BLOB).setResultsName(DATA_TYPE)
    oracle_blob.setName(TYPE.BLOB)

    # etc. Category
    oracle_rowid = CaselessKeyword(TYPE.ROWID).setResultsName(DATA_TYPE)
    oracle_rowid.setName(TYPE.ROWID)

    data_type_def = oracle_long_raw | oracle_char | oracle_nchar | oracle_varchar2 | oracle_nvarchar2 | oracle_long \
                    | oracle_number | oracle_binary_float | oracle_binary_double | oracle_float \
                    | oracle_date | oracle_timestamp | oracle_interval \
                    | oracle_raw \
                    | oracle_clob | oracle_nclob | oracle_blob \
                    | oracle_rowid
    # LONG RAW 타입은 LONG 보다 먼저 Matching 되어야 해서 제일 앞으로 보냄

    return data_type_def


def _get_oracle_data_type(column):

    if column.data_type == TYPE.CHAR:
        if column.length_semantics == "BYTE":
            return oracle.CHAR(column.data_length)
        elif column.length_semantics == "CHAR":
            return CHARLENCHAR(column.data_length)

    elif column.data_type == TYPE.NCHAR:
        return oracle.NCHAR(column.data_length)

    elif column.data_type == TYPE.VARCHAR2:
        if column.length_semantics == "BYTE":
            return VARCHAR2LENBYTE(column.data_length)
        elif column.length_semantics == "CHAR":
            return oracle.VARCHAR2(column.data_length)

    elif column.data_type == TYPE.NVARCHAR2:
        return oracle.NVARCHAR2(column.data_length)

    elif column.data_type == TYPE.NUMBER:
        if len(column.data_length) == 1:
            return oracle.NUMBER(column.data_length[0])
        elif len(column.data_length) == 2:
            return oracle.NUMBER(column.data_length[0], column.data_length[1])
        else:
            return oracle.NUMBER

    elif column.data_type == TYPE.BINARY_FLOAT:
        return oracle.BINARY_FLOAT

    elif column.data_type == TYPE.BINARY_DOUBLE:
        return oracle.BINARY_DOUBLE

    elif column.data_type == TYPE.FLOAT:
        return oracle.FLOAT

    elif column.data_type == TYPE.DATE:
        return oracle.DATE

    elif column.data_type == TYPE.TIMESTAMP:
        return oracle.TIMESTAMP

    elif column.data_type == TYPE.INTERVAL:
        if column.interval_type == "YEAR":
            if column.year_or_day_precision is not None:
                return INTERVALYEARMONTH(column.year_or_day_precision)
            else:
                return INTERVALYEARMONTH
        elif column.interval_type == "DAY":
            if column.year_or_day_precision is None and column.second_precision is None:
                return oracle.INTERVAL
            elif column.year_or_day_precision is not None and column.second_precision is None:
                return oracle.INTERVAL(day_precision=column.year_or_day_precision)
            elif column.year_or_day_precision is None and column.second_precision is not None:
                return oracle.INTERVAL(second_precision=column.second_precision)
            elif column.year_or_day_precision is not None and column.second_precision is not None:
                return oracle.INTERVAL(day_precision=column.year_or_day_precision, second_precision=column.second_precision)

    elif column.data_type == TYPE.LONG:
        return oracle.LONG

    elif column.data_type == TYPE.LONG_RAW:
        return LONGRAW

    elif column.data_type == TYPE.RAW:
        return oracle.RAW(column.data_length)

    elif column.data_type == TYPE.CLOB:
        return oracle.CLOB

    elif column.data_type == TYPE.NCLOB:
        return oracle.NCLOB

    elif column.data_type == TYPE.BLOB:
        return oracle.BLOB

    elif column.data_type == TYPE.ROWID:
        return oracle.ROWID


def _signed_replace_bool(toks):
    if toks.unsigned == W_UNSIGNED:
        toks[W_UNSIGNED.lower()] = True
    else:
        toks[W_UNSIGNED.lower()] = False


def _zerofill_replace_bool(toks):
    if toks.zerofill != '':
        toks[W_ZEROFILL.lower()] = True
    else:
        toks[W_ZEROFILL.lower()] = False


def _mysql_data_type_parser():

    # String Category
    mysql_char = CaselessKeyword(TYPE.CHAR).setResultsName(DATA_TYPE) \
                 + OPT_LBRACKET \
                 + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                 + OPT_RBRACKET
    mysql_char.setName(TYPE.CHAR)

    mysql_nchar = CaselessKeyword(TYPE.NCHAR).setResultsName(DATA_TYPE) \
                  + OPT_LBRACKET \
                  + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                  + OPT_RBRACKET
    mysql_nchar.setName(TYPE.NCHAR)

    mysql_varchar = CaselessKeyword(TYPE.VARCHAR).setResultsName(DATA_TYPE) \
                    + LBRACKET + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) + RBRACKET
    mysql_varchar.setName(TYPE.VARCHAR)

    mysql_nvarchar = CaselessKeyword(TYPE.NVARCHAR).setResultsName(DATA_TYPE) \
                     + LBRACKET + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) + RBRACKET
    mysql_nvarchar.setName(TYPE.NVARCHAR)

    mysql_binary = CaselessKeyword(TYPE.BINARY).setResultsName(DATA_TYPE) \
                   + OPT_LBRACKET \
                   + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                   + OPT_RBRACKET
    mysql_binary.setName(TYPE.BINARY)

    mysql_varbinary = CaselessKeyword(TYPE.VARBINARY).setResultsName(DATA_TYPE) \
                      + LBRACKET + Word(nums).setParseAction(tokenMap(int)).setResultsName(DATA_LENGTH) + RBRACKET
    mysql_varbinary.setName(TYPE.VARBINARY)

    mysql_tinyblob = CaselessKeyword(TYPE.TINYBLOB).setResultsName(DATA_TYPE)
    mysql_tinyblob.setName(TYPE.TINYBLOB)

    mysql_tinytext = CaselessKeyword(TYPE.TINYTEXT).setResultsName(DATA_TYPE)
    mysql_tinytext.setName(TYPE.TINYTEXT)

    mysql_blob = CaselessKeyword(TYPE.BLOB).setResultsName(DATA_TYPE) \
                 + OPT_LBRACKET \
                 + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                 + OPT_RBRACKET
    mysql_blob.setName(TYPE.BLOB)

    mysql_text = CaselessKeyword(TYPE.TEXT).setResultsName(DATA_TYPE) \
                 + OPT_LBRACKET + Optional(Word(nums), default=None).setResultsName(DATA_LENGTH) + OPT_RBRACKET
    mysql_text.setName(TYPE.TEXT)

    mysql_mediumblob = CaselessKeyword(TYPE.MEDIUMBLOB).setResultsName(DATA_TYPE)
    mysql_mediumblob.setName(TYPE.MEDIUMBLOB)

    mysql_mediumtext = CaselessKeyword(TYPE.MEDIUMTEXT).setResultsName(DATA_TYPE)
    mysql_mediumtext.setName(TYPE.MEDIUMTEXT)

    mysql_longblob = CaselessKeyword(TYPE.LONGBLOB).setResultsName(DATA_TYPE)
    mysql_longblob.setName(TYPE.LONGBLOB)

    mysql_longtext = CaselessKeyword(TYPE.LONGTEXT).setResultsName(DATA_TYPE)
    mysql_longtext.setName(TYPE.LONGTEXT)

    # Numeric Category
    mysql_tinyint = CaselessKeyword(TYPE.TINYINT).setResultsName(DATA_TYPE) \
                    + OPT_LBRACKET \
                    + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                    + OPT_RBRACKET \
                    + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                    + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_tinyint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_tinyint.setName(TYPE.TINYINT)

    mysql_smallint = CaselessKeyword(TYPE.SMALLINT).setResultsName(DATA_TYPE) \
                     + OPT_LBRACKET \
                     + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                     + OPT_RBRACKET \
                     + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                     + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_smallint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_smallint.setName(TYPE.SMALLINT)

    mysql_mediumint = CaselessKeyword(TYPE.MEDIUMINT).setResultsName(DATA_TYPE) \
                      + OPT_LBRACKET \
                      + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                      + OPT_RBRACKET \
                      + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED)).setResultsName(W_SIGNED.lower()) \
                      + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_mediumint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_mediumint.setName(TYPE.MEDIUMINT)

    mysql_int = (CaselessKeyword(TYPE.INT) | CaselessKeyword(TYPE.INTEGER)).setResultsName(DATA_TYPE) \
                + OPT_LBRACKET \
                + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                + OPT_RBRACKET \
                + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_int.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_int.setName(TYPE.INT)

    mysql_bigint = CaselessKeyword(TYPE.BIGINT).setResultsName(DATA_TYPE) \
                   + OPT_LBRACKET \
                   + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                   + OPT_RBRACKET \
                   + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                   + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_bigint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_bigint.setName(TYPE.BIGINT)

    mysql_decimal = (CaselessKeyword(TYPE.DECIMAL) | CaselessKeyword(TYPE.NUMERIC)).setResultsName(DATA_TYPE) \
                    + OPT_LBRACKET \
                    + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                    + OPT_RBRACKET \
                    + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                    + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_decimal.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_decimal.setName(TYPE.DECIMAL)

    mysql_float = CaselessKeyword(TYPE.FLOAT).setResultsName(DATA_TYPE) \
                  + OPT_LBRACKET \
                  + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                  + OPT_RBRACKET \
                  + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                  + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_float.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_float.setName(TYPE.FLOAT)

    mysql_double = (CaselessKeyword(TYPE.DOUBLE) | CaselessKeyword(TYPE.REAL)).setResultsName(DATA_TYPE) \
                   + OPT_LBRACKET \
                   + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                   + OPT_RBRACKET \
                   + Optional(CaselessKeyword(W_UNSIGNED) | CaselessKeyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                   + Optional(CaselessKeyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_double.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)
    mysql_double.setName(TYPE.DOUBLE)

    # Date & Time Category
    mysql_time = CaselessKeyword(TYPE.TIME).setResultsName(DATA_TYPE) \
                 + OPT_LBRACKET \
                 + Optional(Word(nums).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                 + OPT_RBRACKET
    mysql_time.setName(TYPE.TIME)

    mysql_date = CaselessKeyword(TYPE.DATE).setResultsName(DATA_TYPE)
    mysql_date.setName(TYPE.DATE)

    mysql_year = CaselessKeyword(TYPE.YEAR).setResultsName(DATA_TYPE) \
                 + OPT_LBRACKET \
                 + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                 + OPT_RBRACKET
    mysql_year.setName(TYPE.YEAR)

    mysql_datetime = CaselessKeyword(TYPE.DATETIME).setResultsName(DATA_TYPE) \
                     + OPT_LBRACKET \
                     + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                     + OPT_RBRACKET
    mysql_datetime.setName(TYPE.DATETIME)

    mysql_timestamp = CaselessKeyword(TYPE.TIMESTAMP).setResultsName(DATA_TYPE) \
                      + OPT_LBRACKET \
                      + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                      + OPT_RBRACKET
    mysql_timestamp.setName(TYPE.TIMESTAMP)

    data_type_def = mysql_char | mysql_nchar | mysql_varchar | mysql_nvarchar \
                    | mysql_binary | mysql_varbinary \
                    | mysql_tinyblob | mysql_tinytext | mysql_blob | mysql_text \
                    | mysql_mediumblob | mysql_mediumtext | mysql_longblob | mysql_longtext \
                    | mysql_tinyint | mysql_smallint | mysql_mediumint | mysql_int | mysql_bigint \
                    | mysql_decimal | mysql_float | mysql_double \
                    | mysql_time | mysql_date | mysql_year | mysql_datetime | mysql_timestamp

    return data_type_def


def _get_mysql_data_type(column):

    if column.data_type == TYPE.CHAR:
        return mysql.CHAR(column.data_length)

    elif column.data_type == TYPE.NCHAR:
        return mysql.NCHAR(column.data_length)

    elif column.data_type == TYPE.VARCHAR:
        return mysql.VARCHAR(column.data_length)

    elif column.data_type == TYPE.NVARCHAR:
        return mysql.NVARCHAR(column.data_length)

    elif column.data_type == TYPE.BINARY:
        return mysql.BINARY(column.data_length)

    elif column.data_type == TYPE.VARBINARY:
        return mysql.VARBINARY

    elif column.data_type == TYPE.TINYBLOB:
        return mysql.TINYBLOB

    elif column.data_type == TYPE.TINYTEXT:
        return mysql.TINYTEXT

    elif column.data_type == TYPE.BLOB:
        return mysql.BLOB(column.data_length)

    elif column.data_type == TYPE.TEXT:
        return mysql.TEXT(column.data_length)

    elif column.data_type == TYPE.MEDIUMBLOB:
        return mysql.MEDIUMBLOB

    elif column.data_type == TYPE.MEDIUMTEXT:
        return mysql.MEDIUMTEXT

    elif column.data_type == TYPE.LONGBLOB:
        return mysql.LONGBLOB

    elif column.data_type == TYPE.LONGTEXT:
        return mysql.LONGTEXT

    elif column.data_type == TYPE.TINYINT:
        return mysql.TINYINT(column.data_length, unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.SMALLINT:
        return mysql.SMALLINT(column.data_length, unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.MEDIUMINT:
        return mysql.MEDIUMINT(column.data_length, unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.INT or column.data_type == TYPE.INTEGER:
        return mysql.INTEGER(column.data_length, unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.BIGINT:
        return mysql.BIGINT(column.data_length, unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.DECIMAL or column.data_type == TYPE.NUMERIC:
        if len(column.data_length) == 1:
            return mysql.DECIMAL(precision=column.data_length[0], unsigned=column.unsigned, zerofill=column.zerofill)
        elif len(column.data_length) == 2:
            return mysql.DECIMAL(precision=column.data_length[0], scale=column.data_length[1],
                                 unsigned=column.unsigned, zerofill=column.zerofill)
        else:
            return mysql.DECIMAL(unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.FLOAT:
        if len(column.data_length) == 1:
            return mysql.FLOAT(precision=column.data_length[0], unsigned=column.unsigned, zerofill=column.zerofill)
        elif len(column.data_length) == 2:
            return mysql.FLOAT(precision=column.data_length[0], scale=column.data_length[1],
                               unsigned=column.unsigned, zerofill=column.zerofill)
        else:
            return mysql.FLOAT(unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.DOUBLE or column.data_type == TYPE.REAL:
        if len(column.data_length) == 1:
            return mysql.DOUBLE(precision=column.data_length[0], unsigned=column.unsigned, zerofill=column.zerofill)
        elif len(column.data_length) == 2:
            return mysql.DOUBLE(precision=column.data_length[0], scale=column.data_length[0],
                                unsigned=column.unsigned, zerofill=column.zerofill)
        else:
            return mysql.DOUBLE(unsigned=column.unsigned, zerofill=column.zerofill)

    elif column.data_type == TYPE.TIME:
        return mysql.TIME(fsp=column.data_length)

    elif column.data_type == TYPE.DATE:
        return mysql.DATE

    elif column.data_type == TYPE.YEAR:
        return mysql.YEAR(column.data_length)

    elif column.data_type == TYPE.DATETIME:
        return mysql.DATETIME(fsp=column.data_length)

    elif column.data_type == TYPE.TIMESTAMP:
        return mysql.TIMESTAMP(fsp=column.data_length)


def _sqlserver_data_type_parser():

    # Character Category
    sqlserver_char = CaselessKeyword(TYPE.CHAR).setResultsName(DATA_TYPE) \
                     + OPT_LBRACKET + Optional(Word(nums), default=None).setResultsName(DATA_LENGTH) + OPT_RBRACKET
    sqlserver_char.setName(TYPE.CHAR)

    sqlserver_varchar = CaselessKeyword(TYPE.VARCHAR).setResultsName(DATA_TYPE) \
                        + OPT_LBRACKET \
                        + Optional(Word(nums).setParseAction(tokenMap(int)) | CaselessKeyword("MAX"), default=None)\
                          .setResultsName(DATA_LENGTH) \
                        + OPT_RBRACKET
    sqlserver_varchar.setName(TYPE.VARCHAR)

    sqlserver_nchar = CaselessKeyword(TYPE.NCHAR).setResultsName(DATA_TYPE) \
                      + OPT_LBRACKET \
                      + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                      + OPT_RBRACKET
    sqlserver_nchar.setName(TYPE.NCHAR)

    sqlserver_nvarchar = CaselessKeyword(TYPE.NVARCHAR).setResultsName(DATA_TYPE) \
                         + OPT_LBRACKET \
                         + Optional(Word(nums).setParseAction(tokenMap(int)) | CaselessKeyword("MAX"), default=None)\
                           .setResultsName(DATA_LENGTH) \
                         + OPT_RBRACKET
    sqlserver_nvarchar.setName(TYPE.NVARCHAR)

    # Numeric Category
    sqlserver_bit = CaselessKeyword(TYPE.BIT).setResultsName(DATA_TYPE)
    sqlserver_bit.setName(TYPE.BIT)

    sqlserver_tinyint = CaselessKeyword(TYPE.TINYINT).setResultsName(DATA_TYPE)
    sqlserver_tinyint.setName(TYPE.TINYINT)

    sqlserver_smallint = CaselessKeyword(TYPE.SMALLINT).setResultsName(DATA_TYPE)
    sqlserver_smallint.setName(TYPE.SMALLINT)

    sqlserver_int = CaselessKeyword(TYPE.INT).setResultsName(DATA_TYPE)
    sqlserver_int.setName(TYPE.INT)

    sqlserver_bigint = CaselessKeyword(TYPE.BIGINT).setResultsName(DATA_TYPE)
    sqlserver_bigint.setName(TYPE.BIGINT)

    sqlserver_decimal = (CaselessKeyword(TYPE.DECIMAL) | CaselessKeyword(TYPE.NUMERIC)).setResultsName(DATA_TYPE) \
                         + OPT_LBRACKET \
                         + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                         + OPT_RBRACKET
    sqlserver_decimal.setName(TYPE.DECIMAL)

    sqlserver_float = (CaselessKeyword(TYPE.FLOAT) | CaselessKeyword(TYPE.DOUBLE_PRECISION)).setResultsName(DATA_TYPE) \
                      + OPT_LBRACKET \
                      + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                      + OPT_RBRACKET
    sqlserver_float.setName(TYPE.FLOAT)

    sqlserver_real = CaselessKeyword(TYPE.REAL).setResultsName(DATA_TYPE)
    sqlserver_real.setName(TYPE.REAL)

    sqlserver_smallmoney = CaselessKeyword(TYPE.SMALLMONEY).setResultsName(DATA_TYPE)
    sqlserver_smallmoney.setName(TYPE.SMALLMONEY)

    sqlserver_money = CaselessKeyword(TYPE.MONEY).setResultsName(DATA_TYPE)
    sqlserver_money.setName(TYPE.MONEY)

    # Date & Time Category
    sqlserver_date = CaselessKeyword(TYPE.DATE).setResultsName(DATA_TYPE)
    sqlserver_date.setName(TYPE.DATE)

    sqlserver_time = CaselessKeyword(TYPE.TIME).setResultsName(DATA_TYPE) \
                     + OPT_LBRACKET \
                     + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                     + OPT_RBRACKET
    sqlserver_time.setName(TYPE.TIME)

    sqlserver_datetime = CaselessKeyword(TYPE.DATETIME).setResultsName(DATA_TYPE)
    sqlserver_datetime.setName(TYPE.DATETIME)

    sqlserver_datetime2 = CaselessKeyword(TYPE.DATETIME2).setResultsName(DATA_TYPE) \
                          + OPT_LBRACKET \
                          + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                          + OPT_RBRACKET
    sqlserver_datetime2.setName(TYPE.DATETIME2)

    sqlserver_smalldatetime = CaselessKeyword(TYPE.SMALLDATETIME).setResultsName(DATA_TYPE)
    sqlserver_smalldatetime.setName(TYPE.SMALLDATETIME)

    sqlserver_datetimeoffset = CaselessKeyword(TYPE.DATETIMEOFFSET).setResultsName(DATA_TYPE) \
                               + OPT_LBRACKET \
                               + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                               + OPT_RBRACKET
    sqlserver_datetimeoffset.setName(TYPE.DATETIMEOFFSET)

    sqlserver_binary = CaselessKeyword(TYPE.BINARY).setResultsName(DATA_TYPE) \
                       + OPT_LBRACKET \
                       + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                       + OPT_RBRACKET
    sqlserver_binary.setName(TYPE.BINARY)

    sqlserver_varbinary = CaselessKeyword(TYPE.VARBINARY).setResultsName(DATA_TYPE) \
                          + OPT_LBRACKET \
                          + Optional(Word(nums).setParseAction(tokenMap(int)) | CaselessKeyword("MAX"), default=None)\
                            .setResultsName(DATA_LENGTH) \
                          + OPT_RBRACKET
    sqlserver_varbinary.setName(TYPE.VARBINARY)

    data_type_def = sqlserver_char | sqlserver_varchar | sqlserver_nchar | sqlserver_nvarchar \
                    | sqlserver_bit | sqlserver_tinyint | sqlserver_smallint | sqlserver_int | sqlserver_bigint \
                    | sqlserver_decimal | sqlserver_float | sqlserver_real | sqlserver_smallmoney | sqlserver_money \
                    | sqlserver_datetimeoffset | sqlserver_datetime2 | sqlserver_datetime | sqlserver_smalldatetime \
                    | sqlserver_date | sqlserver_time \
                    | sqlserver_binary | sqlserver_varbinary

    return data_type_def


def _get_sqlserver_data_type(column):

    if column.data_type == TYPE.CHAR:
        return sqlserver.CHAR(column.data_length)

    elif column.data_type == TYPE.VARCHAR:
        return sqlserver.VARCHAR(column.data_length)

    elif column.data_type == TYPE.NCHAR:
        return sqlserver.NCHAR(column.data_length)

    elif column.data_type == TYPE.NVARCHAR:
        return sqlserver.NVARCHAR(column.data_length)

    elif column.data_type == TYPE.BIT:
        return sqlserver.BIT

    elif column.data_type == TYPE.TINYINT:
        return sqlserver.TINYINT

    elif column.data_type == TYPE.SMALLINT:
        return sqlserver.SMALLINT

    elif column.data_type == TYPE.INT:
        return sqlserver.INTEGER

    elif column.data_type == TYPE.BIGINT:
        return sqlserver.BIGINT

    elif column.data_type == TYPE.DECIMAL or column.data_type == TYPE.NUMERIC:
        if len(column.data_length) == 1:
            return sqlserver.DECIMAL(precision=column.data_length[0])
        elif len(column.data_length) == 2:
            return sqlserver.DECIMAL(precision=column.data_length[0], scale=column.data_length[1])
        else:
            return sqlserver.DECIMAL

    elif column.data_type == TYPE.FLOAT or column.data_type == TYPE.DOUBLE_PRECISION:
        return sqlserver.FLOAT(column.data_length)

    elif column.data_type == TYPE.REAL:
        return sqlserver.REAL

    elif column.data_type == TYPE.SMALLMONEY:
        return sqlserver.SMALLMONEY

    elif column.data_type == TYPE.MONEY:
        return sqlserver.MONEY

    elif column.data_type == TYPE.DATE:
        return sqlserver.DATE

    elif column.data_type == TYPE.TIME:
        return sqlserver.TIME(column.data_length)

    elif column.data_type == TYPE.DATETIME:
        return sqlserver.DATETIME

    elif column.data_type == TYPE.DATETIME2:
        return sqlserver.DATETIME2(column.data_length)

    elif column.data_type == TYPE.SMALLDATETIME:
        return sqlserver.SMALLDATETIME

    elif column.data_type == TYPE.DATETIMEOFFSET:
        return sqlserver.DATETIMEOFFSET(column.data_length)

    elif column.data_type == TYPE.BINARY:
        return sqlserver.BINARY(column.data_length)

    elif column.data_type == TYPE.VARBINARY:
        return sqlserver.VARBINARY(column.data_length)


def _postgresql_data_type_parser():

    # Character Category
    pg_char = CaselessKeyword(TYPE.CHAR).setResultsName(DATA_TYPE) \
              + OPT_LBRACKET \
              + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
              + OPT_RBRACKET
    pg_char.setName(TYPE.CHAR)

    pg_varchar = CaselessKeyword(TYPE.VARCHAR).setResultsName(DATA_TYPE) \
                 + OPT_LBRACKET \
                 + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                 + OPT_RBRACKET
    pg_varchar.setName(TYPE.VARCHAR)

    pg_text = CaselessKeyword(TYPE.TEXT).setResultsName(DATA_TYPE)
    pg_text.setName(TYPE.TEXT)

    # Numeric Category
    pg_smallint = CaselessKeyword(TYPE.SMALLINT).setResultsName(DATA_TYPE)
    pg_smallint.setName(TYPE.SMALLINT)

    pg_integer = (CaselessKeyword(TYPE.INTEGER) | CaselessKeyword(TYPE.INT)).setResultsName(DATA_TYPE)
    pg_integer.setName(TYPE.INTEGER)

    pg_bigint = CaselessKeyword(TYPE.BIGINT).setResultsName(DATA_TYPE)
    pg_bigint.setName(TYPE.BIGINT)

    pg_numeric = (CaselessKeyword(TYPE.NUMERIC) | CaselessKeyword(TYPE.DECIMAL)).setResultsName(DATA_TYPE) \
                  + OPT_LBRACKET \
                  + Optional(delimitedList(Word(nums)).setParseAction(tokenMap(int))).setResultsName(DATA_LENGTH) \
                  + OPT_RBRACKET
    pg_numeric.setName(TYPE.NUMERIC)

    pg_real = CaselessKeyword(TYPE.REAL).setResultsName(DATA_TYPE)
    pg_real.setName(TYPE.REAL)

    pg_double_precision = CaselessKeyword(TYPE.DOUBLE_PRECISION).setResultsName(DATA_TYPE)
    pg_double_precision.setName(TYPE.DOUBLE_PRECISION)

    # Monetary Category
    pg_money = CaselessKeyword(TYPE.MONEY).setResultsName(DATA_TYPE)
    pg_money.setName(TYPE.MONEY)

    # Date & Time Category
    pg_timestamp = CaselessKeyword(TYPE.TIMESTAMP).setResultsName(DATA_TYPE) \
                   + OPT_LBRACKET \
                   + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                   + OPT_RBRACKET
    pg_timestamp.setName(TYPE.TIMESTAMP)

    pg_date = CaselessKeyword(TYPE.DATE).setResultsName(DATA_TYPE)
    pg_date.setName(TYPE.DATE)

    pg_time = CaselessKeyword(TYPE.TIME).setResultsName(DATA_TYPE) \
              + OPT_LBRACKET \
              + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
              + OPT_RBRACKET
    pg_time.setName(TYPE.TIME)

    pg_interval_field = list(map(CaselessKeyword, ["YEAR TO MONTH", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND",
                                                   "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND",
                                                   "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]))
    pg_interval = CaselessKeyword(TYPE.INTERVAL).setResultsName(DATA_TYPE) \
                  + Optional(MatchFirst(pg_interval_field), default=None).setResultsName("interval_field") \
                  + OPT_LBRACKET \
                  + Optional(Word(nums).setParseAction(tokenMap(int)), default=None).setResultsName(DATA_LENGTH) \
                  + OPT_RBRACKET
    pg_interval.setName(TYPE.INTERVAL)

    pg_bytea = CaselessKeyword(TYPE.BYTEA).setResultsName(DATA_TYPE)
    pg_bytea.setName(TYPE.BYTEA)

    data_type_def = pg_char | pg_varchar | pg_text \
                    | pg_smallint | pg_integer | pg_bigint | pg_numeric | pg_real | pg_double_precision \
                    | pg_money \
                    | pg_timestamp | pg_date | pg_time | pg_interval \
                    | pg_bytea

    return data_type_def


def _get_postgresql_data_type(column):

    if column.data_type == TYPE.CHAR:
        return postgresql.CHAR(length=column.data_length)

    elif column.data_type == TYPE.VARCHAR:
        return postgresql.VARCHAR(length=column.data_length)

    elif column.data_type == TYPE.TEXT:
        return postgresql.TEXT

    elif column.data_type == TYPE.SMALLINT:
        return postgresql.SMALLINT

    elif column.data_type == TYPE.INTEGER or column.data_type == TYPE.INT:
        return postgresql.INTEGER

    elif column.data_type == TYPE.BIGINT:
        return postgresql.BIGINT

    elif column.data_type == TYPE.NUMERIC or column.data_type == TYPE.DECIMAL:
        if len(column.data_length) == 1:
            return postgresql.NUMERIC(precision=column.data_length[0])
        elif len(column.data_length) == 2:
            return postgresql.NUMERIC(precision=column.data_length[0], scale=column.data_length[1])
        else:
            return postgresql.NUMERIC

    elif column.data_type == TYPE.REAL:
        return postgresql.REAL

    elif column.data_type == TYPE.DOUBLE_PRECISION:
        return postgresql.DOUBLE_PRECISION

    elif column.data_type == TYPE.MONEY:
        return postgresql.MONEY

    elif column.data_type == TYPE.TIMESTAMP:
        return postgresql.TIMESTAMP(precision=column.data_length)

    elif column.data_type == TYPE.DATE:
        return postgresql.DATE

    elif column.data_type == TYPE.TIME:
        return postgresql.TIME(precision=column.data_length)

    elif column.data_type == TYPE.INTERVAL:
        return postgresql.INTERVAL(fields=column.interval_field, precision=column.data_length)

    elif column.data_type == TYPE.BYTEA:
        return postgresql.BYTEA
