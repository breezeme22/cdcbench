from sqlalchemy import Column, Sequence, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.dialects import oracle, mysql

from commons.constants import *
from commons.oracle_custom_types import VARCHAR2BYTE, LONGRAW, INTERVALYEARMONTH

from pyparsing import Word, delimitedList, Optional, Group, alphas, nums, alphanums, OneOrMore, \
    Keyword, Suppress, oneOf, Forward, ParseException, ParseFatalException, ParseBaseException, FollowedBy

import os
import traceback


class MapperManager:

    __definition_dir = "definitions"

    def __init__(self, connection):

        self.dbms_type = connection.connection_info["dbms_type"]
        self.schema_name = connection.connection_info["schema_name"]
        self.db_session = connection.db_session

        def_file_path = os.path.join(os.path.join(self.__definition_dir, self.dbms_type.lower()))
        def_files = os.listdir(def_file_path)

        for def_file in def_files:

            table_metadata = _table_definition_parser(self.dbms_type, os.path.join(def_file_path, def_file))[0]

            # Table Name 생성
            mapper_attr = {"__tablename__": table_metadata.table_name}

            # Column 정보 생성
            for idx, column in enumerate(table_metadata.columns):

                # DBMS별 Data Type 생성
                data_type = None
                if self.dbms_type == ORACLE:
                    data_type = _get_oracle_data_type(column)
                elif self.dbms_type == MYSQL:
                    pass
                elif self.dbms_type == SQLSERVER:
                    pass
                elif self.dbms_type == POSTGRESQL:
                    pass

                # 첫 번째 컬럼은 Sequence 함께 생성
                if idx == 0:
                    mapper_attr[column.column_name] = Column(column.column_name, data_type,
                                                             Sequence("{}_SEQ".format(table_metadata.table_name)),
                                                             nullable=column.nullable)
                else:
                    mapper_attr[column.column_name] = Column(column.column_name, data_type, nullable=column.nullable)

            # Primary Key Constraint 정보 생성
            mapper_attr["__table_args__"] = (PrimaryKeyConstraint(*table_metadata.constraint.key_column,
                                                                  name=table_metadata.constraint.constraint_name),)

            # Table Mapper를 DBMS별 Mapper Base에 등록
            type("{}".format(table_metadata.table_name.title()), (OracleMapperBase,), mapper_attr)

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


LBRACKET = Suppress("(").setName("LBRACKET")
RBRACKET = Suppress(")").setName("RBRACKET")


def _nullable_replace_bool(toks):
    if toks.nullable == "NULL":
        return True
    else:
        return False


def _debug_act(toks):
    print(toks)


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

    nullable_expr = Optional(Keyword(W_NOT_NULL) | Keyword(W_NULL) | _error(MissingKeywordException),
                             default=W_NULL).setResultsName("nullable")
    nullable_expr.setName("nullable_expr")
    nullable_expr.setParseAction(_nullable_replace_bool)

    column_expr = Group(column_name_expr + data_type_expr + nullable_expr) + Suppress(",")
    column_expr.setName("column_expr")

    column_list_expr = Forward()
    column_list_expr.setName("column_list_expr")

    PRIMARY_KEY = Keyword(W_PRIMARY_KEY) | _error(MissingKeywordException)
    PRIMARY_KEY.setName(W_PRIMARY_KEY)

    key_expr = PRIMARY_KEY.setResultsName("key_type") \
               + LBRACKET + delimitedList(object_name).setResultsName("key_column") + RBRACKET

    CONSTRAINT = Keyword(W_CONSTRAINT) | _error(MissingKeywordException)
    CONSTRAINT.setName(W_CONSTRAINT)

    constraint_expr = Group(CONSTRAINT + object_name.setResultsName("constraint_name") + key_expr).setResultsName("constraint")
    constraint_expr.setName("constraint_expr")

    # column_list_def Expression을 constraint 절 전에 멈춤
    column_list_expr <<= OneOrMore(column_expr, stopOn=constraint_expr).setResultsName("columns")

    table_name_expr = object_name.setResultsName("table_name")
    table_name_expr.setName("table_name_expr")

    table_expr = Group(table_name_expr + LBRACKET + column_list_expr +
                       constraint_expr + RBRACKET + Suppress(";")).setResultsName("table")

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
    BINARY_FLOAT = "BINARY_FLOAT"
    BINARY_DOUBLE = "BINARY_DOUBLE"

    # Date & Time Category
    TIME = "TIME"
    YEAR = "YEAR"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"

    # Binary Category
    RAW = "RAW"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"

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


def _oracle_data_type_parser():

    # String Category
    oracle_char = Keyword(TYPE.CHAR).setResultsName(DATA_TYPE) \
                  + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_char.setName(TYPE.CHAR)

    oracle_nchar = Keyword(TYPE.NCHAR).setResultsName(DATA_TYPE) \
                   + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_nchar.setName(TYPE.NCHAR)

    oracle_varchar2 = Keyword(TYPE.VARCHAR2).setResultsName(DATA_TYPE) \
                      + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) \
                      + Optional(oneOf("BYTE CHAR", asKeyword=True), default="BYTE").setResultsName("varchar2_type") \
                      + RBRACKET
    oracle_varchar2.setName(TYPE.VARCHAR2)

    oracle_nvarchar2 = Keyword(TYPE.NVARCHAR2).setResultsName(DATA_TYPE) \
                       + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_nvarchar2.setName(TYPE.NVARCHAR2)

    oracle_long = Keyword(TYPE.LONG).setResultsName(DATA_TYPE)
    oracle_long.setName(TYPE.LONG)

    # Numeric Category
    oracle_number = Keyword(TYPE.NUMBER).setResultsName(DATA_TYPE) \
                    + Optional(LBRACKET + delimitedList(Word(nums)).setResultsName(DATA_LENGTH) + RBRACKET)
    oracle_number.setName(TYPE.NUMBER)

    oracle_binary_float = Keyword(TYPE.BINARY_FLOAT).setResultsName(DATA_TYPE)
    oracle_binary_float.setName(TYPE.BINARY_FLOAT)

    oracle_binary_double = Keyword(TYPE.BINARY_DOUBLE).setResultsName(DATA_TYPE)
    oracle_binary_double.setName(TYPE.BINARY_DOUBLE)

    oracle_float = Keyword(TYPE.FLOAT).setResultsName(DATA_TYPE)
    oracle_float.setName(TYPE.FLOAT)

    # Date & Time Category
    oracle_date = Keyword(TYPE.DATE).setResultsName(DATA_TYPE)
    oracle_date.setName(TYPE.DATE)

    oracle_timestamp = Keyword(TYPE.TIMESTAMP).setResultsName(DATA_TYPE) \
                       + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET)
    oracle_timestamp.setName(TYPE.TIMESTAMP)

    oracle_interval = Keyword(TYPE.INTERVAL).setResultsName(DATA_TYPE) \
                      + oneOf("YEAR DAY", asKeyword=True).setResultsName("year_or_day") \
                      + Optional(LBRACKET + Word(nums).setResultsName("year_or_day_precision") + RBRACKET) \
                      + Keyword("TO") \
                      + oneOf("MONTH SECOND", asKeyword=True).setResultsName("month_or_second") \
                      + Optional(LBRACKET + Word(nums).setResultsName("second_precision") + RBRACKET)
    oracle_interval.setName(TYPE.INTERVAL)

    # Binary Category
    oracle_raw = Keyword(TYPE.RAW).setResultsName(DATA_TYPE) \
                 + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET
    oracle_raw.setName(TYPE.RAW)

    oracle_long_raw = Keyword(TYPE.LONG_RAW).setResultsName(DATA_TYPE)
    oracle_long_raw.setName(TYPE.LONG_RAW)

    # LOB Category
    oracle_clob = Keyword(TYPE.CLOB).setResultsName(DATA_TYPE)
    oracle_clob.setName(TYPE.CLOB)

    oracle_nclob = Keyword(TYPE.NCLOB).setResultsName(DATA_TYPE)
    oracle_nclob.setName(TYPE.NCLOB)

    oracle_blob = Keyword(TYPE.BLOB).setResultsName(DATA_TYPE)
    oracle_blob.setName(TYPE.BLOB)

    # etc. Category
    oracle_rowid = Keyword(TYPE.ROWID).setResultsName(DATA_TYPE)
    oracle_rowid.setName(TYPE.ROWID)

    data_type_def = oracle_long_raw | oracle_char | oracle_nchar | oracle_varchar2 | oracle_nvarchar2 | oracle_long \
                    | oracle_number | oracle_binary_float | oracle_binary_double | oracle_float \
                    | oracle_date | oracle_timestamp | oracle_interval \
                    | oracle_raw \
                    | oracle_clob | oracle_nclob | oracle_blob \
                    | oracle_rowid \
    # LONG RAW 타입은 LONG 보다 먼저 Matching 되어야 해서 제일 앞으로 보냄

    return data_type_def


def _get_oracle_data_type(column):

    if column.data_type == TYPE.CHAR:
        return oracle.CHAR(int(column.data_length))
    elif column.data_type == TYPE.NCHAR:
        return oracle.NCHAR(int(column.data_length))
    elif column.data_type == TYPE.VARCHAR2:
        if column.varchar2_type == "BYTE":
            return VARCHAR2BYTE(int(column.data_length))
        elif column.varchar2_type == "CHAR":
            return oracle.VARCHAR2(int(column.data_length))
    elif column.data_type == TYPE.NVARCHAR2:
        return oracle.NVARCHAR2(int(column.data_length))
    elif column.data_type == TYPE.LONG:
        return oracle.LONG
    elif column.data_type == TYPE.NUMBER:
        if len(column.data_length) == 1:
            return oracle.NUMBER(int(column.data_length[0]))
        elif len(column.data_length) == 2:
            return oracle.NUMBER(int(column.data_length[0]), int(column.data_length[1]))
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
        if column.year_or_day == "YEAR":
            if column.year_or_day_precision != '':
                return INTERVALYEARMONTH(int(column.year_or_day_precision))
            else:
                return INTERVALYEARMONTH
        elif column.year_or_day == "DAY":
            if column.year_or_day_precision == '' and column.second_precision == '':
                return oracle.INTERVAL
            elif column.year_or_day_precision != '' and column.second_precision == '':
                return oracle.INTERVAL(day_precision=int(column.year_or_day_precision))
            elif column.year_or_day_precision == '' and column.second_precision != '':
                return oracle.INTERVAL(second_precision=int(column.second_precision))
            elif column.year_or_day_precision != '' and column.second_precision != '':
                return oracle.INTERVAL(day_precision=int(column.year_or_day_precision),
                                       second_precision=int(column.second_precision))
    elif column.data_type == TYPE.RAW:
        return oracle.RAW(int(column.data_length))
    elif column.data_type == TYPE.LONG_RAW:
        return LONGRAW
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
    mysql_char = Keyword(TYPE.CHAR).setResultsName(DATA_TYPE) \
                 + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET

    mysql_nchar = Keyword(TYPE.NCHAR).setResultsName(DATA_TYPE) \
                  + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET

    mysql_varchar = Keyword(TYPE.VARCHAR).setResultsName(DATA_TYPE) \
                    + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET

    mysql_nvarchar = Keyword(TYPE.NVARCHAR).setResultsName(DATA_TYPE) \
                     + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET

    # Numeric Category
    mysql_tinyint = Keyword(TYPE.TINYINT).setResultsName(DATA_TYPE) \
                    + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET) \
                    + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_UNSIGNED.lower()) \
                    + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_tinyint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_smallint = Keyword(TYPE.SMALLINT).setResultsName(DATA_TYPE) \
                     + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET) \
                     + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                     + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_tinyint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_mediumint = Keyword(TYPE.MEDIUMINT).setResultsName(DATA_TYPE) \
                      + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET) \
                      + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED)).setResultsName(W_SIGNED.lower()) \
                      + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_mediumint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_int = oneOf("{} {}".format(TYPE.INT, TYPE.INTEGER), asKeyword=True).setResultsName(DATA_TYPE) \
                + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET) \
                + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_int.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_bigint = Keyword(TYPE.BIGINT).setResultsName(DATA_TYPE) \
                   + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET) \
                   + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                   + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_bigint.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_decimal = Keyword(TYPE.DECIMAL).setResultsName(DATA_TYPE) \
                    + Optional(LBRACKET + delimitedList(Word(nums).setResultsName(DATA_LENGTH)) + RBRACKET) \
                    + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                    + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_decimal.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_numeric = Keyword(TYPE.NUMERIC).setResultsName(DATA_TYPE) \
                    + Optional(LBRACKET + delimitedList(Word(nums).setResultsName(DATA_LENGTH)) + RBRACKET) \
                    + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                    + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_numeric.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_float = Keyword(TYPE.FLOAT).setResultsName(DATA_TYPE) \
                  + Optional(LBRACKET + delimitedList(Word(nums).setResultsName(DATA_LENGTH)) + RBRACKET) \
                  + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                  + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_float.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    mysql_double = Keyword(TYPE.DOUBLE).setResultsName(DATA_TYPE) \
                   + Optional(LBRACKET + delimitedList(Word(nums).setResultsName(DATA_LENGTH)) + RBRACKET) \
                   + Optional(Keyword(W_UNSIGNED) | Keyword(W_SIGNED), default=W_SIGNED).setResultsName(W_SIGNED.lower()) \
                   + Optional(Keyword(W_ZEROFILL)).setResultsName(W_ZEROFILL.lower())
    mysql_double.setParseAction(_signed_replace_bool).addParseAction(_zerofill_replace_bool)

    # Date & Time Category
    mysql_time = Keyword(TYPE.TIME).setResultsName(DATA_TYPE) \
                 + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET)

    mysql_date = Keyword(TYPE.DATE).setResultsName(DATA_TYPE)

    mysql_year = Keyword(TYPE.YEAR).setResultsName(DATA_TYPE) \
                 + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET)

    mysql_datetime = Keyword(TYPE.DATETIME).setResultsName(DATA_TYPE) \
                     + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET)

    mysql_timestamp = Keyword(TYPE.TIMESTAMP).setResultsName(DATA_TYPE) \
                      + Optional(LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET)

    # Binary Category
    mysql_binary = Keyword(TYPE.BINARY).setResultsName(DATA_TYPE) \
                   + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET

    mysql_varbinary = Keyword(TYPE.VARBINARY).setResultsName(DATA_TYPE) \
                      + LBRACKET + Word(nums).setResultsName(DATA_LENGTH) + RBRACKET

    # LOB Category
    mysql_tinytext = Keyword(TYPE.TINYTEXT).setResultsName(DATA_TYPE)

    mysql_text = Keyword(TYPE.TEXT).setResultsName(DATA_TYPE)

    mysql_mediumtext = Keyword(TYPE.MEDIUMTEXT).setResultsName(DATA_TYPE)

    mysql_longtext= Keyword(TYPE.LONGTEXT).setResultsName(DATA_TYPE)

    mysql_tinyblob = Keyword(TYPE.TINYBLOB).setResultsName(DATA_TYPE)

    mysql_blob = Keyword(TYPE.BLOB).setResultsName(DATA_TYPE)

    mysql_mediumblob = Keyword(TYPE.MEDIUMBLOB).setResultsName(DATA_TYPE)

    mysql_longblob = Keyword(TYPE.LONGBLOB).setResultsName(DATA_TYPE)

    data_type_def = mysql_char | mysql_nchar | mysql_varchar | mysql_nvarchar \
                     | mysql_tinyint | mysql_smallint | mysql_mediumint | mysql_int | mysql_bigint \
                     | mysql_decimal | mysql_numeric | mysql_float | mysql_double \
                     | mysql_time | mysql_date | mysql_year | mysql_datetime | mysql_timestamp \
                     | mysql_binary | mysql_varbinary \
                     | mysql_tinytext | mysql_text | mysql_mediumtext | mysql_longtext \
                     | mysql_tinyblob | mysql_blob | mysql_mediumblob | mysql_longblob

    return data_type_def


def _get_mysql_data_type(column):

    if column.data_type == TYPE.CHAR:
        return mysql.CHAR(int(column.data_length))
    elif column.data_type == TYPE.NCHAR:
        return mysql.NCHAR(int(column.data_length))
    elif column.data_type == TYPE.VARCHAR:
        return mysql.VARCHAR(int(column.data_length))
    elif column.data_type == TYPE.NVARCHAR:
        return mysql.NVARCHAR(int(column.data_length))


def _sqlserver_data_type_parser():

    data_type_def = ""

    return data_type_def


def _get_sqlserver_data_type(column):
    pass


def _postgresql_data_type_parser():

    data_type_def = ""

    return data_type_def


def _get_postgresql_data_type(column):
    pass


