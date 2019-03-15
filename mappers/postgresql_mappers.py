from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy import Column, Sequence
from sqlalchemy.types import DECIMAL
from sqlalchemy.dialects.postgresql import \
        CHAR, VARCHAR, TEXT, \
        BIGINT, BOOLEAN, DOUBLE_PRECISION, INTEGER, MONEY, NUMERIC, REAL, SMALLINT, \
        INTERVAL, TIMESTAMP, \
        BYTEA

from commons.constants import *


@as_declarative()
class PostgresqlMapperBase:
    pass


class InsertTest(PostgresqlMapperBase):
    """
    테이블 INSERT_TEST의 Mapper Class
    """

    __tablename__ = INSERT_TEST.lower()
    PRODUCT_ID = Column("product_id", INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column("product_name", VARCHAR(50))
    PRODUCT_DATE = Column("product_date", TIMESTAMP)
    SEPARATE_COL = Column("separate_col", INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<InsertTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


class UpdateTest(PostgresqlMapperBase):
    """
    테이블 UPDATE_TEST의 Mapper Class
    """

    __tablename__ = UPDATE_TEST.lower()
    PRODUCT_ID = Column("product_id", INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column("product_name", VARCHAR(50))
    PRODUCT_DATE = Column("product_date", TIMESTAMP)
    SEPARATE_COL = Column("separate_col", INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<UpdateTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


class DeleteTest(PostgresqlMapperBase):
    """
    테이블 DELETE_TEST의 Mapper Class
    """

    __tablename__ = DELETE_TEST.lower()
    PRODUCT_ID = Column("product_id", INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column("product_name", VARCHAR(50))
    PRODUCT_DATE = Column("product_date", TIMESTAMP)
    SEPARATE_COL = Column("separate_col", INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<DeleteTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


class StringTest(PostgresqlMapperBase):
    """
    테이블 STRING_TEST의 Mapper Class
    """

    __tablename__ = STRING_TEST.lower()
    T_ID = Column("t_id", INTEGER, nullable=False, primary_key=True)
    COL_CHAR = Column("col_char", CHAR(50))
    COL_NCHAR = Column("col_nchar", CHAR(50))
    COL_VARCHAR_B = Column("col_varchar_b", VARCHAR(4000))
    COL_VARCHAR_C = Column("col_varchar_c", VARCHAR(4000))
    COL_NVARCHAR = Column("col_nvarchar", VARCHAR(2000))
    COL_TEXT = Column("col_text", TEXT)

    def __init__(self, col_char=None, col_nchar=None, col_varchar_b=None, col_varchar_c=None,
                 col_nvarchar=None, col_text=None):
        self.COL_CHAR = col_char
        self.COL_NCHAR = col_nchar
        self.COL_VARCHAR_B = col_varchar_b
        self.COL_VARCHAR_C = col_varchar_c
        self.COL_NVARCHAR = col_nvarchar
        self.COL_TEXT = col_text

    def __repr__(self):
        return "<StringTest> {}, {}, {}, {}, {}, {}, {}".format(self.T_ID, self.COL_CHAR, self.COL_NCHAR,
                                                                self.COL_VARCHAR_B, self.COL_VARUCHAR_C,
                                                                self.COL_NVARCHAR, self.COL_TEXT)


class NumericTest(PostgresqlMapperBase):
    """
    테이블 NUMERIC_TEST의 Mapper Class
    """

    __tablename__ = NUMERIC_TEST.lower()
    T_ID = Column("t_id", INTEGER, nullable=False, primary_key=True)
    COL_BIT = Column("col_bit", SMALLINT)
    COL_TINYINT = Column("col_tinyint", SMALLINT)
    COL_SMALLINT = Column("col_smallint", SMALLINT)
    COL_MEDIUMINT = Column("col_mediumint", INTEGER)
    COL_INT = Column("col_int", INTEGER)
    COL_BIGINT = Column("col_bigint", BIGINT)
    COL_DECIMAL = Column("col_decimal", DECIMAL(38, 20))
    COL_NUMERIC = Column("col_numeric", NUMERIC(38, 18))
    COL_FLOAT = Column("col_float", REAL)
    COL_DOUBLE = Column("col_double", DOUBLE_PRECISION)
    COL_SMALLMONEY = Column("col_smallmoney", MONEY)
    COL_MONEY = Column("col_money", MONEY)

    def __init__(self, col_bit=None, col_tinyint=None, col_smallint=None, col_mediumint=None,
                 col_int=None, col_bigint=None, col_decimal=None, col_numeric=None, col_real=None,
                 col_float=None, col_smallmoney=None, col_money=None):
        self.COL_BIT = col_bit
        self.COL_TINYINT = col_tinyint
        self.COL_SMALLINT = col_smallint
        self.COL_MEDIUMINT = col_mediumint
        self.COL_INT = col_int
        self.COL_BIGINT = col_bigint
        self.COL_DECIMAL = col_decimal
        self.COL_NUMERIC = col_numeric
        self.COL_FLOAT = col_real
        self.COL_DOUBLE = col_float
        self.COL_SMALLMONEY = col_smallmoney
        self.COL_MONEY = col_money

    def __repr__(self):
        return "<NumericTest> {}, {}, {}".format(self.T_ID, self.COL_BIT, self.COL_TINYINT, self.COL_SMALLINT,
                                                 self.COL_MEDIUMINT, self.COL_INT, self.COL_BIGINT, self.COL_DECIMAL,
                                                 self.COL_NUMERIC, self.COL_FLOAT, self.COL_DOUBLE, self.COL_SMALLMONEY,
                                                 self.COL_MONEY)


class DateTimeTest(PostgresqlMapperBase):
    """
    테이블 DATETIME_TEST의 Mapper Class
    """

    __tablename__ = DATETIME_TEST.lower()
    T_ID = Column("t_id", INTEGER, nullable=False, primary_key=True)
    COL_DATETIME = Column("col_datetime", TIMESTAMP)
    COL_TIMESTAMP = Column("col_timestamp", TIMESTAMP)
    COL_TIMESTAMP2 = Column("col_timestamp2", TIMESTAMP)
    COL_INTER_YEAR_MONTH = Column("col_inter_year_month", INTERVAL)
    COL_INTER_DAY_SEC = Column("col_inter_day_sec", INTERVAL)

    def __init__(self, col_date=None, col_timestamp=None, col_timestamp2=None,
                 col_inter_year_month=None, col_inter_day_sec=None):
        self.COL_DATE = col_date
        self.COL_TIMESTAMP = col_timestamp
        self.COL_TIMESTAMP2 = col_timestamp2
        self.COL_INTER_YEAR_MONTH = col_inter_year_month
        self.COL_INTER_DAY_SEC = col_inter_day_sec

    def __repr__(self):
        return "<DateTimeTest> {}, {}, {}, {}, {}, {}".format(self.T_ID, self.COL_DATE, self.COL_TIMESTAMP,
                                                              self.COL_TIMESTAMP2, self.COL_INTER_YEAR_MONTH,
                                                              self.COL_INTER_DAY_SEC)


class BinaryTest(PostgresqlMapperBase):
    """
    테이블 BINARY_TEST의 Mapper Class
    """

    __tablename__ = BINARY_TEST.lower()
    T_ID = Column("t_id", INTEGER, nullable=False, primary_key=True)
    COL_BINARY = Column("col_binary", BYTEA)
    COL_VARBINARY = Column("col_varbinary", BYTEA)
    COL_LONG_BINARY = Column("col_long_binary", BYTEA)

    def __init__(self, col_binary=None, col_varbinary=None, col_long_binary=None):
        self.COL_BINARY = col_binary
        self.COL_VARBINARY = col_varbinary
        self.COL_LONG_BINARY = col_long_binary

    def __repr__(self):
        return "<BinaryTest> {}, {}, {}, {}".format(self.T_ID, self.COL_BINARY, self.COL_VARBINARY, self.COL_LONG_BINARY)


class LOBTest(PostgresqlMapperBase):
    """
    테이블 LOB_TEST의 Mapper Class
    """

    __tablename__ = LOB_TEST.lower()
    T_ID = Column("t_id", INTEGER, nullable=False, primary_key=True)
    COL_CLOB_ALIAS = Column("col_clob_alias", VARCHAR(50))
    COL_CLOB_DATA = Column("col_clob_data", TEXT)
    COL_NCLOB_ALIAS = Column("col_nclob_alias", VARCHAR(50))
    COL_NCLOB_DATA = Column("col_nclob_data", TEXT)
    COL_BLOB_ALIAS = Column("col_blob_alias", VARCHAR(50))
    COL_BLOB_DATA = Column("col_blob_data", BYTEA)

    def __init__(self, col_clob_alias=None, col_clob_data=None, col_nclob_alias=None, col_nclob_data=None,
                 col_blob_alias=None, col_blob_data=None):
        self.COL_CLOB_ALIAS = col_clob_alias
        self.COL_CLOB_DATA = col_clob_data
        self.COL_NCLOB_ALIAS = col_nclob_alias
        self.COL_NCLOB_DATA = col_nclob_data
        self.COL_BLOB_ALIAS = col_blob_alias
        self.COL_BLOB_DATA = col_blob_data

    def __repr__(self):
        return "<LOBTest> {}, {}, {}, {}, {}, {}, {}".format(self.T_ID, self.COL_CLOB_ALIAS, self.COL_CLOB_DATA,
                                                             self.COL_NCLOB_ALIAS, self.COL_NCLOB_DATA,
                                                             self.COL_BLOB_ALIAS, self.COL_BLOB_DATA)
