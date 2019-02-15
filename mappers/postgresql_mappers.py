from sqlalchemy import Column, Sequence
from sqlalchemy.types import DECIMAL
from sqlalchemy.dialects.postgresql import \
        CHAR, VARCHAR, TEXT, \
        BIGINT, BOOLEAN, DOUBLE_PRECISION, INTEGER, MONEY, NUMERIC, REAL, SMALLINT, \
        INTERVAL, TIMESTAMP, \
        BYTEA

from commons.mgr_connection import PostgresqlMapperBase


class InsertTest(PostgresqlMapperBase):
    """
    테이블 INSERT_TEST의 Mapper Class
    """

    __tablename__ = "INSERT_TEST"
    PRODUCT_ID = Column(INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR(30))
    PRODUCT_DATE = Column(TIMESTAMP)
    SEPARATE_COL = Column(INTEGER)

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

    __tablename__ = "UPDATE_TEST"
    PRODUCT_ID = Column(INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR(30))
    PRODUCT_DATE = Column(TIMESTAMP)
    SEPARATE_COL = Column(INTEGER)

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

    __tablename__ = "DELETE_TEST"
    PRODUCT_ID = Column(INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR(30))
    PRODUCT_DATE = Column(TIMESTAMP)
    SEPARATE_COL = Column(INTEGER)

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

    __tablename__ = "STRING_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_CHAR = Column(CHAR(50))
    COL_NCHAR = Column(CHAR(50))
    COL_VARCHAR_B = Column(VARCHAR(4000))
    COL_VARCHAR_C = Column(VARCHAR(4000))
    COL_NVARCHAR = Column(VARCHAR(2000))
    COL_TEXT = Column(TEXT)

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
                                                                self.COL_VARCHAR_B, self.COL_VARCHAR_C,
                                                                self.COL_NVARCHAR, self.COL_TEXT)


class NumericTest(PostgresqlMapperBase):
    """
    테이블 NUMERIC_TEST의 Mapper Class
    """

    __tablename__ = "NUMERIC_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_BIT = Column(BOOLEAN)
    COL_TINYINT = Column(SMALLINT)
    COL_SMALLINT = Column(SMALLINT)
    COL_MEDIUMINT = Column(SMALLINT)
    COL_INT = Column(INTEGER)
    COL_BIGINT = Column(BIGINT)
    COL_NUMERIC = Column(NUMERIC)
    COL_DECIMAL = Column(DECIMAL)
    COL_FLOAT = Column(REAL)
    COL_DOUBLE = Column(DOUBLE_PRECISION)
    COL_SMALLMONEY = Column(MONEY)
    COL_MONEY = Column(MONEY)

    def __init__(self, col_bit=None, col_tinyint=None, col_smallint=None, col_mediumint=None,
                 col_int=None, col_bigint=None, col_numeric=None, col_decimal=None, col_real=None,
                 col_float=None, col_smallmoney=None, col_money=None):
        self.COL_BIT = col_bit
        self.COL_TINYINT = col_tinyint
        self.COL_SMALLINT = col_smallint
        self.COL_MEDIUMINT = col_mediumint
        self.COL_INT = col_int
        self.COL_BIGINT = col_bigint
        self.COL_NUMERIC = col_numeric
        self.COL_DECIMAL = col_decimal
        self.COL_FLOAT = col_real
        self.COL_DOUBLE = col_float
        self.COL_SMALLMONEY = col_smallmoney
        self.COL_MONEY = col_money

    def __repr__(self):
        return "<NumericTest> {}, {}, {}".format(self.T_ID, self.COL_BIT, self.COL_TINYINT, self.COL_SMALLINT,
                                                 self.COL_MEDIUMINT, self.COL_INT, self.COL_BIGINT, self.COL_NUMERIC,
                                                 self.COL_DECIMAL, self.COL_FLOAT, self.COL_DOUBLE, self.COL_SMALLMONEY,
                                                 self.COL_MONEY)


class DateTimeTest(PostgresqlMapperBase):
    """
    테이블 DATETIME_TEST의 Mapper Class
    """

    __tablename__ = "DATETIME_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_DATETIME = Column(TIMESTAMP)
    COL_TIMESTAMP = Column(TIMESTAMP)
    COL_TIMESTAMP2 = Column(TIMESTAMP)
    COL_INTER_YEAR_MONTH = Column(INTERVAL)
    COL_INTER_DAY_SEC = Column(INTERVAL)

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

    __tablename__ = "BINARY_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_BINARY = Column(BYTEA)
    COL_VARBINARY = Column(BYTEA)
    COL_LONG_BINARY = Column(BYTEA)

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

    __tablename__ = "LOB_TEST"
    T_ID = Column(INTEGER, Sequence("LOB_TEST_SEQ"), nullable=False, primary_key=True)
    COL_CLOB_ALIAS = Column(VARCHAR(50))
    COL_CLOB_DATA = Column(TEXT)
    COL_NCLOB_ALIAS = Column(VARCHAR(50))
    COL_NCLOB_DATA = Column(BYTEA)
    COL_BLOB_ALIAS = Column(VARCHAR(50))
    COL_BLOB_DATA = Column(BYTEA)

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
