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
    product_id = Column(INTEGER, nullable=False, primary_key=True)
    product_name = Column(VARCHAR(50))
    product_date = Column(TIMESTAMP)
    separate_col = Column(INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<InsertTest> {}, {}, {}, {}".format(self.product_id, self.product_name, self.product_date,
                                                    self.separate_col)


class UpdateTest(PostgresqlMapperBase):
    """
    테이블 UPDATE_TEST의 Mapper Class
    """

    __tablename__ = UPDATE_TEST.lower()
    product_id = Column(INTEGER, nullable=False, primary_key=True)
    product_name = Column(VARCHAR(50))
    product_date = Column(TIMESTAMP)
    separate_col = Column(INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<UpdateTest> {}, {}, {}, {}".format(self.product_id, self.product_name, self.product_date,
                                                    self.separate_col)


class DeleteTest(PostgresqlMapperBase):
    """
    테이블 DELETE_TEST의 Mapper Class
    """

    __tablename__ = DELETE_TEST.lower()
    product_id = Column(INTEGER, nullable=False, primary_key=True)
    product_name = Column(VARCHAR(50))
    product_date = Column(TIMESTAMP)
    separate_col = Column(INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<DeleteTest> {}, {}, {}, {}".format(self.product_id, self.product_name, self.product_date,
                                                    self.separate_col)


class StringTest(PostgresqlMapperBase):
    """
    테이블 STRING_TEST의 Mapper Class
    """

    __tablename__ = STRING_TEST.lower()
    t_id = Column(INTEGER, nullable=False, primary_key=True)
    col_char = Column(CHAR(50))
    col_nchar = Column(CHAR(50))
    col_varchar_b = Column(VARCHAR(4000))
    col_varchar_c = Column(VARCHAR(4000))
    col_nvarchar = Column(VARCHAR(2000))
    col_text = Column(TEXT)

    def __init__(self, col_char=None, col_nchar=None, col_varchar_b=None, col_varchar_c=None,
                 col_nvarchar=None, col_text=None):
        self.col_char = col_char
        self.col_nchar = col_nchar
        self.col_varchar_b = col_varchar_b
        self.col_varchar_c = col_varchar_c
        self.col_nvarchar = col_nvarchar
        self.col_text = col_text

    def __repr__(self):
        return "<StringTest> {}, {}, {}, {}, {}, {}, {}".format(self.t_id, self.col_char, self.col_nchar,
                                                                self.col_varchar_b, self.col_varchar_c,
                                                                self.col_nvarchar, self.col_text)


class NumericTest(PostgresqlMapperBase):
    """
    테이블 NUMERIC_TEST의 Mapper Class
    """

    __tablename__ = NUMERIC_TEST.lower()
    t_id = Column(INTEGER, nullable=False, primary_key=True)
    col_bit = Column(SMALLINT)
    col_tinyint = Column(SMALLINT)
    col_smallint = Column(SMALLINT)
    col_mediumint = Column(INTEGER)
    col_int = Column(INTEGER)
    col_bigint = Column(BIGINT)
    col_decimal = Column(DECIMAL(38, 20))
    col_numeric = Column(NUMERIC(38, 18))
    col_float = Column(REAL)
    col_double = Column(DOUBLE_PRECISION)
    col_smallmoney = Column(MONEY)
    col_money = Column(MONEY)

    def __init__(self, col_bit=None, col_tinyint=None, col_smallint=None, col_mediumint=None,
                 col_int=None, col_bigint=None, col_decimal=None, col_numeric=None, col_real=None,
                 col_float=None, col_smallmoney=None, col_money=None):
        self.col_bit = col_bit
        self.col_tinyint = col_tinyint
        self.col_smallint = col_smallint
        self.col_mediumint = col_mediumint
        self.col_int = col_int
        self.col_bigint = col_bigint
        self.col_decimal = col_decimal
        self.col_numeric = col_numeric
        self.col_float = col_real
        self.col_double = col_float
        self.col_smallmoney = col_smallmoney
        self.col_money = col_money

    def __repr__(self):
        return "<NumericTest> {}, {}, {}".format(self.t_id, self.col_bit, self.col_tinyint, self.col_smallint,
                                                 self.col_mediumint, self.col_int, self.col_bigint, self.col_decimal,
                                                 self.col_numeric, self.col_float, self.col_double, self.col_smallmoney,
                                                 self.col_money)


class DateTimeTest(PostgresqlMapperBase):
    """
    테이블 DATETIME_TEST의 Mapper Class
    """

    __tablename__ = DATETIME_TEST.lower()
    t_id = Column(INTEGER, nullable=False, primary_key=True)
    col_datetime = Column(TIMESTAMP)
    col_timestamp = Column(TIMESTAMP)
    col_timestamp2 = Column(TIMESTAMP)
    col_inter_year_month = Column(INTERVAL)
    col_inter_day_sec = Column(INTERVAL)

    def __init__(self, col_date=None, col_timestamp=None, col_timestamp2=None,
                 col_inter_year_month=None, col_inter_day_sec=None):
        self.col_date = col_date
        self.col_timestamp = col_timestamp
        self.col_timestamp2 = col_timestamp2
        self.col_inter_year_month = col_inter_year_month
        self.col_inter_day_sec = col_inter_day_sec

    def __repr__(self):
        return "<DateTimeTest> {}, {}, {}, {}, {}, {}".format(self.t_id, self.col_date, self.col_timestamp,
                                                              self.col_timestamp2, self.col_inter_year_month,
                                                              self.col_inter_day_sec)


class BinaryTest(PostgresqlMapperBase):
    """
    테이블 BINARY_TEST의 Mapper Class
    """

    __tablename__ = BINARY_TEST.lower()
    t_id = Column(INTEGER, nullable=False, primary_key=True)
    col_binary = Column(BYTEA)
    col_varbinary = Column(BYTEA)
    col_long_binary = Column(BYTEA)

    def __init__(self, col_binary=None, col_varbinary=None, col_long_binary=None):
        self.col_binary = col_binary
        self.col_varbinary = col_varbinary
        self.col_long_binary = col_long_binary

    def __repr__(self):
        return "<BinaryTest> {}, {}, {}, {}".format(self.t_id, self.col_binary, self.col_varbinary, self.col_long_binary)


class LOBTest(PostgresqlMapperBase):
    """
    테이블 LOB_TEST의 Mapper Class
    """

    __tablename__ = LOB_TEST.lower()
    t_id = Column(INTEGER, nullable=False, primary_key=True)
    col_clob_alias = Column(VARCHAR(50))
    col_clob_data = Column(TEXT)
    col_nclob_alias = Column(VARCHAR(50))
    col_nclob_data = Column(BYTEA)
    col_blob_alias = Column(VARCHAR(50))
    col_blob_data = Column(BYTEA)

    def __init__(self, col_clob_alias=None, col_clob_data=None, col_nclob_alias=None, col_nclob_data=None,
                 col_blob_alias=None, col_blob_data=None):
        self.col_clob_alias = col_clob_alias
        self.col_clob_data = col_clob_data
        self.col_nclob_alias = col_nclob_alias
        self.col_nclob_data = col_nclob_data
        self.col_blob_alias = col_blob_alias
        self.col_blob_data = col_blob_data

    def __repr__(self):
        return "<LOBTest> {}, {}, {}, {}, {}, {}, {}".format(self.t_id, self.col_clob_alias, self.col_clob_data,
                                                             self.col_nclob_alias, self.col_nclob_data,
                                                             self.col_blob_alias, self.col_blob_data)
