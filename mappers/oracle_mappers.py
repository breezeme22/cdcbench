from sqlalchemy import Column, Sequence
from sqlalchemy.types import NCHAR
from sqlalchemy.dialects.oracle import \
        CHAR, VARCHAR2, NVARCHAR2, RAW, NUMBER, BINARY_FLOAT, BINARY_DOUBLE,\
        LONG, DATE, TIMESTAMP, INTERVAL, BFILE, BLOB, CLOB, NCLOB, ROWID

from mappers.oracle_custom_types import VARCHAR2Byte, LONGRAW, UROWID, INTERVALYearMonth
from commons.mgr_connection import OracleMapperBase


# Tab. insert_test
class InsertTest(OracleMapperBase):
    """
    테스트 테이블인 INSERT_TEST의 Mapper Class
    """

    __tablename__ = "INSERT_TEST"
    PRODUCT_ID = Column(NUMBER, Sequence("INSERT_TEST_SEQ"), nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR2Byte(30))
    PRODUCT_DATE = Column(DATE)
    SEPARATE_COL = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<InsertTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


# Tab. update_test
class UpdateTest(OracleMapperBase):
    """
    테스트 테이블 UPDATE_TEST의 Mapper Class
    """

    __tablename__ = "UPDATE_TEST"
    PRODUCT_ID = Column(NUMBER, Sequence("UPDATE_TEST_SEQ"), nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR2Byte(30))
    PRODUCT_DATE = Column(DATE)
    SEPARATE_COL = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<UpdateTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


# Tab. delete_test
class DeleteTest(OracleMapperBase):
    """
    테스트 테이블 DELETE_TEST의 Mapper Class
    """

    __tablename__ = "DELETE_TEST"
    PRODUCT_ID = Column(NUMBER, Sequence("DELETE_TEST_SEQ"), nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR2Byte(30))
    PRODUCT_DATE = Column(DATE)
    SEPARATE_COL = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<DeleteTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


class StringTest(OracleMapperBase):
    """
    테스트 테이블 STRING_TEST의 Mapper Class
    """

    __tablename__ = "STRING_TEST"
    T_ID = Column(NUMBER, Sequence("STRING_TEST_SEQ"), nullable=False, primary_key=True)
    COL_CHAR = Column(CHAR(50))
    COL_NCHAR = Column(NCHAR(50))
    COL_VARCHAR2_BYTE = Column(VARCHAR2Byte(4000))
    COL_VARCHAR2_CHAR = Column(VARCHAR2(4000))
    COL_NVARCHAR2 = Column(NVARCHAR2(2000))

    def __init__(self, col_char=None, col_nchar=None,
                 col_varchar2_byte=None, col_varchar2_char=None, col_nvarchar2=None):
        self.COL_CHAR = col_char
        self.COL_NCHAR = col_nchar
        self.COL_VARCHAR2_BYTE = col_varchar2_byte
        self.COL_VARCHAR2_CHAR = col_varchar2_char
        self.COL_NVARCHAR2 = col_nvarchar2

    def __repr__(self):
        return "<StringTest> {}, {}, {}, {}, {}, {}".format(self.T_ID, self.COL_CHAR, self.COL_NCHAR,
                                                            self.COL_VARCHAR2_BYTE, self.COL_VARCHAR2_CHAR,
                                                            self.COL_NVARCHAR2)


class NumericTest(OracleMapperBase):
    """
    테스트 테이블 NUMERIC_TEST의 Mapper Class
    """

    __tablename__ = "NUMERIC_TEST"
    T_ID = Column(NUMBER, Sequence("NUMERIC_TEST_SEQ"), nullable=False, primary_key=True)
    COL_NUMBER = Column(NUMBER)
    COL_BINARY_FLOAT = Column(BINARY_FLOAT)
    # col_binary_double = Column(BINARY_DOUBLE)

    def __init__(self, col_number=None,col_binary_float=None):
        self.COL_NUMBER = col_number
        self.COL_BINARY_FLOAT = col_binary_float
        # self.col_binary_double = col_binary_double

    def __repr__(self):
        return "<NumericTest> {}, {}, {}".format(self.T_ID, self.COL_NUMBER, self.COL_BINARY_FLOAT)


class DateTimeTest(OracleMapperBase):
    """
    테스트 테이블 datetime_test의 Mapper Class
    """

    __tablename__ = "DATETIME_TEST"
    T_ID = Column(NUMBER, Sequence("DATETIME_TEST_SEQ"), nullable=False, primary_key=True)
    COL_DATE = Column(DATE)
    COL_TIMESTAMP = Column(TIMESTAMP)
    COL_INTER_YEAR_MONTH = Column(INTERVALYearMonth)
    COL_INTER_DAY_SEC = Column(INTERVAL)

    def __init__(self, col_date=None, col_timestamp=None, col_inter_year_month=None, col_inter_day_sec=None):
        self.COL_DATE = col_date
        self.COL_TIMESTAMP = col_timestamp
        self.COL_INTER_YEAR_MONTH = col_inter_year_month
        self.COL_INTER_DAY_SEC = col_inter_day_sec

    def __repr__(self):
        return "<DateTest> {}, {}, {}, {}, {}".format(self.T_ID, self.COL_DATE, self.COL_TIMESTAMP,
                                                      self.COL_INTER_YEAR_MONTH, self.COL_INTER_DAY_SEC)


class BinaryTest(OracleMapperBase):
    """
    테스트 테이블 BINARY_TEST의 Mapper Class
    """

    __tablename__ = "BINARY_TEST"
    T_ID = Column(NUMBER, Sequence("BINARY_TEST_SEQ"), nullable=False, primary_key=True)
    COL_ROWID = Column(ROWID)
    COL_UROWID = Column(UROWID)
    COL_RAW = Column(RAW(2000))
    COL_LONG_RAW = Column(LONGRAW)

    def __init__(self, col_rowid=None, col_urowid=None, col_raw=None, col_long_raw=None):
        self.COL_ROWID = col_rowid
        self.COL_UROWID = col_urowid
        self.COL_RAW = col_raw
        self.COL_LONG_RAW = col_long_raw

    def __repr__(self):
        return "<BinaryTest> {}, {}, {}".format(self.T_ID, self.COL_ROWID, self.COL_UROWID)


class LOBTest(OracleMapperBase):
    """
    테스트 테이블 LOB_TEST의 Mapper Class
    """

    __tablename__ = "LOB_TEST"
    T_ID = Column(NUMBER, Sequence("LOB_TEST_SEQ"), nullable=False, primary_key=True)
    COL_LONG_ALIAS = Column(VARCHAR2Byte(50))
    COL_LONG_DATA = Column(LONG)
    COL_CLOB_ALIAS = Column(VARCHAR2Byte(50))
    COL_CLOB_DATA = Column(CLOB)
    COL_NCLOB_ALIAS = Column(VARCHAR2Byte(50))
    COL_NCLOB_DATA = Column(NCLOB)
    COL_BLOB_ALIAS = Column(VARCHAR2Byte(50))
    COL_BLOB_DATA = Column(BLOB)

    def __init__(self, col_long_alias=None, col_long_data=None, col_clob_alias=None, col_clob_data=None,
                 col_nclob_alias=None, col_nclob_data=None, col_blob_alias=None, col_blob_data=None):
        self.COL_LONG_ALIAS = col_long_alias
        self.COL_LONG_DATA = col_long_data
        self.COL_CLOB_ALIAS = col_clob_alias
        self.COL_CLOB_DATA = col_clob_data
        self.COL_NCLOB_ALIAS = col_nclob_alias
        self.COL_NCLOB_DATA = col_nclob_data
        self.COL_BLOB_ALIAS = col_blob_alias
        self.COL_BLOB_DATA = col_blob_data

    def __repr__(self):
        return "<LOBTest> {}, {}, {}, {}, {}, {}, {}, {}, {}".format(self.T_ID, self.COL_LONG_ALIAS, self.COL_LONG_DATA,
                                                                     self.COL_CLOB_ALIAS, self.COL_CLOB_DATA,
                                                                     self.COL_NCLOB_ALIAS, self.COL_NCLOB_DATA,
                                                                     self.COL_BLOB_ALIAS, self.COL_BLOB_DATA)
