from sqlalchemy import Column
from sqlalchemy.dialects.mysql import \
        CHAR, NCHAR, VARCHAR, NVARCHAR, INTEGER, BIGINT, FLOAT, DATETIME, \
        TINYBLOB, LONGBLOB, TEXT, LONGTEXT

from commons.mgr_connection import MysqlMapperBase


# Tab. insert_test
class InsertTest(MysqlMapperBase):
    """
    테스트 테이블 INSERT_TEST의 Mapper Class
    """

    __tablename__ = "INSERT_TEST"
    PRODUCT_ID = Column(INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR(30))
    PRODUCT_DATE = Column(DATETIME)
    SEPARATE_COL = Column(INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<InsertTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


# Tab. update_test
class UpdateTest(MysqlMapperBase):
    """
    테스트 테이블 UPDATE_TEST의 Mapper Class
    """

    __tablename__ = "UPDATE_TEST"
    PRODUCT_ID = Column(INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR(30))
    PRODUCT_DATE = Column(DATETIME)
    SEPARATE_COL = Column(INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<UpdateTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


# Tab. delete_test
class DeleteTest(MysqlMapperBase):
    """
    테스트 테이블 DELETE_TEST의 Mapper Class
    """

    __tablename__ = "DELETE_TEST"
    PRODUCT_ID = Column(INTEGER, nullable=False, primary_key=True)
    PRODUCT_NAME = Column(VARCHAR(30))
    PRODUCT_DATE = Column(DATETIME)
    SEPARATE_COL = Column(INTEGER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.PRODUCT_NAME = product_name
        self.PRODUCT_DATE = product_date
        self.SEPARATE_COL = separate_col

    def __repr__(self):
        return "<DeleteTest> {}, {}, {}, {}".format(self.PRODUCT_ID, self.PRODUCT_NAME, self.PRODUCT_DATE,
                                                    self.SEPARATE_COL)


class StringTest(MysqlMapperBase):
    """
    테스트 테이블 STRING_TEST의 Mapper Class
    """

    __tablename__ = "STRING_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_CHAR = Column(CHAR(50))
    COL_NCHAR = Column(NCHAR(50))
    COL_VARCHAR2_BYTE = Column(VARCHAR(4000))
    COL_VARCHAR2_CHAR = Column(VARCHAR(4000))
    COL_NVARCHAR2 = Column(NVARCHAR(2000))

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


class NumericTest(MysqlMapperBase):
    """
    테스트 테이블 NUMERIC_TEST의 Mapper Class
    """

    __tablename__ = "NUMERIC_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_NUMBER = Column(BIGINT)
    COL_BINARY_FLOAT = Column(FLOAT)
    # col_binary_double = Column(BINARY_DOUBLE)

    def __init__(self, col_number=None, col_binary_float=None):
        self.COL_NUMBER = col_number
        self.COL_BINARY_FLOAT = col_binary_float
        # self.col_binary_double = col_binary_double

    def __repr__(self):
        return "<NumericTest> {}, {}, {}".format(self.T_ID, self.COL_NUMBER, self.COL_BINARY_FLOAT)


class DateTimeTest(MysqlMapperBase):
    """
    테스트 테이블 DATETIME_TEST의 Mapper Class
    """

    __tablename__ = "DATETIME_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_DATE = Column(DATETIME)
    COL_TIMESTAMP = Column(DATETIME)
    COL_INTER_YEAR_MONTH = Column(VARCHAR(255))
    COL_INTER_DAY_SEC = Column(VARCHAR(255))

    def __init__(self, col_date=None, col_timestamp=None, col_inter_year_month=None, col_inter_day_sec=None):
        self.COL_DATE = col_date
        self.COL_TIMESTAMP = col_timestamp
        self.COL_INTER_YEAR_MONTH = col_inter_year_month
        self.COL_INTER_DAY_SEC = col_inter_day_sec

    def __repr__(self):
        return "<DateTest> {}, {}, {}, {}, {}".format(self.T_ID, self.COL_DATE, self.COL_TIMESTAMP,
                                                      self.COL_INTER_YEAR_MONTH, self.COL_INTER_DAY_SEC)


class BinaryTest(MysqlMapperBase):
    """
    테스트 테이블 BINARY_TEST의 Mapper Class
    """

    __tablename__ = "BINARY_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_ROWID = Column(VARCHAR(64))
    COL_UROWID = Column(VARCHAR(64))
    COL_RAW = Column(TINYBLOB)
    COL_LONG_RAW = Column(LONGBLOB)

    def __init__(self, col_rowid=None, col_urowid=None, col_raw=None, col_long_raw=None):
        self.COL_ROWID = col_rowid
        self.COL_UROWID = col_urowid
        self.COL_RAW = col_raw
        self.COL_LONG_RAW = col_long_raw

    def __repr__(self):
        return "<BinaryTest> {}, {}, {}, {}, {}".format(self.T_ID, self.COL_ROWID, self.COL_UROWID,
                                                        self.COL_RAW, self.COL_LONG_RAW)


class LOBTest(MysqlMapperBase):
    """
    테스트 테이블 LOB_TEST의 Mapper Class
    """

    __tablename__ = "LOB_TEST"
    T_ID = Column(INTEGER, nullable=False, primary_key=True)
    COL_LONG_ALIAS = Column(VARCHAR(50))
    COL_LONG_DATA = Column(TEXT)
    COL_CLOB_ALIAS = Column(VARCHAR(50))
    COL_CLOB_DATA = Column(LONGTEXT)
    COL_NCLOB_ALIAS = Column(VARCHAR(50))
    COL_NCLOB_DATA = Column(LONGTEXT)
    COL_BLOB_ALIAS = Column(VARCHAR(50))
    COL_BLOB_DATA = Column(LONGBLOB)

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
