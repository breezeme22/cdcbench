from sqlalchemy import Column, Sequence
from sqlalchemy.types import NCHAR
from sqlalchemy.dialects.oracle import CHAR, VARCHAR2, NVARCHAR2, RAW, NUMBER, BINARY_FLOAT, BINARY_DOUBLE,\
                                       LONG, DATE, TIMESTAMP, INTERVAL, BFILE, BLOB, CLOB, NCLOB, ROWID

from mappers.oracle_custom_types import VARCHAR2Byte, LONGRAW, UROWID
from commons.mgr_connection import MapperBase


# Tab. insert_test
class InsertTest(MapperBase):
    """
    테스트 테이블인 insert_test의 Mapper Class
    """

    __tablename__ = "insert_test"
    product_id = Column(NUMBER, Sequence("insert_test_seq", 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2Byte(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<InsertTest> {}, {}, {}, {}".format(self.product_id, self.product_name, self.product_date,
                                                    self.separate_col)


# Tab. update_test
class UpdateTest(MapperBase):
    """
    테스트 테이블 update_test의 Mapper Class
    """

    __tablename__ = "update_test"
    product_id = Column(NUMBER, Sequence("update_test_seq", 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2Byte(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<UpdateTest> {}, {}, {}, {}".format(self.product_id, self.product_name, self.product_date,
                                                    self.separate_col)


# Tab. delete_test
class DeleteTest(MapperBase):
    """
    테스트 테이블 delete_test의 Mapper Class
    """

    __tablename__ = "delete_test"
    product_id = Column(NUMBER, Sequence("delete_test_seq", 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2Byte(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<DeleteTest> {}, {}, {}, {}".format(self.product_id, self.product_name, self.product_date,
                                                    self.separate_col)


class StringTest(MapperBase):
    """
    테스트 테이블 string_test의 Mapper Class
    """

    __tablename__ = "string_test"
    t_id = Column(NUMBER, Sequence("string_test_seq", 1001), nullable=False, primary_key=True)
    col_char = Column(CHAR(50))
    col_nchar = Column(NCHAR(50))
    col_varchar2_byte = Column(VARCHAR2Byte(4000))
    col_varchar2_char = Column(VARCHAR2(4000))
    col_nvarchar2 = Column(NVARCHAR2(2000))

    def __init__(self, col_char=None, col_nchar=None,
                 col_varchar2_byte=None, col_varchar2_char=None, col_nvarchar2=None):
        self.col_char = col_char
        self.col_nchar = col_nchar
        self.col_varchar2_byte = col_varchar2_byte
        self.col_varchar2_char = col_varchar2_char
        self.col_nvarchar2 = col_nvarchar2

    def __repr__(self):
        return "<StringTest> {}, {}, {}, {}, {}, {}".format(self.t_id, self.col_char, self.col_nchar,
                                                            self.col_varchar2_byte, self.col_varchar2_char,
                                                            self.col_nvarchar2)


class NumericTest(MapperBase):
    """
    테스트 테이블 numeric_test의 Mapper Class
    """

    __tablename__ = "numeric_test"
    t_id = Column(NUMBER, Sequence("numeric_test_seq", 1001), nullable=False, primary_key=True)
    col_number = Column(NUMBER)
    col_binary_float = Column(BINARY_FLOAT)
    # col_binary_double = Column(BINARY_DOUBLE)

    def __init__(self, col_number=None, col_binary_float=None):
        self.col_number = col_number
        self.col_binary_float = col_binary_float
        # self.col_binary_double = col_binary_double

    def __repr__(self):
        return "<NumericTest> {}, {}, {}".format(self.t_id, self.col_number, self.col_binary_float)


class DateTest(MapperBase):
    """
    테스트 테이블 date_test의 Mapper Class
    """

    __tablename__ = "date_test"
    t_id = Column(NUMBER, Sequence("date_test_seq", 1001), nullable=False, primary_key=True)
    col_date = Column(DATE)
    col_timestamp = Column(TIMESTAMP)
    col_timezone = Column(TIMESTAMP(True))
    col_inter_day_sec = Column(INTERVAL)

    def __init__(self, col_date=None, col_timestamp=None, col_timezone=None, col_inter_day_sec=None):
        self.col_date = col_date
        self.col_timestamp = col_timestamp
        self.col_timezone = col_timezone
        self.col_inter_day_sec = col_inter_day_sec

    def __repr__(self):
        return "<DateTest> {}, {}, {}, {}, {}, {}".format(self.t_id, self.col_date, self.col_timestamp,
                                                          self.col_timezone, self.col_timestamp,
                                                          self.col_inter_day_sec)


class BinaryTest(MapperBase):
    """
    테스트 테이블 binary_test의 Mapper Class
    """

    __tablename__ = "binary_test"
    t_id = Column(NUMBER, Sequence("binary_test_seq", 1001), nullable=False, primary_key=True)
    col_raw = Column(RAW(2000))
    col_long_raw = Column(LONGRAW)
    col_rowid = Column(ROWID)
    col_urowid = Column(UROWID)

    def __init__(self, col_raw=None, col_long_raw=None, col_rowid=None, col_urowid=None):
        self.col_raw = col_raw
        self.col_long_raw = col_long_raw
        self.col_rowid = col_rowid
        self.col_urowid = col_urowid

    def __repr__(self):
        return "<BinaryTest> {}, {}, {}, {}".format(self.t_id, self.col_raw, self.col_long_raw,
                                                    self.col_rowid, self.col_urowid)


class LOBTest(MapperBase):
    """
    테스트 테이블 lob_test의 Mapper Class
    """

    __tablename__ = "lob_test"
    t_id = Column(NUMBER, Sequence("lob_test_seq", 1001), nullable=False, primary_key=True)
    col_long_alias = Column(VARCHAR2Byte(50))
    col_long_data = Column(LONG)
    col_clob_alias = Column(VARCHAR2Byte(50))
    col_clob_data = Column(CLOB)
    col_nclob_alias = Column(VARCHAR2Byte(50))
    col_nclob_data = Column(NCLOB)
    col_blob_alias = Column(VARCHAR2Byte(50))
    col_blob_data = Column(BLOB)

    def __init__(self, col_long_alias=None, col_long_data=None, col_clob_alias=None, col_clob_data=None,
                 col_nclob_alias=None, col_nclob_data=None, col_blob_alias=None, col_blob_data=None):
        self.col_long_alias = col_long_alias
        self.col_long_data = col_long_data
        self.col_clob_alias = col_clob_alias
        self.col_clob_data = col_clob_data
        self.col_nclob_alias = col_nclob_alias
        self.col_nclob_data = col_nclob_data
        self.col_blob_alias = col_blob_alias
        self.col_blob_data = col_blob_data

    def __repr__(self):
        return "<LOBTest> {}, {}, {}, {}, {}, {}, {}, {}, {}".format(self.t_id, self.long_alias, self.long_data,
                                                                     self.clob_alias, self.clob_data, self.nclob_alias,
                                                                     self.nclob_data, self.blob_alias, self.blob_data)
