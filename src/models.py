from sqlalchemy import Column, Sequence
from sqlalchemy.dialects.oracle import DATE, NUMBER, VARCHAR2

from src.installer import Base


# Tab. insert_test
class InsertTest(Base):
    """
    테스트 테이블인 insert_test의 Mapper Class
    """

    __tablename__ = 'insert_test'
    product_id = Column(NUMBER, Sequence('insert_test_seq', 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<InsertTest> " + str(self.product_id) + ", " + self.product_name + ", " + \
               str(self.product_date) + ", " + str(self.separate_col)


# Tab. update_test
class UpdateTest(Base):
    """
    테스트 테이블 update_test의 Mapper Class
    """

    __tablename__ = 'update_test'
    product_id = Column(NUMBER, Sequence('update_test_seq', 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<UpdateTest> " + str(self.product_id) + ", " + self.product_name + ", " + \
               str(self.product_date) + ", " + str(self.separate_col)


# Tab. delete_test
class DeleteTest(Base):
    """
    테스트 테이블 delete_test의 Mapper Class
    """

    __tablename__ = 'delete_test'
    product_id = Column(NUMBER, Sequence('delete_test_seq', 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<DeleteTest> " + str(self.product_id) + ", " + self.product_name + ", " + \
               str(self.product_date) + ", " + str(self.separate_col)