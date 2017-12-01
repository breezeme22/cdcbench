from sqlalchemy import Column, Sequence
from sqlalchemy.dialects.oracle import \
    DATE, NUMBER, VARCHAR2

from source.installer import Base


# Tab. insert_demo
class BenchTest(Base):

    __tablename__ = 'insert_demo_test'
    # product_id = Column(NUMBER, nullable=False, primary_key=True)
    product_id = Column(NUMBER, Sequence('insert_demo_test_seq', 1001), nullable=False, primary_key=True)
    product_name = Column(VARCHAR2(30))
    product_date = Column(DATE)
    separate_col = Column(NUMBER)

    def __init__(self, product_name=None, product_date=None, separate_col=None):
        # self.product_id = product_id
        self.product_name = product_name
        self.product_date = product_date
        self.separate_col = separate_col

    def __repr__(self):
        return "<InsertDemoTest> " + str(self.product_id) + ", " + self.product_name + ", " + \
               str(self.product_date) + ", " + str(self.separate_col)