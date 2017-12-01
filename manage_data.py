from connection import db_session
from models import *
from table_data import *

import random


# 단위 insert
def insert_demo_test(insert_row, sep_unit=1000, start_val=1):

    data_len = len(insert_demo_data)

    for i in range(1, insert_row+1):
        t = insert_demo_data[random.randrange(0, data_len)]
        new_data = InsertDemoTest(t[1], t[2], start_val)
        db_session.add(new_data)

        if i % sep_unit == 0:
            db_session.commit()
            start_val += 1

    db_session.commit()


# 단위 update
def update_demo_test(start_separate_col=1, end_separate_col=200):

    data_len = len(insert_demo_data)

    for i in range(start_separate_col, end_separate_col+1):
        t = insert_demo_data[random.randrange(0, data_len)]
        db_session.query(InsertDemoTest).filter(InsertDemoTest.separate_col == i).update({InsertDemoTest.product_name: t[1]})

        db_session.commit()