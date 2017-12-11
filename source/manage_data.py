from source.connection import db_session, engine
from source.models import *
from source.table_data import *

from datetime import datetime

import random, time


# update_test & delete_test table data initialize
def data_init(table, data_row=300000, commit_unit=20000, start_val=1):

    data_len = len(bench_data)
    data_list = []

    for i in range(1, data_row+1):
        t = bench_data[random.randrange(0, data_len)]
        product_date = datetime.strptime(t[2], '%Y-%m-%d-%H-%M-%S')

        if table == UpdateTest:
            product_name = '1'
        else:
            product_name = t[1]

        data_list.append({"product_name": product_name, "product_date": product_date, "separate_col": start_val})

        if i % commit_unit == 0:
            engine.execute(table.__table__.insert(), data_list)
            start_val += 1
            data_list.clear()

    if data_row % commit_unit != 0:
        engine.execute(table.__table__.insert(), data_list)


# ORM insert
def insert_test(insert_row, commit_unit=1000, start_val=1):

    data_len = len(bench_data)

    for i in range(1, insert_row+1):
        t = bench_data[random.randrange(0, data_len)]
        new_data = InsertTest(t[1], t[2], start_val)
        db_session.add(new_data)

        if i % commit_unit == 0:
            db_session.commit()
            start_val += 1

    db_session.commit()


# bulk_insert
def insert_test_bulk(insert_row, commit_unit=1000, start_val=1):

    data_len = len(bench_data)
    insert_data = []
    commit_count = 0

    for chunk in range(0, insert_row, commit_unit):

        for i in range(chunk, min(chunk + commit_unit, insert_row + 1)):
            t = bench_data[random.randrange(0, data_len)]
            product_date = datetime.strptime(t[2], '%Y-%m-%d-%H-%M-%S')
            insert_data.append({"product_name": t[1], "product_date": product_date, "separate_col": start_val})

        db_session.bulk_insert_mappings(InsertTest, insert_data)
        start_val += 1
        insert_data.clear()

        if chunk % 20000 == 0:
            db_session.commit()
            commit_count += 1

    db_session.commit()
    commit_count += 1


# insert core
def insert_test_core(insert_row, commit_unit=1000, start_val=1):

    data_len = len(bench_data)
    insert_data = []

    for i in range(1, insert_row+1):
        t = bench_data[random.randrange(0, data_len)]
        product_date = datetime.strptime(t[2], '%Y-%m-%d-%H-%M-%S')
        insert_data.append({"product_name": t[1], "product_date": product_date, "separate_col": start_val})

        if i % commit_unit == 0:
            engine.execute(InsertTest.__table__.insert(), insert_data)
            start_val += 1
            insert_data.clear()

    if insert_row % commit_unit != 0:
        engine.execute(InsertTest.__table__.insert(), insert_data)


# ORM update
def update_test(start_separate_col=1, end_separate_col=15):

    data_len = len(bench_data)

    for i in range(start_separate_col, end_separate_col+1):
        t = bench_data[random.randrange(0, data_len)]
        db_session.query(UpdateTest).filter(UpdateTest.separate_col == i).update({UpdateTest.product_name: t[1]})

        db_session.commit()

    db_session.commit()


# update core
def update_test_core(start_separate_col=1, end_separate_col=15):

    data_len = len(bench_data)

    for i in range(start_separate_col, end_separate_col+1):
        t = bench_data[random.randrange(0, data_len)]

        engine.execute(UpdateTest.__table__.update()\
                                           .where(UpdateTest.separate_col == i)\
                                           .values(product_name=t[1]))


# delete
def delete_test(start_separate_col=1, end_separate_col=15):

    for i in range(start_separate_col, end_separate_col+1):
        db_session.query(DeleteTest).filter(DeleteTest.separate_col == i).delete()
        db_session.commit()


# delete core
def delete_test_core(start_separate_col=1, end_separate_col=15):

    for i in range(start_separate_col, end_separate_col+1):
        engine.execute(DeleteTest.__table__.delete().where(DeleteTest.separate_col == i))