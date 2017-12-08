import random

from source.connection import db_session, engine
from source.models import *

from source.table_data import *


# 단위 insert
def insert_test(insert_row, sep_unit=1000, start_val=1):

    data_len = len(bench_data)

    for i in range(1, insert_row+1):
        t = bench_data[random.randrange(0, data_len)]
        new_data = InsertTest(t[1], t[2], start_val)
        db_session.add(new_data)

        if i % sep_unit == 0:
            db_session.commit()
            start_val += 1

    db_session.commit()


# bulk_save_objects
def insert_test_bulk_save_objects(insert_row, sep_unit=1000, start_val=1):

    data_len = len(bench_data)

    for chunk in range(1, insert_row+1, sep_unit):
        t = bench_data[random.randrange(0, data_len)]
        db_session.bulk_save_objects(
            [
                InsertTest(t[1], t[2], start_val)
                for i in range(chunk, min(chunk + sep_unit, insert_row + 1))
            ]

        )
        start_val += 1

    db_session.commit()


# bulk_insert
def insert_test_bulk_insert(insert_row, sep_unit=1000, start_val=1):

    data_len = len(bench_data)

    for chunk in range(1, insert_row+1, sep_unit):
        t = bench_data2[random.randrange(0, data_len)]

        db_session.bulk_insert_mappings(
            InsertTest,
            [
                dict(product_name=t[1], product_date=func.to_date(t[2], 'yyyy-mm-dd-hh24-mi-ss'), separate_col=start_val)
                for i in range(chunk, min(chunk + sep_unit, insert_row + 1))
            ], render_nulls=True
        )
        start_val += 1

    db_session.commit()


# core
def insert_test_core(insert_row, sep_unit=1000, start_val=1):

    data_len = len(bench_data)
    insert_data = []

    # for i in range(1, insert_row+1):
    #     t = bench_data2[random.randrange(0, data_len)]
    #     insert_data.append({"product_name": t[1], "product_date": func.to_date(t[2]), "separate_col": start_val})
    #
    #     if i % sep_unit == 0:
    #         start_val += 1

    t = bench_data[random.randrange(0, data_len)]

    # engine.execute(
    #     InsertTest.__table__.insert(),
    #     [{"product_name": t[1], "product_date": t[2], "separate_col": start_val} for i in range(insert_row + 1)]
    # )

    engine.execute(
        InsertTest.__table__.insert(),
        [{"product_name": t[1], "product_date": t[2], "separate_col": start_val} for i in range(insert_row + 1)]
    )


# 단위 update
def update_test(start_separate_col=1, end_separate_col=200):

    data_len = len(bench_data)

    for i in range(start_separate_col, end_separate_col+1):
        t = bench_data[random.randrange(0, data_len)]
        db_session.query(InsertTest).filter(InsertTest.separate_col == i).update({InsertTest.product_name: t[1]})

        db_session.commit()