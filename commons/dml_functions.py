from commons.connection_manager import *

from mappers.oracle_mappings import *

from datetime import datetime

import random


# ORM insert
def insert_test(insert_row, commit_unit=1000, start_val=1):
    """
    ORM 방식으로 insert_test 테이블에 데이터를 insert. 나머지 insert 함수들 또한 동일한 사용법을 가짐.

    :param insert_row: 테이블에 insert할 데이터의 양을 지정
    :param commit_unit: commit 기준을 지정. 기본 값은 1000건당 commit 수행
    :param start_val: separate_col 컬럼의 시작값을 지정. separate_col의 값은 commit이 수행되면 1 증가됨.
    """

    data_len = len(bench_data)

    for i in range(1, insert_row+1):
        t = bench_data[random.randrange(0, data_len)]
        product_date = datetime.strptime(t[2], '%Y-%m-%d-%H-%M-%S')
        new_data = InsertTest(t[1], product_date, start_val)
        db_session.add(new_data)

        if i % commit_unit == 0:
            db_session.commit()
            start_val += 1

    db_session.commit()


# bulk_insert
def insert_test_bulk(insert_row, commit_unit=1000, start_val=1):
    """
    Bulk Insert 방식으로 insert_test 테이블에 데이터를 insert.
    """

    data_len = len(bench_data)
    insert_data = []

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

    db_session.commit()


# insert core - used
def insert_test_core(insert_row, commit_unit=1000, start_val=1):
    """
    SQLAlchemy Core 방식으로 insert_test 테이블에 데이터를 insert
    """

    data_len = len(bench_data)
    insert_data = []

    for i in range(1, insert_row+1):

        t = bench_data[random.randrange(0, data_len)]
        product_date = datetime.strptime(t[2], '%Y-%m-%d-%H-%M-%S')
        insert_data.append({"product_name": t[1], "product_date": product_date, "separate_col": start_val})

        if i % commit_unit == 0:
            # start_time = time.time()
            engine.execute(InsertTest.__table__.insert(), insert_data)
            start_val += 1
            insert_data.clear()
            # end_time = time.time()
            # run_time = end_time - start_time

            # if run_time < 0.0318:
            #      time.sleep(0.0318 - run_time)

    if insert_row % commit_unit != 0:
        engine.execute(InsertTest.__table__.insert(), insert_data)


# ORM update - used
def update_test(start_separate_col=1, end_separate_col=15):
    """
    ORM 방식으로 update_test 테이블의 product_name 컬럼의 값을 변경.

    :param start_separate_col: update시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                그러므로 update할 데이터의 시작 separate_col 값을 지정.
    :param end_separate_col: update할 데이터의 마지막 separate_col 값을 지정.
    """

    data_len = len(bench_data)

    for i in range(start_separate_col, end_separate_col+1):
        t = bench_data[random.randrange(0, data_len)]
        # db_session.query(UpdateTest).filter(UpdateTest.separate_col == i).update({UpdateTest.product_name: t[1]})
        db_session.query(UpdateTest).filter(UpdateTest.separate_col == i).update({UpdateTest.product_name: '2'})

        db_session.commit()
        # time.sleep(0.022)

    db_session.commit()


# update core
def update_test_core(start_separate_col=1, end_separate_col=15):
    """
    SQLAlchemy Core 방식으로 update_test 테이블의 product_name 컬럼의 값을 변경.

    :param start_separate_col: update시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                그러므로 update할 데이터의 시작 separate_col 값을 지정.
    :param end_separate_col: update할 데이터의 마지막 separate_col 값을 지정.
    """

    data_len = len(bench_data)

    for i in range(start_separate_col, end_separate_col+1):
        t = bench_data[random.randrange(0, data_len)]

        engine.execute(UpdateTest.__table__.update()\
                                           .where(UpdateTest.separate_col == i)\
                                           .values(product_name=t[1]))


# delete - used
def delete_test(start_separate_col=1, end_separate_col=15):
    """
    ORM 방식으로 delete_test 테이블의 데이터를 삭제.

    :param start_separate_col: delete시 separate_col 컬럼의 값이 where 조건으로 사용됨.
                                그러므로 delete할 데이터의 시작 separate_col 값을 지정.
    :param end_separate_col: delete할 데이터의 마지막 separate_col 값을 지정.
    """

    for i in range(start_separate_col, end_separate_col+1):
        db_session.query(DeleteTest).filter(DeleteTest.separate_col == i).delete()
        db_session.commit()
        # time.sleep(0.022)


# delete core
def delete_test_core(start_separate_col=1, end_separate_col=15):

    for i in range(start_separate_col, end_separate_col+1):
        engine.execute(DeleteTest.__table__.delete().where(DeleteTest.separate_col == i))



