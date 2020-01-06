from src.constants import *
from src.funcs_common import get_start_time_msg, get_elapsed_time_msg, \
                             print_error_msg, print_complete_msg
from src.funcs_datagen import get_sample_table_data
from src.mgr_logger import LoggerManager

from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.expression import func, bindparam, text

import time
import random
import logging
import os
import texttable


class FuncRanBench:

    __report_dir = "reports"

    def __init__(self, conn):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.engine = conn.engine
        self.connection = conn.engine.connect()
        self.db_session = conn.db_session
        self.dbms_type = conn.connection_info["dbms_type"]

    def run_record_random(self, total_record, record_range, sleep, tables, dml, files_data, rollback, now):

        result_dict = {"total_record": 0, "elapsed_time": None, "detail": {}}

        report_file_name = f"ranbench_{now:%Y-%m-%d}.rep"
        dml_seq = 1

        try:

            if not os.path.exists(self.__report_dir):
                os.makedirs(self.__report_dir)

            with open(os.path.join(self.__report_dir, report_file_name), "a", encoding="utf-8") as f:
                
                # Report 파일 및 TextTable 생성
                detail_tab = _make_report(f, now)

                start_time = time.time()

                with self.connection.begin() as tx:

                    while True:
                        random_record = random.randrange(record_range[0], record_range[1] + 1)
                        # random_record가 남은 record보다 클 경우 남은 record로 수행
                        if random_record > total_record:
                            random_record = total_record

                        random_table = tables[random.randrange(len(tables))]
                        if random_table.name not in result_dict["detail"]:
                            result_dict["detail"][random_table.name] = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}

                        random_dml = dml[random.randrange(len(dml))]

                        performed_column_names = random_table.columns.keys()[1:]

                        table_alias = random_table.name.split("_")[0].upper()
                        file_data = files_data[table_alias]

                        random_data = _get_random_data(random_record, file_data, random_table, performed_column_names,
                                                       self.dbms_type)

                        if random_dml == "INSERT":
                            dml_result = self.connection.execute(random_table.insert(), random_data)

                            if self.dbms_type == SQLSERVER:
                                _sum_record_count(result_dict, random_table.name, "INSERT", random_record)
                                total_record -= random_record
                            else:
                                _sum_record_count(result_dict, random_table.name, "INSERT", dml_result.rowcount)
                                total_record -= dml_result.rowcount

                        elif random_dml == "UPDATE":
                            dml_result = self._run_update(random_table, random_record, performed_column_names,
                                                          random_data)
                            if dml_result is None:
                                continue

                            if self.dbms_type == SQLSERVER:
                                _sum_record_count(result_dict, random_table.name, "UPDATE", random_record)
                                total_record -= random_record
                            else:
                                _sum_record_count(result_dict, random_table.name, "UPDATE", dml_result.rowcount)
                                total_record -= dml_result.rowcount

                        elif random_dml == "DELETE":
                            dml_result = self._run_delete(random_table, random_record)
                            if dml_result is None:
                                continue

                            if self.dbms_type == SQLSERVER:
                                _sum_record_count(result_dict, random_table.name, "DELETE", random_record)
                                total_record -= random_record
                            else:
                                _sum_record_count(result_dict, random_table.name, "DELETE", dml_result.rowcount)
                                total_record -= dml_result.rowcount

                        detail_tab.add_row([dml_seq, random_table, random_dml, random_record])
                        dml_seq += 1

                        time.sleep(sleep)

                        if total_record <= 0:
                            break

                    end_time = time.time()

                    # Report 출력
                    f.write(f"\n{detail_tab.draw()}\n\n")

                    # Transaction 종료
                    _complete_tx(tx, rollback, f)

        except DatabaseError as dberr:
            f.write("  Transaction Rollback;\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        result_dict["elapsed_time"] = get_elapsed_time_msg(end_time, start_time)

        return result_dict

    def run_time_random(self, running_time, record_range, sleep, tables, dml, files_data, rollback, now):

        result_dict = {"total_record": 0, "elapsed_time": None, "detail": {}}

        report_file_name = f"ranbench_{now:%Y-%m-%d}.rep"
        dml_seq = 1

        try:

            if not os.path.exists(self.__report_dir):
                os.makedirs(self.__report_dir)

            with open(os.path.join(self.__report_dir, report_file_name), "a", encoding="utf-8") as f:

                # Report 파일 및 TextTable 생성
                detail_tab = _make_report(f, now)

                run_end_time = time.time() + running_time
                start_time = time.time()

                with self.connection.begin() as tx:

                    while True:
                        random_record = random.randrange(record_range[0], record_range[1] + 1)

                        random_table = tables[random.randrange(len(tables))]
                        if random_table.name not in result_dict["detail"]:
                            result_dict["detail"][random_table.name] = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}

                        random_dml = dml[random.randrange(len(dml))]

                        performed_column_names = random_table.columns.keys()[1:]

                        table_alias = random_table.name.split("_")[0].upper()
                        file_data = files_data[table_alias]

                        random_data = _get_random_data(random_record, file_data, random_table, performed_column_names,
                                                       self.dbms_type)

                        if random_dml == "INSERT":
                            dml_result = self.connection.execute(random_table.insert(), random_data)
                            if self.dbms_type == SQLSERVER:
                                _sum_record_count(result_dict, random_table.name, "INSERT", random_record)
                            else:
                                _sum_record_count(result_dict, random_table.name, "INSERT", dml_result.rowcount)

                        elif random_dml == "UPDATE":
                            dml_result = self._run_update(random_table, random_record, performed_column_names,
                                                          random_data)
                            if dml_result is None:
                                continue

                            if self.dbms_type == SQLSERVER:
                                _sum_record_count(result_dict, random_table.name, "UPDATE", random_record)
                            else:
                                _sum_record_count(result_dict, random_table.name, "UPDATE", dml_result.rowcount)

                        elif random_dml == "DELETE":
                            dml_result = self._run_delete(random_table, random_record)
                            if dml_result is None:
                                continue

                            if self.dbms_type == SQLSERVER:
                                _sum_record_count(result_dict, random_table.name, "DELETE", random_record)
                            else:
                                _sum_record_count(result_dict, random_table.name, "DELETE", dml_result.rowcount)

                        detail_tab.add_row([dml_seq, random_table, random_dml, random_record])
                        dml_seq += 1

                        time.sleep(sleep)

                        if time.time() >= run_end_time:
                            break

                    end_time = time.time()

                    # Report 출력
                    f.write(f"\n{detail_tab.draw()}\n\n")

                    # Transaction 종료
                    _complete_tx(tx, rollback, f)

        except DatabaseError as dberr:
            f.write("  Transaction Rollback;\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        result_dict["elapsed_time"] = get_elapsed_time_msg(end_time, start_time)

        return result_dict

    def _run_update(self, random_table, random_record, performed_column_names, random_data):

        where_column = random_table.columns[random_table.columns.keys()[0]]

        # Update 실행 여부를 결정하기 위해 Count 조회
        row_count_query = self.db_session.query(where_column.label(where_column.name)).statement \
            .with_only_columns([func.count(where_column).label("ID_COUNT")])
        update_row_count = self.connection.execute(row_count_query).scalar()

        # Table의 총 레코드 수가 random_record 보다 작을 경우 Skip 함
        if update_row_count < random_record:
            return

        update_stmt = random_table.update() \
            .values(dict((column_name, bindparam(column_name))
                         for column_name in performed_column_names)) \
            .where(where_column == bindparam(f"b_{where_column.name}"))

        # DBMS 별로 랜덤함수 분리
        order_by_clause = _get_dbms_rand_function(self.dbms_type)

        update_rows_query = self.db_session.query(where_column).statement \
            .with_only_columns([where_column]) \
            .order_by(order_by_clause) \
            .limit(random_record)

        update_rows = self.connection.execute(update_rows_query).fetchall()

        for row_data, update_row in zip(random_data, update_rows):
            row_data[f"b_{where_column.name}"] = update_row[0]

        return self.connection.execute(update_stmt, random_data)

    def _run_delete(self, random_table, random_record):

        where_column = random_table.columns[random_table.columns.keys()[0]]

        # Delete 실행 여부를 결정하기 위해 Count 조회
        row_count_query = self.db_session.query(where_column.label(where_column.name)).statement \
            .with_only_columns([func.count(where_column).label("ID_COUNT")])
        delete_row_count = self.connection.execute(row_count_query).scalar()

        # Table의 총 레코드 수가 random_record 보다 작을 경우 Skip 함
        if delete_row_count < random_record:
            return

        delete_stmt = random_table.delete() \
            .where(where_column == bindparam(f"b_{where_column.name}"))

        # DBMS 별로 랜덤함수 분리
        order_by_clause = _get_dbms_rand_function(self.dbms_type)

        delete_rows_query = self.db_session.query(where_column).statement \
                                           .with_only_columns([where_column]) \
                                           .order_by(order_by_clause) \
                                           .limit(random_record)

        delete_rows = self.connection.execute(delete_rows_query).fetchall()

        key_data = []
        for key in delete_rows:
            key_data.append({f"b_{where_column.name}": key[0]})

        return self.connection.execute(delete_stmt, key_data)


def _sum_record_count(result_dict, table_name, dml, record_count):
    result_dict["total_record"] += record_count
    result_dict["detail"][table_name][dml] += record_count


def _get_random_data(num_of_record, file_data, table, column_names, dbms_type):

    list_of_row_data = []
    for i in range(num_of_record):
        list_of_row_data.append(
            get_sample_table_data(file_data, table.name.upper(), column_names, dbms_type=dbms_type)
        )

    return list_of_row_data


def _get_dbms_rand_function(dbms_type):
    if dbms_type == ORACLE:
        return text("dbms_random.value")
    elif dbms_type == MYSQL:
        return func.rand()
    elif dbms_type == SQLSERVER:
        return func.newid()
    else:
        return func.random()


def _make_report(f, now):

    f.write(f"{get_start_time_msg(now)}\n")

    detail_tab = texttable.Texttable()
    detail_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)

    detail_tab.set_cols_align(["r", "l", "l", "l"])
    detail_tab.header(["Seq#", "Table", "DML", "Record"])

    return detail_tab


def _complete_tx(tx, rollback, f):
    if rollback is True:
        tx.rollback()
        f.write("  ::: Transaction Rollback ::: \n")
    else:
        tx.commit()
        f.write("  ::: Transaction Commit ::: \n")
