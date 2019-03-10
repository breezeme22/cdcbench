from commons.constants import *
from commons.funcs_common import get_elapsed_time_msg, get_equals_msg, strftimedelta
from commons.mgr_config import ConfigManager
from commons.mgr_connection import ConnectionManager
from commons.mgr_logger import LoggerManager

from datetime import datetime
from hashlib import md5
from sqlalchemy import Table, MetaData, desc, asc
from sqlalchemy.exc import DatabaseError, SAWarning

import os
import time
import warnings
import json
import binascii


class VerifyFunctions:

    def __init__(self):
        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()

        self.src_engine = conn_mgr.src_engine
        self.src_db_session = conn_mgr.src_db_session
        self.trg_engine = conn_mgr.trg_engine
        self.trg_db_session = conn_mgr.trg_db_session

        self.src_mapper = conn_mgr.get_src_mapper()
        self.trg_mapper = conn_mgr.get_trg_mapper()

        if self.config.source_dbms_type == dialect_driver[SQLSERVER]:
            for src_table in self.src_mapper.metadata.sorted_tables:
                src_table.schema = self.config.source_user_id

        if self.config.target_dbms_type == dialect_driver[SQLSERVER]:
            for trg_table in self.trg_mapper.metadata.sorted_tables:
                trg_table.schema = self.config.target_user_id

    def data_verify(self, table_name):

        __rpt_dir_name = "reports"

        self.logger.debug("Func. data_verify is started")

        try:

            src_table = self.src_mapper.metadata.tables[table_name]
            trg_table = self.src_mapper.metadata.tables[table_name]

            start_time = time.time()

            # START Step 1
            time_id = datetime.now()
            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(time_id))
            print("  Checking Number of Columns".format(table_name), flush=True, end=" ")
            self.logger.info("Start data check in the \"{}\" Table".format(table_name))

            # custom type (ex. nchar) 정의로 인해 출력되는 SAWarning 미출력 처리
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)

                src_column_names = src_table.columns.keys()[:]

                trg_column_names = trg_table.columns.keys()[:]

                # INTERVAL YEAR TO MONTH Type 미지원으로 column list 에서 제거
                if table_name == DATETIME_TEST:
                    src_column_names.remove("COL_INTER_YEAR_MONTH")
                    trg_column_names.remove("COL_INTER_YEAR_MONTH")

            cols_cmp = (src_column_names == trg_column_names)

            if not cols_cmp:
                print("... {}".format(get_equals_msg(cols_cmp)))
                print("    Number of Columns: {} Source: {}, Target: {} {}\n"
                      .format("{", len(src_column_names), len(trg_column_names), "}"))
                self.logger.info("  Number of Columns: {} Source: {}, Target: {}, Result: {} {}"
                                 .format("{", len(src_column_names), len(trg_column_names), get_equals_msg(cols_cmp), "}"))
                self.logger.info("End data check in the \"{}\" Table".format(table_name))
                return None

            print("... {}\n".format(get_equals_msg(cols_cmp)))
            self.logger.info("  Number of Columns: {} Source: {}, Target: {}, Result: {} {}"
                             .format("{", len(src_column_names), len(trg_column_names), get_equals_msg(cols_cmp), "}"))
            # END Step 1

            # START Step 2
            print("  Checking Count of Rows".format(table_name), flush=True, end=" ")

            src_row_count = self.src_db_session.query(src_table).count()
            trg_row_count = self.trg_db_session.query(trg_table).count()

            row_count_compare = (src_row_count == trg_row_count)

            if not row_count_compare:
                print("... {}".format(get_equals_msg(row_count_compare)))
                print("    Count of Rows: {} Source: {}, Target: {} {}\n"
                      .format("{", src_row_count, trg_row_count, "}"))
                self.logger.info("  Count of Rows: {} Source: {}, Target: {}, Result: {} {}"
                                 .format("{", src_row_count, trg_row_count, get_equals_msg(row_count_compare), "}"))
                self.logger.info("End data check in the \"{}\" Table".format(table_name))
                return None

            print("... {}\n".format(get_equals_msg(row_count_compare)))
            self.logger.info("  Count of Rows: {} Source: {}, Target: {}, Result: {} {}"
                             .format("{", src_row_count, trg_row_count, get_equals_msg(row_count_compare), "}"))
            # END Step 2

            # START Step 3
            print("  Checking Data".format(table_name), flush=True, end=" ")

            self.logger.debug("Select Source Table")
            # Source Table Select
            if table_name == DATETIME_TEST:
                src_table_result = self.src_db_session.query(src_table.columns[src_column_names[0]],
                                                             src_table.columns[src_column_names[1]],
                                                             src_table.columns[src_column_names[2]],
                                                             src_table.columns[src_column_names[3]],
                                                             src_table.columns[src_column_names[4]])\
                                                      .order_by(asc(src_table.columns[src_column_names[0]]))

            else:
                src_table_result = self.src_db_session.query(src_table)\
                                                      .order_by(asc(src_table.columns[src_column_names[0]])).all()

            # Target Table Select
            self.logger.debug("Select Target Table")
            if table_name == DATETIME_TEST:
                trg_table_result = self.trg_db_session.query(trg_table.columns[trg_column_names[0]],
                                                             trg_table.columns[trg_column_names[1]],
                                                             trg_table.columns[trg_column_names[2]],
                                                             trg_table.columns[trg_column_names[3]],
                                                             trg_table.columns[trg_column_names[4]])\
                                                      .order_by(asc(trg_table.columns[trg_column_names[0]]))
            else:
                trg_table_result = self.trg_db_session.query(trg_table)\
                                                      .order_by(asc(trg_table.columns[trg_column_names[0]])).all()

            # END Step 3

            # START Step 4
            total_compare_result = True

            detail_result = {}
            file_id = "{}-{:%Y-%m-%d_%H-%M-%S}".format(table_name, time_id)

            if table_name != LOB_TEST:
                for src_row, trg_row, i in zip(src_table_result, trg_table_result, range(src_row_count)):
                    detail_result[i + 1] = {}

                    for src_column_data, trg_column_data, column_name in zip(src_row, trg_row, src_column_names):

                        # T_ID 값 FLOAT → INT 형변환
                        if column_name == "T_ID":
                            src_column_data = int(src_column_data)
                            trg_column_data = int(trg_column_data)

                        # NUMERIC_TEST의 일부 데이터 타입 객체 타입 변환
                        elif column_name in ["COL_DECIMAL", "COL_NUMERIC", "COL_SMALLMONEY", "COL_MONEY"]:
                            src_column_data = float(src_column_data)
                            trg_column_data = float(trg_column_data)

                        # DATETIME_TEST의 경우 datetime 객체 변환
                        elif column_name == "COL_DATETIME":
                            src_column_data = src_column_data.strftime("%Y-%m-%d %H:%M:%S")
                            trg_column_data = trg_column_data.strftime("%Y-%m-%d %H:%M:%S")
                        elif column_name == "COL_TIMESTAMP":
                            src_column_data = src_column_data.strftime("%Y-%m-%d %H:%M:%S.%f")
                            trg_column_data = trg_column_data.strftime("%Y-%m-%d %H:%M:%S.%f")
                        elif column_name == "COL_TIMESTAMP2":
                            if self.config.source_dbms_type == dialect_driver[SQLSERVER]:
                                src_column_data = src_column_data
                                trg_column_data = trg_column_data
                            else:
                                src_column_data = src_column_data.strftime("%Y-%m-%d %H:%M:%S.%f")
                                trg_column_data = trg_column_data.strftime("%Y-%m-%d %H:%M:%S.%f")
                        # elif c_name == "COL_INTER_YEAR_MONTH":
                        #     s = None
                        #     t = None
                        elif column_name == "COL_INTER_DAY_SEC":
                            src_column_data = strftimedelta(src_column_data, "{days} {hours:02d}:{minutes:02d}:{seconds:02d}")
                            trg_column_data = strftimedelta(trg_column_data, "{days} {hours:02d}:{minutes:02d}:{seconds:02d}")

                        elif column_name in ["COL_BINARY", "COL_VARBINARY", "COL_LONG_BINARY"]:
                            src_column_data = binascii.hexlify(src_column_data).decode("utf-8")
                            trg_column_data = binascii.hexlify(trg_column_data).decode("utf-8")

                        column_compare_result = (src_column_data == trg_column_data)
                        # 데이터별 비교 결과가 false일 경우 전체 false
                        if not column_compare_result:
                            total_compare_result = False

                        detail_result[i + 1][column_name] = {
                            "Result": column_compare_result,
                            "Source data": src_column_data,
                            "Target data": trg_column_data
                        }

            else:
                lob_test_columns = ["COL_CLOB", "COL_NCLOB", "COL_BLOB"]
                for src_row, trg_row, i in zip(src_table_result, trg_table_result, range(src_row_count)):
                    detail_result[i + 1] = {}
                    # t_id dict 저장
                    detail_result[i + 1]["T_ID"] = {
                        "Result": src_row[0] == trg_row[0],
                        "Source": int(src_row[0]),
                        "Target": int(trg_row[0])
                    }
                    # 나머지 컬럼 dict 저장
                    for column_name, j in zip(lob_test_columns, range(1, len(src_column_names) - 1, 2)):
                        src_lob_alias = src_row[j]
                        trg_lob_alias = trg_row[j]

                        if column_name == "COL_BLOB":
                            src_lob_value = src_row[j + 1].hex()
                            trg_lob_value = trg_row[j + 1].hex()
                        else:
                            src_lob_value = str(src_row[j + 1])
                            trg_lob_value = str(trg_row[j + 1])

                        column_compare_result = (src_lob_alias == trg_lob_alias) and (src_lob_value == trg_lob_value)
                        # 데이터별 비교 결과가 false일 경우 전체 false
                        if not column_compare_result:
                            total_compare_result = False

                        detail_result[i + 1][column_name] = {
                            "Result": column_compare_result,
                            "Source Alias": src_lob_alias,
                            "Source Data Length": len(src_row[j + 1]),
                            "Source Data Value": str(src_lob_value),
                            "Target Alias": trg_lob_alias,
                            "Target Data Length": len(trg_row[j + 1]),
                            "Target Data Value": str(trg_lob_value)
                        }

                        # lob_save가 yes일 경우
                        if self.config.lob_save == "YES":
                            if not os.path.exists(os.path.join(__rpt_dir_name, file_id)):
                                os.makedirs(os.path.join(__rpt_dir_name, file_id))

                            src_write_file_name = os.path.join(__rpt_dir_name, file_id,
                                                               "{}_src_{}_{}".format(
                                                                   detail_result[i + 1]["T_ID"].get("Source"),
                                                                   column_name.replace("COL_", ""),
                                                                   src_lob_alias)
                                                               )

                            trg_write_file_name = os.path.join(__rpt_dir_name, file_id,
                                                               "{}_trg_{}_{}".format(
                                                                   detail_result[i + 1]["T_ID"].get("Target"),
                                                                   column_name.replace("COL_", ""),
                                                                   trg_lob_alias)
                                                               )

                            if column_name == "COL_BLOB":
                                with open(src_write_file_name, "wb") as f:
                                    f.write(src_row[j + 1])
                                with open(trg_write_file_name, "wb") as f:
                                    f.write(trg_row[j + 1])
                            else:
                                with open(src_write_file_name, "w", encoding="utf-8") as f:
                                    f.write(src_row[j + 1])
                                with open(src_write_file_name, "w", encoding="utf-8") as f:
                                    f.write(trg_row[j + 1])

            print("... {}\n".format(get_equals_msg(total_compare_result)))
            self.logger.info("  Total Result: {}".format(get_equals_msg(total_compare_result)))

            # END Step 4

            # START Step 5
            print("  Writing Data in the detail result file", flush=True, end=" ")

            write_result = json.dumps(detail_result, indent=4, ensure_ascii=False)

            rpt_file_name = os.path.join(__rpt_dir_name, "{}.rpt".format(file_id))

            self.logger.info("  Detail Result File: {}".format(rpt_file_name))

            if not os.path.exists(__rpt_dir_name):
                os.makedirs(__rpt_dir_name)

            with open(rpt_file_name, "a", encoding="utf-8") as f:

                f.write("+" + "-" * 118 + "+\n\n")
                f.write("  Table Name: {}\n".format(table_name))
                f.write("  Check Time: {:%Y-%m-%d %H:%M:%S}\n".format(time_id))
                f.write("  Columns: {} ({})\n".format(len(src_column_names), src_column_names))
                f.write("  Count of Rows: {}\n".format(src_row_count))
                if table_name == LOB_TEST:
                    f.write("  LOB File Save: {}\n".format(self.config.lob_save))
                f.write("  Total Result: {}\n".format(total_compare_result))
                f.write("  Detail Result File: {}\n\n".format(write_result))

                f.write("+" + "-" * 118 + "+\n")

            # END Step 5

            end_time = time.time()

            print("... Success\n")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data check in the \"{}\" Table".format(table_name))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. data_verify is ended")
