from commons.mgr_config import ConfigManager
from commons.mgr_logger import LoggerManager
from commons.mgr_connection import ConnectionManager
from commons.funcs_common import get_mapper, get_elapsed_time_msg, get_equals_msg
from mappers.oracle_mappings import StringTest, NumericTest, DateTimeTest, BinaryTest, LOBTest

from datetime import datetime
from hashlib import md5
from sqlalchemy import Table, MetaData, desc, asc
from sqlalchemy.exc import DatabaseError, SAWarning

import os
import time
import warnings
import json


class VerifyFunctions:

    def __init__(self):
        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()

        self.src_engine = conn_mgr.src_engine
        self.src_db_session = conn_mgr.src_db_session

        self.trg_engine = conn_mgr.trg_engine
        self.trg_db_session = conn_mgr.trg_db_session

    def data_verify(self, data_type):

        rpt_dir_name = "datachecker_report"

        self.logger.debug("Func. data_verify is started")

        try:

            mapper = get_mapper(data_type)

            s_time = time.time()

            # START Step 1
            time_id = datetime.now()
            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(time_id))
            print("  Checking Number of Columns".format(mapper.__tablename__), flush=True, end=" ")
            self.logger.info("Start data check in the \"{}\" Table".format(mapper.__tablename__))

            # custom type (ex. nchar) 정의로 인해 출력되는 SAWarning 미출력 처리
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)

                src_mapper_tab = Table(mapper.__tablename__, MetaData(), autoload=True, autoload_with=self.src_engine)
                src_cols = [c.name for c in src_mapper_tab.columns]

                trg_mapper_tab = Table(mapper.__tablename__, MetaData(), autoload=True, autoload_with=self.trg_engine)
                trg_cols = [c.name for c in trg_mapper_tab.columns]

                # INTERVAL YEAR TO MONTH Type 미지원으로 column list 에서 제거
                if mapper == DateTimeTest:
                    src_cols.remove("col_inter_year_month")
                    trg_cols.remove("col_inter_year_month")

            cols_cmp = (src_cols == trg_cols)

            if not cols_cmp:
                print("... {}".format(get_equals_msg(cols_cmp)))
                print("    Number of Columns: {} Source: {}, Target: {} {}\n"
                      .format("{", len(src_cols), len(trg_cols), "}"))
                self.logger.info("  Number of Columns: {} Source: {}, Target: {}, Result: {} {}"
                                 .format("{", len(src_cols), len(trg_cols), get_equals_msg(cols_cmp), "}"))
                self.logger.info("End data check in the \"{}\" Table".format(mapper.__tablename__))
                return None

            print("... {}\n".format(get_equals_msg(cols_cmp)))
            self.logger.info("  Number of Columns: {} Source: {}, Target: {}, Result: {} {}"
                             .format("{", len(src_cols), len(trg_cols), get_equals_msg(cols_cmp), "}"))
            # END Step 1

            # START Step 2
            print("  Checking Count of Rows".format(mapper.__tablename__), flush=True, end=" ")

            src_row_cnt = self.src_db_session.query(mapper).count()
            trg_row_cnt = self.trg_db_session.query(mapper).count()

            row_cnt_cmp = (src_row_cnt == trg_row_cnt)

            if not row_cnt_cmp:
                print("... {}".format(get_equals_msg(row_cnt_cmp)))
                print("    Count of Rows: {} Source: {}, Target: {} {}\n"
                      .format("{", src_row_cnt, trg_row_cnt, "}"))
                self.logger.info("  Count of Rows: {} Source: {}, Target: {}, Result: {} {}"
                                 .format("{", src_row_cnt, trg_row_cnt, get_equals_msg(row_cnt_cmp), "}"))
                self.logger.info("End data check in the \"{}\" Table".format(mapper.__tablename__))
                return None

            print("... {}\n".format(get_equals_msg(row_cnt_cmp)))
            self.logger.info("  Count of Rows: {} Source: {}, Target: {}, Result: {} {}"
                             .format("{", src_row_cnt, trg_row_cnt, get_equals_msg(row_cnt_cmp), "}"))
            # END Step 2

            # START Step 3
            print("  Checking Data".format(mapper.__tablename__), flush=True, end=" ")

            self.logger.debug("Select source data")
            # Source Data Select
            if mapper == DateTimeTest:
                # src_data = self.src_engine.execute(mapper.__table__.select().order_by(asc(mapper.t_id)))
                src_data = self.src_db_session.query(mapper.t_id, mapper.col_date, mapper.col_timestamp,
                                                     mapper.col_inter_day_sec)\
                                              .order_by(asc(mapper.t_id))

            else:
                # src_data = self.src_engine.execute(mapper.__table__.select().order_by(asc(mapper.t_id)))
                src_data = self.src_db_session.query(mapper.__table__).order_by(asc(mapper.t_id)).all()

            self.logger.debug("SQL Query: {}".format(src_data))

            # Target Data Select
            self.logger.debug("Select target data")
            if mapper == DateTimeTest:
                # trg_data = self.trg_engine.execute(mapper.__table__.select().order_by(asc(mapper.t_id)))
                trg_data = self.trg_db_session.query(mapper.t_id, mapper.col_date, mapper.col_timestamp,
                                                     mapper.col_inter_day_sec)\
                                              .order_by(asc(mapper.t_id))
            else:
                # trg_data = self.trg_engine.execute(mapper.__table__.select().order_by(asc(mapper.t_id)))
                trg_data = self.trg_db_session.query(mapper.__table__).order_by(asc(mapper.t_id)).all()

            self.logger.debug("SQL Query: {}".format(trg_data))
            # END Step 3

            # START Step 4
            total_cmp_result = True

            detail_result = {}
            file_id = "{}-{:%Y-%m-%d_%H-%M-%S}".format(data_type, time_id)

            if mapper != LOBTest:
                for sd, td, i in zip(src_data, trg_data, range(src_row_cnt)):
                    detail_result[i + 1] = {}

                    for s, t, c_name in zip(sd, td, src_cols):
                        # t_id 값 float → int 형변환
                        if c_name == "t_id":
                            s = int(s)
                            t = int(t)

                        # date_test의 경우 datetime 객체 변환
                        if mapper == DateTimeTest:
                            if c_name == "col_date":
                                s = s.strftime("%Y-%m-%d %H:%M:%S")
                                t = t.strftime("%Y-%m-%d %H:%M:%S")
                            elif c_name == "col_timestamp":
                                s = s.strftime("%Y-%m-%d %H:%M:%S.%f")
                                t = t.strftime("%Y-%m-%d %H:%M:%S.%f")
                            elif c_name == "col_timezone":
                                s = s.strftime("%Y-%m-%d %H:%M:%S.%f %z")
                                t = t.strftime("%Y-%m-%d %H:%M:%S.%f %z")
                            # elif c_name == "col_inter_year_month":
                            #     s = None
                            #     t = None
                            elif c_name == "col_inter_day_sec":
                                s = str(s)
                                t = str(t)

                        cols_cmp_result = s == t
                        # 데이터별 비교 결과가 false일 경우 전체 false
                        if not cols_cmp_result:
                            total_cmp_result = False

                        detail_result[i + 1][c_name] = {
                            "Result": cols_cmp_result,
                            "Source data": s,
                            "Target data": t
                        }

            else:
                tmp_lob_cols = ["col_long", "col_clob", "col_nclob", "col_blob"]
                for sd, td, i in zip(src_data, trg_data, range(src_row_cnt)):
                    detail_result[i + 1] = {}
                    # t_id dict 저장
                    detail_result[i + 1]["t_id"] = {
                        "Result": sd[0] == td[0],
                        "Source": int(sd[0]),
                        "Target": int(td[0])
                    }
                    # 나머지 컬럼 dict 저장
                    for c_name, j in zip(tmp_lob_cols, range(1, len(src_cols) - 1, 2)):
                        s = sd[j]
                        t = td[j]

                        # lob data hashing
                        sld = md5(str(sd[j + 1]).encode("utf-8")).hexdigest()
                        tld = md5(str(td[j + 1]).encode("utf-8")).hexdigest()

                        cols_cmp_result = (s == t) and (sld == tld)
                        # 데이터별 비교 결과가 false일 경우 전체 false
                        if not cols_cmp_result:
                            total_cmp_result = False

                        detail_result[i + 1][c_name] = {
                            "Result": cols_cmp_result,
                            "Source alias": s,
                            "Source data length": len(sd[j + 1]),
                            "Source hash data": sld,
                            "Target alias": t,
                            "Target data length": len(td[j + 1]),
                            "Target hash data": tld
                        }

                        # lob_save가 yes일 경우
                        if self.config.lob_save == "YES":
                            if not os.path.exists(os.path.join(rpt_dir_name, file_id)):
                                os.makedirs(os.path.join(rpt_dir_name, file_id))

                            src_write_file_name = os.path.join(rpt_dir_name, file_id,
                                                               "{}_src_{}_{}".format(
                                                                   detail_result[i + 1]["t_id"].get("Source"),
                                                                   c_name.replace("col_", ""),
                                                                   s)
                                                               )

                            trg_write_file_name = os.path.join(rpt_dir_name, file_id,
                                                               "{}_trg_{}_{}".format(
                                                                   detail_result[i + 1]["t_id"].get("Target"),
                                                                   c_name.replace("col_", ""),
                                                                   t)
                                                               )

                            if c_name == "col_blob":
                                with open(src_write_file_name, "wb") as f:
                                    f.write(sd[j + 1])
                                with open(trg_write_file_name, "wb") as f:
                                    f.write(td[j + 1])
                            else:
                                with open(src_write_file_name, "w", encoding="utf-8") as f:
                                    f.write(sd[j + 1])
                                with open(src_write_file_name, "w", encoding="utf-8") as f:
                                    f.write(td[j + 1])

            print("... {}\n".format(get_equals_msg(total_cmp_result)))
            self.logger.info("  Total Result: {}".format(get_equals_msg(total_cmp_result)))

            # END Step 4

            # START Step 5
            print("  Writing Data in the detail result file", flush=True, end=" ")

            write_result = json.dumps(detail_result, indent=4, ensure_ascii=False)

            rpt_file_name = os.path.join(rpt_dir_name, "{}.rpt".format(file_id))

            self.logger.info("  Detail Result File: {}".format(rpt_file_name))

            if not os.path.exists(rpt_dir_name):
                os.makedirs(rpt_dir_name)

            with open(rpt_file_name, "a", encoding="utf-8") as f:

                f.write("+" + "-" * 118 + "+\n\n")
                f.write("  Data Type: {}\n".format(str(mapper.__tablename__).rstrip("_test").upper()))
                f.write("  Check Time: {:%Y-%m-%d %H:%M:%S}\n".format(time_id))
                f.write("  Columns: {} ({})\n".format(len(src_cols), src_cols))
                f.write("  Count of Rows: {}\n".format(src_row_cnt))
                if mapper == LOBTest:
                    f.write("  LOB File Save: {}\n".format(self.config.lob_save))
                f.write("  Total Result: {}\n".format(total_cmp_result))
                f.write("  Detail Result File: {}\n\n".format(write_result))

                f.write("+" + "-" * 118 + "+\n")

            # END Step 5

            e_time = time.time()

            print("... Success\n")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data check in the \"{}\" Table".format(mapper.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. data_verify is ended")
