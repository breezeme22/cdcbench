from commons.mgr_config import ConfigManager
from commons.mgr_logger import LoggerManager
from commons.mgr_connection import ConnectionManager
from commons.funcs_common import get_mapper
from mappers.oracle_mappings import StringTest, NumericTest, DateTest, BinaryTest, LOBTest

from sqlalchemy import Table, MetaData, select
from sqlalchemy.exc import DatabaseError, SAWarning
from datetime import datetime

import os
import time
import warnings


class VerifyFunctions:

    def __init__(self):
        self.CONFIG = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.CONFIG.log_level)

        conn_mgr = ConnectionManager()

        self.src_engine = conn_mgr.src_engine
        self.src_db_session = conn_mgr.src_db_session

        self.trg_engine = conn_mgr.trg_engine
        self.trg_db_session = conn_mgr.trg_db_session

    def data_verify(self, data_type):

        self.logger.debug("Func. data_verify is started")

        try:

            mapper = get_mapper(data_type)

            s_time = time.time()

            # Step 1-2
            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Checking Number of Columns & Count of Rows in the \"{}\" Table"
                  .format(mapper.__tablename__), flush=True, end=" ")
            self.logger.info("Start number of columns & count of rows check in the \"{}\" Table"
                             .format(mapper.__tablename__))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)

                src_md = MetaData()
                trg_md = MetaData()

                src_mapper_tab = Table(mapper.__tablename__, MetaData(), autoload=True, autoload_with=self.src_engine)
                src_cols = [c.name for c in src_mapper_tab.columns]

                trg_mapper_tab = Table(mapper.__tablename__, MetaData(), autoload=True, autoload_with=self.trg_engine)
                trg_cols = [c.name for c in trg_mapper_tab.columns]

            cols_cmp = (src_cols == trg_cols)

            src_row_cnt = self.src_db_session.query(mapper).count()
            trg_row_cnt = self.trg_db_session.query(mapper).count()

            row_cnt_cmp = (src_row_cnt == trg_row_cnt)

            if not cols_cmp or not row_cnt_cmp:
                print("... Not Equal")
                print("    Source: {}\"Num of Cols\": {}, \"Count of Rows\": {}{}".format("{", len(src_cols), src_row_cnt, "}"))
                print("    Target: {}\"Num of Cols\": {}, \"Count of Rows\": {}{}".format("{", len(trg_cols), trg_row_cnt, "}"))
                self.logger.info()
                return None

            print("... Equal\n")
            # END Step 1-2

            print("  Checking Data in the \"{}\" Table".format(mapper.__tablename__), flush=True, end=" ")

            print()
            print(mapper.__table__.select())
            print()
            src_data = self.src_engine.execute(mapper.__table__.select())

            for r in src_data:
                print(list(r))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. data_verify is ended")
