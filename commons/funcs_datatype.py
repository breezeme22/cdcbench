from commons.constants import *
from commons.funcs_common import get_json_data, get_rowid_data, get_commit_msg, get_elapsed_time_msg
from commons.mgr_config import ConfigManager
from commons.mgr_connection import ConnectionManager
from commons.mgr_logger import LoggerManager

from sqlalchemy import and_
from sqlalchemy.exc import DatabaseError
from datetime import datetime, timedelta

import random
import os
import time


class DataTypeFunctions:

    __data_dir = "data"
    __lob_data_dir = "lob_files"

    def __init__(self):
        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        conn_mgr = ConnectionManager()
        self.src_engine = conn_mgr.src_engine

        self.src_mapper = conn_mgr.get_src_mapper()

        if self.config.source_dbms_type == dialect_driver[SQLSERVER] or \
                self.config.source_dbms_type == dialect_driver[POSTGRESQL]:
            for table in self.src_mapper.metadata.sorted_tables:
                table.schema = self.config.source_schema_name

    def dtype_insert(self, table_name, number_of_data, commit_unit):

        self.logger.debug("Func. dtype_insert is started")

        try:

            src_table = self.src_mapper.metadata.tables[table_name]

            insert_info_msg = "Insert Information: {}\"Table Name\" : {}, \"Number of Data\": {}, " \
                              "\"Commit Unit\": {} {}".format("{", table_name, number_of_data, commit_unit, "}")

            self.logger.info(insert_info_msg)

            file_data = None

            if table_name != BINARY_TEST:
                file_name = "{}.dat".format(table_name.split("_")[0].lower())
                file_data = get_json_data(os.path.join(self.__data_dir, file_name))
                self.logger.debug("Load data file ({})".format(file_name))

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(src_table), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(src_table))

            # table column name 획득
            column_names = src_table.columns.keys()[:]
            column_names.remove("T_ID")

            list_of_row_data = []
            commit_count = 1

            start_time = time.time()

            for i in range(1, number_of_data+1):

                row_data = {}

                # STRING_TEST 테이블 데이터 처리
                if table_name == STRING_TEST:
                    for key in column_names:

                        sample_data_count = len(file_data[key])

                        if sample_data_count > 0:
                            # COL_TEXT 컬럼 데이터 처리
                            if key == "COL_TEXT":
                                text_file_name = file_data[key][random.randrange(sample_data_count)]
                                with open(os.path.join(self.__data_dir, self.__lob_data_dir, text_file_name), "r",
                                          encoding="utf-8") as f:
                                    column_data = f.read()
                            else:
                                column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                # NUMERIC_TEST 테이블 데이터 처리
                elif table_name == NUMERIC_TEST:
                    for key in column_names:
                        sample_data_count = len(file_data[key])
                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                # DATETIME_TEST 테이블 데이터 처리
                elif table_name == DATETIME_TEST:
                    for key in column_names:

                        sample_data_count = len(file_data[key])
                        formatted_data = None

                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]

                            if key == "COL_DATETIME":
                                formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S")
                            elif key == "COL_TIMESTAMP":
                                formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S.%f")
                            elif key == "COL_TIMESTAMP2":
                                formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S.%f")
                            elif key == "COL_INTER_YEAR_MONTH":
                                formatted_data = "{}-{}".format(column_data[0], column_data[1])
                            elif key == "COL_INTER_DAY_SEC":
                                if self.config.source_dbms_type == dialect_driver[ORACLE]:
                                    formatted_data = timedelta(days=column_data[0], hours=column_data[1],
                                                           minutes=column_data[2], seconds=column_data[3],
                                                           microseconds=column_data[4])
                                else:
                                    formatted_data = "{} {}:{}:{}.{}".format(column_data[0], column_data[1],
                                                                             column_data[2], column_data[3],
                                                                             column_data[4])

                        row_data[key] = formatted_data

                # BINARY_TEST 테이블 데이터 처리
                elif table_name == BINARY_TEST:
                    col_binary = os.urandom(random.randrange(1, 1001))
                    col_varbinary = os.urandom(random.randrange(1, 1001))
                    col_long_binary = os.urandom(random.randrange(1, 2001))
                    self.logger.debug("{}'COL_BINARY Length': {}, 'COL_VARBINARY Length': {}, "
                                      "'COL_LONG_BINARY Length': {}{}"
                                      .format("{", len(col_binary), len(col_varbinary), len(col_long_binary), "}"))

                    row_data = {
                        "COL_BINARY": col_binary,
                        "COL_VARBINARY": col_varbinary,
                        "COL_LONG_BINARY": col_long_binary
                    }

                # LOB_TEST 테이블 데이터 처리
                elif table_name == LOB_TEST:
                    for key in file_data.keys():
                        sample_data_count = len(file_data[key])

                        if sample_data_count > 0:
                            lob_file_name = file_data[key][random.randrange(sample_data_count)]
                            file_extension = lob_file_name.split(".")[1]

                            row_data["{}_ALIAS".format(key)] = lob_file_name

                            if file_extension == "txt":
                                with open(os.path.join(self.__data_dir, self.__lob_data_dir, lob_file_name), "r",
                                          encoding="utf-8") as f:
                                    row_data["{}_DATA".format(key)] = f.read()
                            else:
                                with open(os.path.join(self.__data_dir, self.__lob_data_dir, lob_file_name), "rb") as f:
                                    row_data["{}_DATA".format(key)] = f.read()

                        else:
                            row_data["{}_ALIAS".format(key)] = None
                            row_data["{}_DATA".format(key)] = None

                # ORACLE_TEST 테이블 데이터 처리
                elif table_name == ORACLE_TEST:
                    row_data["COL_ROWID"] = get_rowid_data()

                    for key in file_data.keys():
                        sample_data_count = len(file_data[key])

                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                # SQLSERVER_TEST 테이블 데이터 처리
                elif table_name == SQLSERVER_TEST:
                    for key in column_names:
                        sample_data_count = len(file_data[key])

                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                list_of_row_data.append(row_data)

                if i % commit_unit == 0:
                    self.src_engine.execute(src_table.insert(), list_of_row_data)
                    self.logger.debug(get_commit_msg(commit_count))
                    commit_count += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:
                self.src_engine.execute(src_table.insert(inline=True), list_of_row_data)
                self.logger.debug(get_commit_msg(commit_count))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(src_table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        except UnicodeEncodeError as unierr:
            print("... Fail")
            self.logger.error(unierr)
            raise

        except FileNotFoundError as ferr:
            print("... Fail")
            self.logger.error(ferr)
            raise

        finally:
            self.logger.debug("Func. dtype_insert is ended")

    def dtype_update(self, table_name, start_t_id, end_t_id):

        self.logger.debug("Func. dtype_update is started")

        try:

            src_table = self.src_mapper.metadata.tables[table_name]

            update_info_msg = "Update Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
                              .format("{", table_name, start_t_id, end_t_id, "}")

            self.logger.info(update_info_msg)

            file_data = None

            if table_name != BINARY_TEST:
                file_name = "{}.dat".format(table_name.split("_")[0].lower())
                file_data = get_json_data(os.path.join(self.__data_dir, file_name))
                self.logger.debug("Load data file ({})".format(file_name))

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Updating data in the \"{}\" Table".format(src_table), flush=True, end=" ")
            self.logger.info("Start data update in the \"{}\" Table".format(src_table))

            # table column name 획득
            column_names = src_table.columns.keys()[:]
            column_names.remove("T_ID")

            list_of_row_data = []
            commit_count = 1

            start_time = time.time()

            for i in range(start_t_id, end_t_id+1):

                row_data = {}

                # STRING_TEST 테이블 데이터 처리
                if table_name == STRING_TEST:
                    for key in column_names:
                        sample_data_count = len(file_data[key])
                        if sample_data_count > 0:
                            # COL_TEXT 컬럼 데이터 처리
                            if key == "COL_TEXT":
                                text_file_name = file_data[key][random.randrange(sample_data_count)]
                                with open(os.path.join(self.__data_dir, self.__lob_data_dir, text_file_name), "r",
                                          encoding="utf-8") as f:
                                    column_data = f.read()
                            else:
                                column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                # NUMERIC_TEST 테이블 데이터 처리
                if table_name == NUMERIC_TEST:
                    for key in column_names:
                        sample_data_count = len(file_data[key])
                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                # DATETIME_TEST 테이블 데이터 처리
                elif table_name == DATETIME_TEST:
                    for key in column_names:
                        column_total_data_len = len(file_data[key])
                        formatted_data = None

                        if column_total_data_len > 0:
                            column_data = file_data[key][random.randrange(column_total_data_len)]

                            if key == "COL_DATETIME":
                                formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S")
                            elif key == "COL_TIMESTAMP":
                                formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S.%f")
                            elif key == "COL_TIMESTAMP2":
                                formatted_data = datetime.strptime(column_data, "%Y-%m-%d %H:%M:%S.%f")
                            elif key == "COL_INTER_YEAR_MONTH":
                                formatted_data = "{}-{}".format(column_data[0], column_data[1])
                            elif key == "COL_INTER_DAY_SEC":
                                if self.config.source_dbms_type == dialect_driver[ORACLE]:
                                    formatted_data = timedelta(days=column_data[0], hours=column_data[1],
                                                               minutes=column_data[2], seconds=column_data[3],
                                                               microseconds=column_data[4])
                                else:
                                    formatted_data = "{} {}:{}:{}.{}".format(column_data[0], column_data[1],
                                                                             column_data[2], column_data[3],
                                                                             column_data[4])

                        row_data[key] = formatted_data

                # BINARY_TEST 테이블 데이터 처리
                elif table_name == BINARY_TEST:
                    col_binary = os.urandom(random.randrange(1, 1001))
                    col_varbinary = os.urandom(random.randrange(1, 1001))
                    col_long_binary = os.urandom(random.randrange(1, 2001))
                    self.logger.debug("{}'COL_BINARY Length': {}, 'COL_VARBINARY Length': {}, "
                                      "'COL_LONG_BINARY Length': {}{}"
                                      .format("{", len(col_binary), len(col_varbinary), len(col_long_binary), "}"))

                    row_data = {
                        "COL_BINARY": col_binary,
                        "COL_VARBINARY": col_varbinary,
                        "COL_LONG_BINARY": col_long_binary
                    }

                # LOB_TEST 테이블 데이터 처리
                elif table_name == LOB_TEST:
                    for key in file_data.keys():
                        column_total_data_len = len(file_data[key])

                        if column_total_data_len > 0:
                            lob_file_name = file_data[key][random.randrange(column_total_data_len)]
                            file_extension = lob_file_name.split(".")[1]

                            row_data["{}_ALIAS".format(key)] = lob_file_name

                            if file_extension == "txt":
                                with open(os.path.join(self.__data_dir, self.__lob_data_dir, lob_file_name), "r",
                                          encoding="utf-8") as f:
                                    row_data["{}_DATA".format(key)] = f.read()
                            else:
                                with open(os.path.join(self.__data_dir, self.__lob_data_dir, lob_file_name), "rb") as f:
                                    row_data["{}_DATA".format(key)] = f.read()

                        else:
                            row_data["{}_ALIAS".format(key)] = None
                            row_data["{}_DATA".format(key)] = None

                # ORACLE_TEST 테이블 데이터 처리
                elif table_name == ORACLE_TEST:
                    row_data["COL_ROWID"] = get_rowid_data()

                    for key in file_data.keys():
                        sample_data_count = len(file_data[key])

                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                # SQLSERVER_TEST 테이블 데이터 처리
                elif table_name == SQLSERVER_TEST:
                    for key in column_names:
                        sample_data_count = len(file_data[key])

                        if sample_data_count > 0:
                            column_data = file_data[key][random.randrange(sample_data_count)]
                        else:
                            column_data = None

                        row_data[key] = column_data

                list_of_row_data.append(row_data)

                self.src_engine.execute(src_table.update()
                                                 .values(row_data)
                                                 .where(src_table.columns["T_ID"] == i))
                commit_count += 1
                list_of_row_data.clear()

                self.logger.debug(get_commit_msg(i))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data update in the \"{}\" Table".format(src_table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        except UnicodeEncodeError as unierr:
            print("... Fail")
            self.logger.error(unierr)
            raise

        except FileNotFoundError as ferr:
            print("... Fail")
            self.logger.error(ferr)
            raise

        finally:
            self.logger.debug("Func. dtype_update is ended")

    def dtype_delete(self, table_name, start_t_id, end_t_id):

        self.logger.debug("Func. dtype_delete is started")

        try:

            src_table = self.src_mapper.metadata.tables[table_name]

            delete_info_msg = "Delete Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
                .format("{", table_name, start_t_id, end_t_id, "}")

            self.logger.info(delete_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(src_table), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(src_table))

            start_time = time.time()

            self.src_engine.execute(src_table.delete()
                                             .where(and_(start_t_id <= src_table.columns["T_ID"],
                                                         src_table.columns["T_ID"] <= end_t_id)))
            self.logger.debug(get_commit_msg(1))

            end_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(src_table))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. dtype_delete is ended")

