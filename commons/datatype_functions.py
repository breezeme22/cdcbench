from commons.config_manager import ConfigManager
from commons.logger_manager import LoggerManager
from commons.connection_manager import ConnectionManager
from commons.common_functions import *

from sqlalchemy.exc import DatabaseError
from datetime import datetime

import random
import os
import time


class DataTypeFunctions:

    def __init__(self):
        self.CONFIG = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.CONFIG.log_level)

        self.connection = ConnectionManager(self.CONFIG.get_source_connection_string())
        self.engine = self.connection.engine
        self.db_session = self.connection.db_session

    def dtype_insert(self, data_type, insert_data, commit_unit):

        self.logger.debug("Func. dtype_insert is started")
        
        try:

            mapper = get_mapper(data_type)

            file_name = "{}_type.dat".format(data_type)
            read_data = get_json_data(os.path.join("data", file_name))
            self.logger.debug("Load data file ({})".format(file_name))

            # JSON String을 배열 형태로 변형
            read_data_list = []
            for i in read_data:
                read_data_list.append(read_data.get(i))

            insert_info_msg = "Insert Information: {}'data type': {}, 'number of data': {}, 'commit_unit': {}{}" \
                              .format("{", data_type, insert_data, commit_unit, "}")

            self.logger.info(insert_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(mapper.__tablename__), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(mapper.__tablename__))

            data_list = []
            commit_count = 1

            # table column name 획득
            col_list = mapper.__table__.columns.keys()[:]
            col_list.remove("t_id")

            s_time = time.time()

            for i in range(1, insert_data+1):

                row_data = {}

                for j in range(len(col_list)):
                    row_data[col_list[j]] = read_data_list[j][random.randrange(0, len(read_data_list[j]))]

                data_list.append(row_data)

                if i % commit_unit == 0:
                    self.engine.execute(mapper.__table__.insert(), data_list)
                    self.logger.debug(get_commit_msg(commit_count))
                    commit_count += 1
                    data_list.clear()

            if insert_data % commit_unit != 0:
                self.engine.execute(mapper.__table__.insert(), data_list)
                self.logger.debug(get_commit_msg(commit_count))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(mapper.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. dtype_insert is ended")

    def binary_insert(self, insert_data, commit_unit):

        self.logger.debug("Func. binary_insert is started")

        try:

            data_type = "binary"
            mapper = get_mapper(data_type)

            insert_info_msg = "Insert Information: {}'data type': {}, 'number of data': {}, 'commit_unit': {}{}" \
                .format("{", data_type, insert_data, commit_unit, "}")

            self.logger.info(insert_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(mapper.__tablename__), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(mapper.__tablename__))

            data_list = []
            commit_count = 1

            s_time = time.time()

            for i in range(1, insert_data + 1):
                col_raw_rand = random.randrange(1, 2001)
                col_long_raw_rand = random.randrange(1, 2001)
                self.logger.debug("{}'col_row_rand_val': {}, 'col_long_raw_rand': {}{}"
                                  .format("{", col_raw_rand, col_long_raw_rand, "}"))

                row_data = {"col_raw": os.urandom(col_raw_rand),
                            "col_long_raw": os.urandom(col_long_raw_rand),
                            "col_rowid": get_rowid_data(),
                            "col_urowid": get_rowid_data()}

                data_list.append(row_data)

                if i % commit_unit == 0:
                    self.engine.execute(mapper.__table__.insert(), data_list)
                    self.logger.debug(get_commit_msg(commit_count))
                    commit_count += 1
                    data_list.clear()

            if insert_data % commit_unit != 0:
                self.engine.execute(mapper.__table__.insert(), data_list)
                self.logger.debug(get_commit_msg(commit_count))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(mapper.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. binary_insert is ended")

    def lob_insert(self, insert_data, commit_unit):

        self.logger.debug("Func. lob_insert is started")

        try:

            data_type = "lob"
            mapper = get_mapper(data_type)

            file_name = "{}_type.dat".format(data_type)
            read_data = get_json_data(os.path.join("data", file_name))
            self.logger.debug("Load data file ({})".format(file_name))

            # JSON String을 배열 형태로 변형
            read_data_list = []
            for i in read_data:
                read_data_list.append(read_data.get(i))

            insert_info_msg = "Insert Information: {}'data type': {}, 'number of data': {}, 'commit_unit': {}{}" \
                .format("{", data_type, insert_data, commit_unit, "}")

            self.logger.info(insert_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(mapper.__tablename__), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(mapper.__tablename__))

            data_list = []
            commit_count = 1

            # table column name 획득
            col_list = mapper.__table__.columns.keys()[:]
            col_list.remove("t_id")

            s_time = time.time()

            for i in range(1, insert_data + 1):

                row_data = {}

                # Data type에 따라 데이터 구성
                for j in range(0, len(col_list), 2):

                    lob_file_name = read_data_list[int(j/2)][random.randrange(0, len(read_data_list[int(j/2)]))]
                    file_extension = lob_file_name.split(".")[1]

                    row_data[col_list[j]] = lob_file_name

                    if file_extension == "txt":
                        with open(os.path.join("data", "lob_files", lob_file_name), "r", encoding="utf-8") as f:
                            row_data[col_list[j+1]] = f.read().encode("utf-8")
                            print("\n" + str(col_list[j+1]) + " | " + str(row_data[col_list[j + 1]]) + "\n")
                    else:
                        with open(os.path.join("data", "lob_files", lob_file_name), "rb") as f:
                            row_data[col_list[j+1]] = f.read()

                data_list.append(row_data)

                if i % commit_unit == 0:
                    self.engine.execute(mapper.__table__.insert(), data_list)
                    self.logger.debug(get_commit_msg(commit_count))
                    commit_count += 1
                    data_list.clear()

            if insert_data % commit_unit != 0:
                self.engine.execute(mapper.__table__.insert(), data_list)
                self.logger.debug(get_commit_msg(commit_count))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data insert in the \"{}\" Table".format(mapper.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. lob_insert is ended")

    def dtype_delete(self, data_type):

        self.logger.debug("Func. dtype_delete is started")

        try:

            mapper = get_mapper(data_type)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(mapper.__tablename__), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(mapper.__tablename__))

            s_time = time.time()

            self.engine.execute(mapper.__table__.delete())
            self.logger.debug(get_commit_msg(1))

            e_time = time.time()

            print("... Success")

            elapse_time_msg = get_elapsed_time_msg(e_time, s_time)
            print("  {}".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(mapper.__tablename__))

        except DatabaseError as dberr:
            print("... Fail\n")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise

        finally:
            self.logger.debug("Func. dtype_delete is ended")

