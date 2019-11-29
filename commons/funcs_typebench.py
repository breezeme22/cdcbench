from commons.constants import *
from commons.funcs_common import get_json_data, get_commit_msg, get_rollback_msg, get_elapsed_time_msg
from commons.mgr_logger import LoggerManager
from commons.funcs_datagen import gen_sample_table_data

from sqlalchemy import and_
from sqlalchemy.exc import DatabaseError
from datetime import datetime

import os
import time
import logging


class FuncsTypebench:

    __data_dir = "data"
    __lob_data_dir = "lob_files"

    def __init__(self, conn, mapper):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.src_connection = conn.engine.connect()

        self.source_dbms_type = conn.connection_info["dbms_type"]

        self.src_mapper = mapper.get_mappers()

    def insert(self, table_name, number_of_data, commit_unit, is_rollback):

        try:

            if self.source_dbms_type == POSTGRESQL:
                src_table = self.src_mapper.metadata.tables[table_name.lower()]
            else:
                src_table = self.src_mapper.metadata.tables[table_name]

            # table column name 획득
            column_names = src_table.columns.keys()[:]
            column_names.remove(column_names[0])

            insert_info_msg = "Insert Information: {}\"Table Name\" : {}, \"Number of Data\": {}, " \
                              "\"Commit Unit\": {} {}".format("{", src_table, number_of_data, commit_unit, "}")

            self.logger.info(insert_info_msg)

            file_data = None

            if table_name != BINARY_TEST:
                file_name = "{}.dat".format(table_name.split("_")[0].lower())
                self.logger.debug("Load data file ({})".format(file_name))
                file_data = get_json_data(os.path.join(self.__data_dir, file_name))

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Inserting data in the \"{}\" Table".format(src_table), flush=True, end=" ")
            self.logger.info("Start data insert in the \"{}\" Table".format(src_table))

            list_of_row_data = []
            commit_count = 1

            start_time = time.time()

            for i in range(1, number_of_data+1):

                list_of_row_data.append(gen_sample_table_data(self.source_dbms_type, file_data, table_name, column_names))

                if i % commit_unit == 0:

                    with self.src_connection.begin() as tx:
                        self.src_connection.execute(src_table.insert(), list_of_row_data)
                        if is_rollback is True:
                            tx.rollback()
                            self.logger.debug(get_rollback_msg(commit_count))
                        else:
                            tx.commit()
                            self.logger.debug(get_commit_msg(commit_count))

                    commit_count += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:

                with self.src_connection.begin() as tx:
                    self.src_connection.execute(src_table.insert(), list_of_row_data)
                    if is_rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(commit_count))
                    else:
                        tx.commit()
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
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        except UnicodeEncodeError as unierr:
            print("... Fail")
            self.logger.error(unierr)
            if self.log_level == logging.DEBUG:
                self.logger.exception(unierr)
            raise

        except FileNotFoundError as ferr:
            print("... Fail")
            self.logger.error(ferr)
            if self.log_level == logging.DEBUG:
                self.logger.exception(ferr)
            raise

        finally:
            self.logger.debug("Func.insert is ended")

    def update(self, table_name, start_t_id, end_t_id, is_rollback):

        try:

            if self.source_dbms_type == POSTGRESQL:
                src_table = self.src_mapper.metadata.tables[table_name.lower()]
            else:
                src_table = self.src_mapper.metadata.tables[table_name]

            # table column name 획득
            column_names = src_table.columns.keys()[:]
            column_t_id = column_names.pop(0)

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

            commit_count = 1

            start_time = time.time()

            for i in range(start_t_id, end_t_id+1):

                with self.src_connection.begin() as tx:
                    self.src_connection.execute(src_table.update()
                                                .values(gen_sample_table_data(self.source_dbms_type, file_data, table_name, column_names))
                                                .where(src_table.columns[column_t_id] == i))
                    if is_rollback is True:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(i))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(i))

                commit_count += 1

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
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        except UnicodeEncodeError as unierr:
            print("... Fail")
            self.logger.error(unierr)
            if self.log_level == logging.DEBUG:
                self.logger.exception(unierr)
            raise

        except FileNotFoundError as ferr:
            print("... Fail")
            self.logger.error(ferr)
            if self.log_level == logging.DEBUG:
                self.logger.exception(ferr)
            raise

        finally:
            self.logger.debug("Func.update is ended")

    def delete(self, table_name, start_t_id, end_t_id, is_rollback):

        try:

            if self.source_dbms_type == POSTGRESQL:
                src_table = self.src_mapper.metadata.tables[table_name.lower()]
            else:
                src_table = self.src_mapper.metadata.tables[table_name]

            column_t_id = src_table.columns.keys()[0]

            delete_info_msg = "Delete Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
                .format("{", src_table, start_t_id, end_t_id, "}")

            self.logger.info(delete_info_msg)

            print("\n  @{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
            print("  Deleting data in the \"{}\" Table".format(src_table), flush=True, end=" ")
            self.logger.info("Start data delete in the \"{}\" Table".format(src_table))

            start_time = time.time()

            with self.src_connection.begin() as tx:
                self.src_connection.execute(src_table.delete()
                                            .where(and_(start_t_id <= src_table.columns[column_t_id],
                                                        src_table.columns[column_t_id] <= end_t_id)))
                if is_rollback is True:
                    tx.rollback()
                    self.logger.debug(get_rollback_msg(1))
                else:
                    tx.commit()
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
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            raise

        finally:
            self.logger.debug("Func.delete is ended")

