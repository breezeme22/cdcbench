from src.constants import tqdm_bar_format, tqdm_ncols
from src.funcs_common import get_commit_msg, get_rollback_msg, get_elapsed_time_msg, get_object_name, \
                             get_start_time_msg, print_complete_msg, print_description_msg, print_error_msg
from src.funcs_datagen import get_sample_table_data, get_file_data, data_file_name
from src.mgr_logger import LoggerManager

from sqlalchemy.exc import DatabaseError
from datetime import datetime
from tqdm import tqdm

import time
import logging


class FuncsTypebench:

    def __init__(self, conn, mapper):

        # Logger 생성
        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.connection = conn.engine.connect()
        self.dbms_type = conn.connection_info["dbms_type"]
        self.mapper = mapper

    def insert(self, table_name, column_items, number_of_data, commit_unit, rollback, verbose):

        table = self.mapper.metadata.tables[get_object_name(self.mapper.metadata.tables.keys(), table_name)]

        all_column_names = table.columns.keys()[:]  # table column name 획득

        if column_items is None:
            selected_column_names = all_column_names[:]
            selected_column_names.remove(selected_column_names[0])  # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거
        else:
            if all(isinstance(item, int) for item in column_items):
                selected_column_names = [all_column_names[column_id-1] for column_id in column_items]
            else:
                selected_column_names = [get_object_name(all_column_names, column_name) for column_name in column_items]

        insert_info_msg = "Insert Information: {}\"Table Name\" : {}, \"Number of Data\": {}, " \
                          "\"Commit Unit\": {} {}".format("{", table, number_of_data, commit_unit, "}")

        self.logger.info(insert_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("INSERT", table, verbose)
        self.logger.info("Start data insert in the \"{}\" Table".format(table))

        list_of_row_data = []
        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])
        commit_count = 1

        start_time = time.time()

        try:
            for i in tqdm(range(1, number_of_data+1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):

                list_of_row_data.append(
                    get_sample_table_data(file_data, table_name, selected_column_names, dbms_type=self.dbms_type)
                )

                if i % commit_unit == 0:

                    with self.connection.begin() as tx:
                        self.connection.execute(table.insert(), list_of_row_data)
                        if rollback is True:
                            tx.rollback()
                            self.logger.debug(get_rollback_msg(commit_count))
                        else:
                            tx.commit()
                            self.logger.debug(get_commit_msg(commit_count))

                    commit_count += 1
                    list_of_row_data.clear()

            if number_of_data % commit_unit != 0:

                with self.connection.begin() as tx:
                    self.connection.execute(table.insert(), list_of_row_data)
                    if rollback:
                        tx.rollback()
                        self.logger.debug(get_rollback_msg(commit_count))
                    else:
                        tx.commit()
                        self.logger.debug(get_commit_msg(commit_count))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

        e_time = time.time()

        print_complete_msg(verbose, separate=False)

        elapse_time_msg = get_elapsed_time_msg(e_time, start_time)
        print("  {}\n".format(elapse_time_msg))
        self.logger.info(elapse_time_msg)

        self.logger.info("End data insert in the \"{}\" Table".format(table))

    def update(self, table_name, start_t_id, end_t_id, rollback, verbose):

        table = self.mapper.metadata.tables[get_object_name(self.mapper.metadata.tables.keys(), table_name)]

        update_info_msg = "Update Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
                          .format("{", table_name, start_t_id, end_t_id, "}")

        self.logger.info(update_info_msg)

        print(get_start_time_msg(datetime.now()))
        print_description_msg("UPDAT", table, verbose)
        self.logger.info("Start data update in the \"{}\" Table".format(table))

        commit_count = 1
        file_data = get_file_data(data_file_name[table_name.split("_")[0].upper()])

        column_names = table.columns.keys()[:]  # table column name 획득
        t_id = column_names[0]
        column_names.remove(column_names[0])  # Key 값은 Sequence 방식으로 생성하기에 Column List에서 제거

        start_time = time.time()

        try:
            with self.connection.begin() as tx:
                for i in tqdm(range(start_t_id, end_t_id + 1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):
                    self.connection.execute(
                        table.update().values(
                            get_sample_table_data(file_data, table_name, column_names, dbms_type=self.dbms_type)
                        ).where(table.columns[t_id] == i)
                    )

                if rollback:
                    tx.rollback()
                    self.logger.debug(get_rollback_msg(commit_count))
                else:
                    tx.commit()
                    self.logger.debug(get_commit_msg(commit_count))

        except DatabaseError as dberr:
            print("... Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            if self.log_level == logging.DEBUG:
                self.logger.exception(dberr.args[0])
            print_error_msg(dberr.args[0])

        end_time = time.time()

        print_complete_msg(verbose, separate=False)

        elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
        print("  {}\n".format(elapse_time_msg))
        self.logger.info(elapse_time_msg)

        self.logger.info("End data update in the \"{}\" Table".format(table))

    def delete(self, table_name, start_t_id, end_t_id, rollback, verbose):

        try:

            table = self.mapper.metadata.tables[get_object_name(self.mapper.metadata.tables.keys(), table_name)]

            t_id = table.columns.keys()[0]

            delete_info_msg = "Delete Information: {}\"Table Name\" : {}, \"Start T_ID\": {}, \"End T_ID\": {} {}" \
                .format("{", table, start_t_id, end_t_id, "}")

            self.logger.info(delete_info_msg)

            print(get_start_time_msg(datetime.now()))
            print_description_msg("DELET", table, verbose)
            self.logger.info("Start data delete in the \"{}\" Table".format(table))

            start_time = time.time()

            with self.connection.begin() as tx:

                for i in tqdm(range(start_t_id, end_t_id + 1), disable=verbose, ncols=tqdm_ncols, bar_format=tqdm_bar_format):
                    self.connection.execute(
                        table.delete().where(table.columns[t_id] == i)
                    )

                if rollback is True:
                    tx.rollback()
                    self.logger.debug(get_rollback_msg(1))
                else:
                    tx.commit()
                    self.logger.debug(get_commit_msg(1))

            end_time = time.time()

            print_complete_msg(verbose, separate=False)

            elapse_time_msg = get_elapsed_time_msg(end_time, start_time)
            print("  {}\n".format(elapse_time_msg))
            self.logger.info(elapse_time_msg)

            self.logger.info("End data delete in the \"{}\" Table".format(table))

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
