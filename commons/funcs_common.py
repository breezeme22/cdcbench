from mappers import oracle_mappers, mysql_mappers, sqlserver_mappers, postgresql_mappers
from commons.constants import *

import json
import random


def get_cdcbench_version():
    """
    Changes
     - initializer에서 수행 대상을 source/target/both로 지정할 수 있도록 옵션 추가
     - db_type에 MySQL 추가 (현재 initializer 기능에서만 지원)
    :return: CDCBENCH Version
    """
    return "CDCBENCH Version 1.2.0"


# Selection Function
def get_selection(print_text):
    user_input = input(print_text)

    if len(user_input) == 0 or user_input is None:
        user_input = "N"

    user_input = user_input.strip().upper()

    if user_input == "Y":
        return True
    elif user_input == "N":
        return False
    else:
        return None


# data file read
def get_json_data(file_name):

    try:
        with open(file_name, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data

    except FileNotFoundError:
        raise FileNotFoundError("Data file ({}) does not exist.".format(file_name))


# return Elapse time
def get_elapsed_time_msg(start_time, end_time):

    try:
        s_time = float(start_time)
        e_time = float(end_time)

        return "Elapsed Time: {:.2f} Sec.".format(max(s_time, e_time) - min(s_time, e_time))

    except ValueError as err:
        raise ValueError(err)


# return Commit Message
def get_commit_msg(commit_value):
    return "{} Commit is occurred".format(commit_value)


# get true option
def get_true_option(args):

    for i in args:
        if args.get(i):
            return i

    return None


def get_mapper(dbms_type, table_name):

    mappers = None

    if dbms_type == dialect_driver[ORACLE]:
        mappers = oracle_mappers
    elif dbms_type == dialect_driver[MYSQL]:
        mappers = mysql_mappers
    elif dbms_type == dialect_driver[SQLSERVER]:
        mappers = sqlserver_mappers
    elif dbms_type == dialect_driver[POSTGRESQL]:
        mappers = postgresql_mappers

    if table_name == INSERT_TEST:
        return mappers.InsertTest
    elif table_name == UPDATE_TEST:
        return mappers.UpdateTest
    elif table_name == DELETE_TEST:
        return mappers.DeleteTest
    elif table_name == STRING_TEST:
        return mappers.StringTest
    elif table_name == NUMERIC_TEST:
        return mappers.NumericTest
    elif table_name == DATETIME_TEST:
        return mappers.DateTimeTest
    elif table_name == BINARY_TEST:
        return mappers.BinaryTest
    elif table_name == LOB_TEST:
        return mappers.LOBTest
    elif table_name == ORACLE_TEST:
        return mappers.OracleTest
    elif table_name == SQLSERVER_TEST:
        return mappers.SqlserverTest


def get_rowid_data():

    char_list = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
                 "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d",
                 "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x",
                 "y", "z"]

    rowid = "AAAShYAAFAAAAC9A"

    for i in range(2):
        rowid += char_list[random.randrange(len(char_list))]

    return rowid


def get_equals_msg(cmp_rst):

    if cmp_rst:
        return "Equals"
    else:
        return "Not Equals"


def get_except_msg(err):
    print()
    print("This program was terminated by force for the following reasons: ")
    print("  {}".format(err))
    print()


