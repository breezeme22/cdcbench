from mappers.oracle_mappings import StringTest, NumericTest, DateTimeTest, BinaryTest, LOBTest

import json
import random


def get_cdcbench_version():
    return "CDCBENCH Ver.1.1.1"


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


def get_mapper(mapper_name):

    if mapper_name == "string":
        return StringTest
    elif mapper_name == "numeric":
        return NumericTest
    elif mapper_name == "datetime":
        return DateTimeTest
    elif mapper_name == "binary":
        return BinaryTest
    elif mapper_name == "lob":
        return LOBTest


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


