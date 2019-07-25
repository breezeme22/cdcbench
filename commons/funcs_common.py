import json
import random
import argparse


class CustomHelpFormatter(argparse.HelpFormatter):
    def __init__(self,
                 prog,
                 indent_increment=2,
                 max_help_position=24,
                 width=None):
        super().__init__(prog,
                         indent_increment=indent_increment,
                         max_help_position=max_help_position,
                         width=width)

    def _format_action_invocation(self, action):
        if not action.option_strings or action.nargs == 0:
            return super()._format_action_invocation(action)
        default = self._get_default_metavar_for_optional(action)
        args_string = self._format_args(action, default)
        return ', '.join(action.option_strings) + ' ' + args_string


def get_cdcbench_version():
    """
    ### Changes
     * [common]: STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST 테이블 구조 변경
     * [common]: ORACLE_TEST (Oracle 전용), SQLSERVER_TEST (SQL Server 전용) 테이블 추가
     * [common]: SQL Logging 추가
     * [common]: Short Option 추가 (README.md 또는 -h/--help 참고)
     * [common]: Oracle 12c Pluggable Database 지원되도록 Oracle Connection 방식 변경
     * [common]: Configuration 변경
            ** 추가: sql_logging / schema_name
            ** 수정: db_type → dbms_type / total_num_of_data → number_of_data
            ** 삭제: lob_save
     * [common]: 모든 기능 (initializer, cdcbench, typebench)에서 모든 DBMS 지원
     * [datachecker]: 해당 기능 제거

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
    print("... See the log file (\"cdcbench.log\") for more information")
    print()


def strftimedelta(timedelta, fmt):
    d = {"days": timedelta.days}
    d["hours"], rem = divmod(timedelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
