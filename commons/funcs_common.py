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
     * [common]: -f, --config 옵션의 인자에 확장자 없는 파일명을 허용합니다.
     * [common]: sql_logging 레벨이 변경되었습니다.
             ** None: SQL Logging을 수행하지 않음
             ** SQL: 수행되는 SQL을 로깅
             ** ALL: 수행되는 SQL과 데이터가 함께 로깅됨
     * [initializer]: 테이블 생성시 키 제약조건을 설정할 수 있습니다. 조건 및 옵션은 다음과 같습니다. 해당 기능은 MySQL에서는 지원하지 않습니다.
             ** -n, --non-key: 테이블에 키 제약조건을 설정하지 않습니다. (라이브러리 제약으로 PK가 생성된 후 삭제하는 절차로 진행)
             ** -u, --unique: 테이블의 키 컬럼을 Unique Constraint를 설정합니다. (라이브러리 제약으로 PK 삭제 후 생성하는 절차로 진행)
             ** -p, --primary (Default): 테이블의 키 컬럼을 Primary key로 설정합니다.
     * [initializer]: initializer 수행시 초기 데이터를 생성하지 않고, 구조만을 생성할 수 있습니다.
             ** -w, --without-data

    :return: CDCBENCH Version
    """
    return "CDCBENCH Version 1.3.0"


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


# return Rollback Message
def get_rollback_msg(rollback_value):
    return "{} Rollback is occurred".format(rollback_value)


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


def strftimedelta(timedelta, fmt):
    d = {"days": timedelta.days}
    d["hours"], rem = divmod(timedelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
