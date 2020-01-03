from src.constants import SOURCE, TARGET, BOTH

import argparse
import texttable


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


# return Elapse time
def get_elapsed_time_msg(end_time, start_time):

    s_time = float(start_time)
    e_time = float(end_time)
    elapse_time = e_time - s_time

    return f"Elapsed Time: {elapse_time:.2f} Sec."


# return Commit Message
def get_commit_msg(commit_value):
    return f"{commit_value} Commit is occurred"


# return Rollback Message
def get_rollback_msg(rollback_value):
    return f"{rollback_value} Rollback is occurred"


# get true option
def get_true_option(args):

    for i in args:
        if args.get(i):
            return i

    return None


def print_error_msg(err):
    print()
    print("This program was terminated by force for the following reasons: ")
    print(f"  {err}")
    print()
    exit(1)


def get_object_name(match_object_name, object_name_list):
    for object_name in object_name_list:
        if object_name.upper() == match_object_name.upper():
            return object_name
    raise KeyError


def _view_config_name(config_name):
    return f"\n  [File: {config_name}]\n"


def _view_setting_config(setting_conf):

    setting_tab = texttable.Texttable()
    setting_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
    setting_tab.set_cols_width([20, 35])
    setting_tab.set_cols_align(["r", "l"])
    setting_tab.header(["[Setting Info.]", ""])

    for x, y in zip(setting_conf.keys(), setting_conf.values()):
        setting_tab.add_row([x, y])

    return f"\n{setting_tab.draw()}\n"


def _view_connection_config(destination, db_conf, trg_db_conf=None):

    db_tab = texttable.Texttable()
    db_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)

    if destination == BOTH:
        db_tab.set_cols_width([20, 16, 16])
        db_tab.set_cols_align(["r", "l", "l"])
        db_tab.header(["[Database Info.]", "Source", "Target"])

        for x, y, z in zip(db_conf.keys(), db_conf.values(), trg_db_conf.values()):
            db_tab.add_row([x, y, z])
    else:
        db_tab.set_cols_width([20, 35])
        db_tab.set_cols_align(["r", "l"])
        db_tab.header(["[Database Info.]", destination.title()])

        for x, y in zip(db_conf.keys(), db_conf.values()):
            db_tab.add_row([x, y])

    return f"\n{db_tab.draw()}\n"


def _view_data_config(initial_update_conf, initial_delete_conf, view_flag=False):

    if view_flag is True:
        return ""

    init_tab = texttable.Texttable()
    init_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
    init_tab.set_cols_width([20, 16, 16])
    init_tab.set_cols_align(["r", "l", "l"])
    init_tab.header(["[Data Info.]", "UPDATE_TEST", "DELETE_TEST"])

    for x, y, z in zip(initial_update_conf.keys(), initial_update_conf.values(), initial_delete_conf.values()):
        init_tab.add_row([x, y, z])

    return f"\n{init_tab.draw()}\n"


def _view_option_info(args):

    option_tab = texttable.Texttable()
    option_tab.set_deco(texttable.Texttable.HEADER | texttable.Texttable.VLINES)
    option_tab.set_cols_width([20, 35])
    option_tab.set_cols_align(["r", "l"])
    option_tab.header(["[Option Info.]", ""])

    if not args.primary and not args.unique and not args.non_key:
        args.primary = True

    option_dict = {
        "Execution Option": get_true_option({"Create": args.create, "Drop": args.drop, "Reset": args.reset}),
    }

    if args.create or args.reset:
        option_dict["Data Option"] = get_true_option({"Objects": args.without_data, "Data": args.only_data,
                                                      "Objects & Data": True})
        option_dict["Key Option"] = get_true_option({"Primary Key": args.primary, "Unique Key": args.unique,
                                                     "Non Key": args.non_key})

    for x, y in zip(option_dict.keys(), option_dict.values()):
        option_tab.add_row([x, y])

    return f"\n{option_tab.draw()}\n"


def view_config_file(config):
    return _view_config_name(config.get("config_name")) \
           + _view_setting_config(config.get("setting")) \
           + _view_connection_config(BOTH, config.get("source_database"), config.get("target_database")) \
           + _view_data_config(config.get("initial_update_test_data"), config.get("initial_delete_test_data"))


def view_runtime_config(destination, config, args):

    config_name = config.get("config_name")
    initial_update_conf = config.get("initial_update_test_data")
    initial_delete_conf = config.get("initial_delete_test_data")

    if destination == SOURCE:
        return _view_config_name(config_name) \
               + _view_connection_config(destination, config.get("source_database")) \
               + _view_data_config(initial_update_conf, initial_delete_conf, args.drop) \
               + _view_option_info(args)

    elif destination == TARGET:
        return _view_config_name(config_name) \
               + _view_connection_config(destination, config.get("target_database")) \
               + _view_data_config(initial_update_conf, initial_delete_conf, args.drop) \
               + _view_option_info(args)

    else:
        return _view_config_name(config_name) \
               + _view_connection_config(destination, config.get("source_database"), config.get("target_database")) \
               + _view_data_config(initial_update_conf, initial_delete_conf, args.drop) \
               + _view_option_info(args)


def get_start_time_msg(time):
    return f"\n  ::: {time:%Y-%m-%d %H:%M:%S} ::: "


def print_complete_msg(verbose, end="", separate=True):
    if verbose is True:
        print(f"... Complete{end}")
    else:
        if separate:
            print()
        else:
            return


def print_description_msg(dml, table_name, end_flag):

    if end_flag:
        print(f"  {dml.title()}ing data in the \"{table_name}\" Table ", end="", flush=True)
    else:
        print(f"  {dml.title()}ing data in the \"{table_name}\" Table ", flush=True)


def isint(s):
    try:
        int(s)
        return True
    except ValueError:
        return False
