from commons.constants import SOURCE, TARGET, BOTH, sa_unsupported_dbms

from sqlalchemy.sql import select, func

import argparse
import logging
import texttable


class CustomHelpFormatter(argparse.RawTextHelpFormatter):
    """
    --help 명령 formatting Class
    """
    def __init__(self, prog, indent_increment=2, max_help_position=24, width=100):
        super().__init__(prog, indent_increment=indent_increment, max_help_position=max_help_position, width=width)

    def _format_action_invocation(self, action):
        if not action.option_strings or action.nargs == 0:
            return super()._format_action_invocation(action)
        default = self._get_default_metavar_for_optional(action)
        args_string = self._format_args(action, default)
        return ', '.join(action.option_strings) + ' ' + args_string


def get_cdcbench_version():
    """
    ## Changes

    ### cdcbench
    * --update 수행시 --where 옵션 사용할 경우 progress bar 출력되는 버그 수정

    :return: CDCBENCH Version
    """
    return "CDCBENCH Version 1.4.2.1"


def get_elapsed_time_msg(end_time, start_time):
    """
    작업 소요시간을 CDCBENCH에서 보여주는 format으로 생성
    :param end_time:
    :param start_time:
    :return: 정해진 포맷의 작업 소요시간
    """

    s_time = float(start_time)
    e_time = float(end_time)
    elapse_time = e_time - s_time

    return f"Elapsed Time: {elapse_time:.2f} Sec."


def get_commit_msg(commit_value):
    return f"{commit_value} Commit is occurred"


def get_rollback_msg(rollback_value):
    return f"{rollback_value} Rollback is occurred"


def get_true_option(args):
    """
    Dictionary에서 Value가 None이 아닌 Key를 검색
    :param args: Dictionary
    :return: None이 아닌 Key (없으면 None)
    """
    for i in args:
        if args.get(i):
            return i

    return None


def print_error_msg(err):
    """
    작업 수행 중 예외처리에 의해 종료될 경우 매개변수를 정해진 포맷으로 출력하고 프로그램을 종료
    :param err: 에러 메시지
    """
    print()
    print("This program was terminated by force for the following reasons: ")
    print(f"  {err}")
    exit(1)


def get_object_name(match_object_name, object_name_list):
    """
    :param match_object_name: 찾고자 하는 object name 
    :param object_name_list: object name을 검색할 리스트
    :return: 
    """
    for object_name in object_name_list:
        if object_name.upper() == match_object_name.upper():
            return object_name
    raise KeyError(match_object_name)


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
               + _view_option_info(args) \
               + _view_data_config(initial_update_conf, initial_delete_conf, (args.drop or args.without_data))

    elif destination == TARGET:
        return _view_config_name(config_name) \
               + _view_connection_config(destination, config.get("target_database")) \
               + _view_option_info(args) \
               + _view_data_config(initial_update_conf, initial_delete_conf, (args.drop or args.without_data))

    else:
        return _view_config_name(config_name) \
               + _view_connection_config(destination, config.get("source_database"), config.get("target_database")) \
               + _view_option_info(args) \
               + _view_data_config(initial_update_conf, initial_delete_conf, (args.drop or args.without_data))


def get_start_time_msg(time):
    return f"\n  ::: {time:%Y-%m-%d %H:%M:%S} ::: "


def print_complete_msg(rollback, verbose, end="", separate=True):
    msg = f"{'Rollback' if rollback else 'Commit'}"
    if verbose is True:
        print(f"... {msg}{end}")
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


def exec_database_error(logger, log_level, dberr, fail_print=True):
    """
    Database Error 발생시 처리하는 작업들
    :param logger: logger
    :param log_level: log_level
    :param dberr: DB Error Exception
    :param fail_print: True or False
    """
    if fail_print:
        print("... Fail")
    logger.error(dberr.args[0])
    logger.error(dberr.statement)
    logger.error(dberr.params)
    if log_level == logging.DEBUG:
        logger.exception(dberr.args[0])
    print_error_msg(dberr.args[0])


def exec_statement_error(logger, log_level, staterr):
    """
    Statement Error (데이터와 데이터 타입 불일치) 발생시 처리하는 작업들
    :param logger: logger
    :param log_level: log_level
    :param msg: Error Message
    """

    logger.error(staterr.orig)
    err_data = str(staterr.orig).split(": ")[1]
    print_error_msg(f"Entered data are not suitable for the data type of column: \n"
                    f"    {err_data}")


def pyodbc_exec_database_error(logger, log_level, dberr, fail_print=True):
    """
    Tibero Database Error 발생시 처리 작업
    :param logger: logger Object
    :param log_level: Log Level
    :param dberr: Exception Object
    :param fail_print: True or False
    :return:
    """
    if fail_print:
        print("... Fail")
    logger.error(dberr.args[1])
    if log_level == logging.DEBUG:
        logger.exception(dberr.args[1])
    print_error_msg(dberr.args[1])


def get_separate_col_val(engine, table, column):
    """
    특정 테이블 (table)의 최대 SEPARATE_COL값을 조회
    :param engine: engine Object
    :param table: Table Object
    :param column: Column (SEPARATE_COL) Object
    :return: select 결과가 null일 경우 1, null이 아닌 경우 결과값 + 1
    """
    sql = select([func.max(table.columns[column]).label("MAX_SEPARATE_COL")])
    result = engine.execute(sql).scalar()
    if result is None:
        return 1
    else:
        return result + 1


def sa_unsupported_dbms_module_limit(dbms_type):

    if dbms_type in sa_unsupported_dbms:
        print_error_msg(f"This module is not available in the following DBMS {sa_unsupported_dbms}")
