from commons.logger_manager import LoggerManager
from commons.config_manager import ConfigManager
from commons.dml_functions import DmlFuntions

from sqlalchemy.exc import DatabaseError

import os
import argparse


def cdcbench():

    # Working Directory를 cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog="cdcbench", usage="%(prog)s [option...][argument...]", allow_abbrev=False,
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument("--insert", action="store", metavar="<number of data>", type=int,
                        help="insert data in the database.")

    parser.add_argument("--commit", action="store", metavar="commit units", type=int,
                        help="specify the commit unit. (--insert option is required)")

    parser.add_argument("--single", action="store_true",
                        help="change the insert method to single insert. (--insert option is required)")

    groups.add_argument("--update", action="store", nargs=2, metavar=("<start separate_col>", "<end separate_col>"),
                        type=int, help="update data in the database. (Value of <separate_col> "
                                       "must be greater than the value of <start separate_col>)")

    groups.add_argument("--delete", action="store", nargs=2, metavar=("<start separate_col>", "<end separate_col>"),
                        type=int, help="delete data in the database. (Value of <separate_col> "
                                       "must be greater than the value of <start separate_col>)")

    parser.add_argument("--config", action="store", nargs="?", metavar="config_file_name", const="default.ini",
                        help="view or select configuration file.")

    groups.add_argument("-v", "--version", action="version", version="%(prog)s Ver.1.1",
                        help="print CDCBENCH\"s version.")

    args = parser.parse_args()

    # --commit, --single 옵션이 --insert 옵션 없이 실행될 경우
    if args.insert is None and (args.commit is not None or args.single):
        parser.error("--commit or --single is required --insert")

    config = None
    logger = None

    try:
        # config 옵션 존재 유무에 따라 Config 객체 생성 분리
        # config 옵션 있음
        if args.config:

            config = ConfigManager(args.config)
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module cdcbench is started")

            # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 Config 내용을 출력
            if args.insert is None and args.commit is None and not args.single and \
               args.update is None and args.delete is None:

                print(" ########## " + str(args.config) + " ########## \n" +
                      config.view_setting_config() + " \n" +
                      config.view_source_connection_config() + " \n" +
                      config.view_init_data_config())

                exit(1)

        # config 옵션 없음
        elif (args.config is None) and (args.insert is not None or args.update is not None or args.delete is not None):

            config = ConfigManager()
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module cdcbench is started")

        # 아무 옵션도 없을 경우
        else:
            parser.error("Invalid options or arguments")

        logger.info("Load configuration file ({})".format(config.config_name))
        logger.info(repr(config))

        dml_functions = DmlFuntions()
        val_err_msg = "value of start separate_col is greater than value of end separate_col"

        # insert execution
        if args.insert:

            if args.commit:
                commit_unit = args.commit
            else:
                commit_unit = 1000

            if args.single:
                dml_functions.insert_orm(args.insert, commit_unit)
            else:
                dml_functions.insert_core(args.insert, commit_unit)

        # update execution
        elif args.update:

            start_val = args.update[0]
            end_val = args.update[1]

            if start_val <= end_val:
                dml_functions.update_core(start_val, end_val)
            else:
                parser.error(val_err_msg)

        # delete execution
        elif args.delete:

            start_val = args.delete[0]
            end_val = args.delete[1]

            if start_val <= end_val:
                dml_functions.delete_core(start_val, end_val)
            else:
                parser.error(val_err_msg)

    except DatabaseError as dberr:

        print("The program was forced to end because of the following reasons : ")
        print("  {}".format(dberr.args[0]))
        exit(1)

    finally:
        logger.info("Module cdcbench is ended\n")


if __name__ == "__main__":
    try:
        cdcbench()
    except KeyboardInterrupt:
        print("\ncdcbench: warning: operation is canceled by user")
        exit(1)
