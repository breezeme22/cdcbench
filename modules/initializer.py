from commons.logger_manager import LoggerManager
from commons.config_manager import ConfigManager
from commons.common_functions import get_selection
from commons.initial_functions import InitialFunctions
from mappers.oracle_mappings import UpdateTest, DeleteTest

from sqlalchemy.exc import DatabaseError

import os
import argparse


def initializer():

    # Working Directory를 ~/cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog="initializer", usage="%(prog)s <Options> [Argument]", allow_abbrev=False,
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument("--create", action="store_true",
                        help="create the objects and initiate the data related to CDCBENCH")

    groups.add_argument("--drop", action="store_true",
                        help="drop the objects related to CDCBENCH")

    groups.add_argument("--reset", action="store_true",
                        help="reset the objects and data related to CDCBENCH")

    groups.add_argument("-v", "--version", action="version", version="CDCBENCH Ver.1.1",
                        help="print CDCBENCH\'s Version.")

    parser.add_argument("-f", "--config", action="store", nargs="?", metavar="config_file_name", const="default.ini",
                        help="view or select configuration file")

    args = parser.parse_args()
    config = None
    logger = None

    try:

        # config 옵션 존재 유무에 따라 Config 객체 생성 분리
        # config 옵션 존재
        if args.config:

            config = ConfigManager(args.config)
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module initializer is started")

            # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 Config 내용을 출력
            if not args.create and not args.drop and not args.reset:
                print(" ########## " + str(args.config) + " ########## \n" +
                      config.view_setting_config() + " \n" +
                      config.view_source_connection_config() + " \n" +
                      config.view_init_data_config())

                exit(1)
                logger.info("Module initializer is ended")

        # config 옵션 없음
        elif (args.config is None) and (args.create or args.drop or args.reset):

            config = ConfigManager()
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module initializer started")

        # 아무 옵션도 없을 경우
        else:
            parser.error("Invalid options or arguments")

        logger.info("Load configuration file ({})".format(config.config_name))
        logger.info(repr(config))

        # db connection & initial data config 출력
        print(config.view_source_connection_config() + "\n" +
              config.view_init_data_config() + "\n")

        select_warn_msg = "initializer: warning: invalid input value. please enter \"y\" or \"n\".\n"

        initial_functions = InitialFunctions()
        update_total_data, update_commit_unit, delete_total_data, delete_commit_unit = \
            config.get_init_data_info().values()

        # create option
        if args.create:

            print_msg = "Do you want to create CDCBENCH related objects and data from the above database? [y/N]: "

            while True:
                select = get_selection(print_msg)

                if select is True:
                    initial_functions.create()
                    initial_functions.initializing_data(UpdateTest, update_total_data, update_commit_unit)
                    print()
                    initial_functions.initializing_data(DeleteTest, delete_total_data, delete_commit_unit)
                    break
                elif select is False:
                    print("\ninitializer: warning: operation is canceled by user")
                    break
                else:
                    print(select_warn_msg)

        # drop option
        elif args.drop:

            print_msg = "Do you want to delete CDCBENCH related objects and data from the above database? [y/N]: "

            while True:
                select = get_selection(print_msg)

                if select is True:
                    initial_functions.drop()
                    break
                elif select is False:
                    print("\ninitializer: warning: operation is canceled by user")
                else:
                    print(select_warn_msg)

        # reset option
        elif args.reset:

            print_msg = "Do you want to reset CDCBENCH related objects and data from the above database? [y/N]: "

            while True:
                select = get_selection(print_msg)

                if select is True:
                    initial_functions.drop()
                    initial_functions.create()
                    initial_functions.initializing_data(UpdateTest, update_total_data, update_commit_unit)
                    print()
                    initial_functions.initializing_data(DeleteTest, delete_total_data, delete_commit_unit)
                    break
                elif select is False:
                    print("\ninitializer: warning: operation is canceled by user")
                else:
                    print(select_warn_msg)

    except DatabaseError as dberr:

        print("The program was forced to end because of the following reasons : ")
        print("  {}".format(dberr.args[0]))
        exit(1)

    finally:
        logger.info("Module initializer is ended\n")


if __name__ == "__main__":
    try:
        initializer()
    except KeyboardInterrupt:
        print("\ninitializer: warning: operation is canceled by user")
        exit(1)
