from commons.logger_manager import LoggerManager
from commons.config_manager import ConfigManager
from commons.common_functions import *
from commons.initial_functions import InitialFunctions

import os
import argparse


def initializer():

    # Working Directory를 ~/cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog="initializer", usage="%(prog)s [options]",
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument("-c", "--create", action="store_true",
                        help="create the objects and initiate the data related to CDCBENCH")

    groups.add_argument("-d", "--drop", action="store_true",
                        help="drop the objects related to CDCBENCH")

    groups.add_argument("-r", "--reset", action="store_true",
                        help="reset the objects and data related to CDCBENCH")

    groups.add_argument("-v", "--version", action="version", version="CDCBENCH Ver.1.1",
                        help="print CDCBENCH\'s Version.")

    parser.add_argument("-f", "--config", action="store", nargs="?", metavar="config_file_name", const="default.ini",
                        help="view or select configuration file")

    args = parser.parse_args()
    config = None
    logger = None

    # config 옵션 존재 유무에 따라 Config 객체 생성 분리
    # config 옵션 존재
    if args.config:
        config = ConfigManager(args.config)
        logger = LoggerManager.get_logger(__name__, config.log_level)

        logger.info("the initializer started.")

        # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 Config 내용을 출력
        if not args.create and not args.drop and not args.reset:
            print(" ########## " + str(args.config) + " ########## \n" +
                  config.view_setting_config() + " \n" +
                  config.view_source_connection_config() + " \n" +
                  config.view_init_data_config())

            exit(1)
            print("the initializer ended.")
            logger.info("the initializer ended.")

    # config 옵션 없음
    elif (args.config is None) and (args.create or args.drop or args.reset):
        config = ConfigManager()
        logger = LoggerManager.get_logger(__name__, config.log_level)

        logger.info("the initializer started.")

    # 아무 옵션도 없을 경우
    else:
        parser.error("invalid options or arguments")

    logger.info("configuration file ({}) load".format(config.config_name))
    logger.info(repr(config))

    # db connection & initial data config 출력
    print(config.view_source_connection_config() + "\n" +
          config.view_init_data_config() + "\n")

    select_warn_msg = "initializer: warning: invalid input value. please enter 'y' or 'n'.\n"

    initial_functions = InitialFunctions()

    # create option
    if args.create:

        print_msg = "Do you want to proceed with the CDCBENCH initial configuration in the above database? [y/N]: "

        while True:
            select = selection_return(print_msg)

            if select is True:
                initial_functions.create()
                break
            elif select is False:
                print()
                break
            else:
                print(select_warn_msg)

    # drop option
    elif args.drop:

        print_msg = "Do you want to delete CDCBENCH related objects and data from the above database now? [y/N]: "

        while True:
            select = selection_return(print_msg)

            if select is True:
                initial_functions.drop()
                break
            elif select is False:
                break
            else:
                print(select_warn_msg)

    # reset option
    elif args.reset:

        print_msg = "Do you want to reset CDCBENCH related objects and data from the above database now? [y/N]: "

        while True:
            select = selection_return(print_msg)

            if select is True:
                initial_functions.reset()
                break
            elif select is False:
                break
            else:
                print(select_warn_msg)

    print("the initializer is ended.")
    logger.info("the initializer is ended.")


if __name__ == "__main__":
    try:
        initializer()
    except KeyboardInterrupt:
        print("\ninitializer: warning: operation is cancelled by user")
        exit(1)
