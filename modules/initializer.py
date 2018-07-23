from commons.config_load import *
from commons.logger_manager import *

import os
import argparse


def initializer():

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog="CDCBENCH", usage="%(prog)s [options]",
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument("-c", "--create", action="store_true",
                        help="Creates the objects used by the CDCBENCH and initiates the data.")

    groups.add_argument("-d", "--drop", action="store_true",
                        help="")

    groups.add_argument("-r", "--reset", action="store_true",
                        help="")

    groups.add_argument("-v", "--version", action="version", version="%(prog)s Ver.1.1",
                        help="Print CDCBENCH\'s Version.")

    parser.add_argument("-f", "--config", action="store", nargs="?", metavar="config_file_name", const="default.ini",
                        help="View Config file & Select Config file.")

    args = parser.parse_args()

    # Working Directory를 ~/cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

    # config 옵션 존재 유무에 따라 Config 객체 생성 분리
    if args.config:
        config = ConfigLoad(args.config)
        logger = LoggerManager(__name__, config.log_level).get_logger()

        # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 Config 내용을 출력
        if not args.create and not args.drop and not args.reset:
            print(" ########## " + str(args.config) + " ########## \n" +
                  config.view_setting_config() + " \n" +
                  config.view_connection_config() + " \n" +
                  config.view_init_data_config())

    else:
        config = ConfigLoad()
        logger = LoggerManager(__name__, config.log_level).get_logger()

    # db connection & initial data config 출력
    print(config.view_connection_config() + "\n" +
          config.view_init_data_config() + "\n")

    if args.create:
        # print(args)
        select = input("Do you want to proceed with the CDCBENCH initial configuration in the above database? [y/N]: ")

        print(select)
        print(type(select))

        if select.upper() == "Y":
            pass
        elif select.upper() == "N":
            pass
        else:
            pass

    elif args.drop:
        select = input("")

        if select.upper() == "Y":
            pass
        elif select.upper() == "N":
            pass
        else:
            pass

    elif args.reset:
        print(args)

    else:
        print("Invalid Options or Arguments. Please check your command.")


if __name__ == "__main__":
    initializer()
