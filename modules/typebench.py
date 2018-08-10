from commons.mgr_logger import LoggerManager
from commons.mgr_config import ConfigManager
from commons.funcs_common import get_true_option, get_cdcbench_version
from commons.funcs_datatype import DataTypeFunctions

from sqlalchemy.exc import DatabaseError

import os
import argparse


def typebench():

    # Working Directory를 cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog="typebench", usage="%(prog)s [option...][argument...]", allow_abbrev=False,
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument("--string", action="store_true",
                        help="specifies the data of String type.")

    groups.add_argument("--numeric", action="store_true",
                        help="specifies the data of Numeric type.")

    groups.add_argument("--date", action="store_true",
                        help="specifies the data of Date type.")

    groups.add_argument("--binary", action="store_true",
                        help="specifies the data of Binary type.")

    groups.add_argument("--lob", action="store_true",
                        help="specifies the data of LOB type.")

    parser.add_argument("--insert", action="store", metavar="<number of data>", type=int,
                        help="insert the data into the specified data type table.")

    parser.add_argument("--commit", action="store", metavar="commit units", type=int,
                        help="specifies the commit unit. (--insert option is required)")

    parser.add_argument("--delete", action="store_true",
                        help="deletes the data into the specified data type table.")

    parser.add_argument("--config", action="store", nargs="?", metavar="config_file_name", const="default.ini",
                        help="view or select configuration file.")

    groups.add_argument("-v", "--version", action="version", version=get_cdcbench_version(),
                        help="print CDCBENCH\"s version.")

    args = parser.parse_args()

    # --string, --numeric, --date, --binary, --lob 이 --insert, --delete 없이 사용될 때
    if (args.string or args.numeric or args.date or args.binary or args.lob) and \
       (not args.insert and not args.delete):
        true_opt = get_true_option(args.__dict__)
        parser.error("--{} is required --insert or --delete".format(true_opt))
        
    # --insert, --delete 가 --string, --numeric, --date, --binary, --lob 없이 사용될 때
    elif (not args.string and not args.numeric and not args.date and not args.binary and not args.lob) \
            and (args.insert or args.delete):
        true_opt = get_true_option(args.__dict__)
        parser.error("--{} is required --string or --numeric or --date or --binary or --lob".format(true_opt))

    elif args.insert is None and args.commit is not None:
        parser.error("--commit is required --insert")

    config = None
    logger = None

    try:
        # config 옵션 존재 유무에 따라 Config 객체 생성 분리
        # config 옵션 있음
        if args.config:

            config = ConfigManager(args.config)
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module typebench is started")

            # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 Config 내용을 출력
            if not args.string and not args.numeric and not args.date and not args.binary and not args.lob \
               and not args.insert and not args.delete:

                print(" ########## " + str(args.config) + " ########## \n" +
                      config.view_setting_config() + " \n" +
                      config.view_source_connection_config() + " \n" +
                      config.view_init_data_config())

                exit(1)

        # config 옵션 없음
        elif (args.config is None) and (args.string or args.numeric or args.date or args.binary or args.lob or
                                        args.insert or args.delete):

            config = ConfigManager()
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module typebench is started")

        # 아무 옵션도 없을 경우
        else:
            parser.error("Invalid options or arguments")

        logger.info("Load configuration file ({})".format(config.config_name))
        logger.info(repr(config))

        datatype_functions = DataTypeFunctions()

        # string execution
        if args.string:

            if args.insert:

                if args.commit:
                    commit_unit = args.commit
                else:
                    commit_unit = 100

                datatype_functions.dtype_insert("string", args.insert, commit_unit)

            else:
                datatype_functions.dtype_delete("string")

        # numeric execution
        elif args.numeric:

            if args.insert:

                if args.commit:
                    commit_unit = args.commit
                else:
                    commit_unit = 100

                datatype_functions.dtype_insert("numeric", args.insert, commit_unit)

            else:
                datatype_functions.dtype_delete("numeric")

        # date execution
        elif args.date:

            if args.insert:

                if args.commit:
                    commit_unit = args.commit
                else:
                    commit_unit = 100

                datatype_functions.dtype_insert("date", args.insert, commit_unit)

            else:
                datatype_functions.dtype_delete("date")

        # binary execution
        elif args.binary:

            if args.insert:

                if args.commit:
                    commit_unit = args.commit
                else:
                    commit_unit = 100

                datatype_functions.dtype_insert("binary", args.insert, commit_unit)

            else:
                datatype_functions.dtype_delete("binary")

        # lob execution
        elif args.lob:

            if args.insert:

                if args.commit:
                    commit_unit = args.commit
                else:
                    commit_unit = 100

                datatype_functions.dtype_insert("lob", args.insert, commit_unit)

            else:
                datatype_functions.dtype_delete("lob")

    except FileNotFoundError as file_err:
        print("\nThe program was forced to end because of the following reasons: ")
        print("  {}".format(file_err))
        exit(1)

    except DatabaseError as dberr:
        print("The program was forced to end because of the following reasons: ")
        print("  {}".format(dberr.args[0]))
        exit(1)

    finally:
        if logger is not None:
            logger.info("Module typebench is ended\n")


if __name__ == "__main__":
    try:
        typebench()
    except KeyboardInterrupt:
        print("\ntypebench: warning: operation is canceled by user")
        exit(1)
