import os
import argparse
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

from commons.mgr_logger import LoggerManager
from commons.mgr_config import ConfigManager
from commons.funcs_verify import VerifyFunctions
from commons.funcs_common import get_cdcbench_version, get_except_msg

from sqlalchemy.exc import DatabaseError


def dchecker():
    # Working Directory를 cdcbench로 변경
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog="datachecker", usage="%(prog)s [option...][argument...]", allow_abbrev=False,
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

    parser.add_argument("--config", action="store", nargs="?", metavar="config_file_name", const="default.ini",
                        help="view or select configuration file.")

    groups.add_argument("-v", "--version", action="version", version=get_cdcbench_version(),
                        help="print CDCBENCH\"s version.")

    args = parser.parse_args()

    config = None
    logger = None

    try:
        # config 옵션 존재 유무에 따라 Config 객체 생성 분리
        # config 옵션 있음
        if args.config:

            config = ConfigManager(args.config)
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module datachecker is started")

            # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 Config 내용을 출력
            if not args.string and not args.numeric and not args.date and not args.binary and not args.lob:
                print(" ########## " + str(args.config) + " ########## \n" +
                      config.view_setting_config() + " \n" +
                      config.view_source_connection_config() + " \n" +
                      config.view_target_connection_config() + " \n" +
                      config.view_init_data_config())

                exit(1)

        # config 옵션 없음
        elif (args.config is None) and (args.string or args.numeric or args.date or args.binary or args.lob):

            config = ConfigManager()
            logger = LoggerManager.get_logger(__name__, config.log_level)

            logger.info("Module datachecker is started")

        # 아무 옵션도 없을 경우
        else:
            parser.error("Invalid options or arguments")

        logger.info("Load configuration file ({})".format(config.config_name))
        logger.info(repr(config))

        verify_functions = VerifyFunctions()

        # string execution
        if args.string:
            verify_functions.data_verify("string")

        # numeric execution
        elif args.numeric:
            verify_functions.data_verify("numeric")

        # date execution
        elif args.date:
            verify_functions.data_verify("date")

        # binary execution
        elif args.binary:
            verify_functions.data_verify("binary")

        # lob execution
        elif args.lob:
            verify_functions.data_verify("lob")

    except FileNotFoundError as ferr:
        get_except_msg(ferr)
        exit(1)

    except KeyError as kerr:
        get_except_msg("Configuration Parameter does not existed: {}".format(kerr))
        exit(1)

    except DatabaseError as dberr:
        get_except_msg(dberr.args[0])
        exit(1)

    finally:
        if logger is not None:
            logger.info("Module datachecker is ended\n")


if __name__ == "__main__":
    try:
        dchecker()
    except KeyboardInterrupt:
        print("\ndatachecker: warning: operation is canceled by user")
        exit(1)
