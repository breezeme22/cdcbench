import logging
import sys
import time
import os
import argparse

from datetime import datetime

from cdcbench.config_load import *
from cdcbench.installer import Installer
from cdcbench.manage_data import *


def main():

    config = ConfigLoad()
    logger = LoggerManager().get_logger()

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=6)

    parser = argparse.ArgumentParser(prog='cdcbench', usage='%(prog)s [options]',
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument('-n', '--install', action="store_true",
                        help='create Object & Init models')

    groups.add_argument('-i', '--insert', action='store', metavar='<Data rows>', type=int, default=0,
                        help='models insert')

    parser.add_argument('-c', '--commit', action='store', nargs='?', metavar='commit units', type=int, const=1000,
                        help='-i or --insert option is required. commit unit value input.')

    parser.add_argument('-m', '--method', action='store', nargs='?', choices=['bulk', 'row'],
                        help='-i or --insert option is required.')

    groups.add_argument('-u', '--update', action='store', nargs=2, metavar=('<start sep_col_val>', '<end sep_col_val>'),
                        help='models update')

    groups.add_argument('-d', '--delete', action='store', nargs=2, metavar=('<start sep_col_val>', '<end sep_col_val>'),
                        help='models delete.')

    parser.add_argument('-f', '--config', action='store', nargs='?', metavar='config_file_name',
                        const='default.ini',
                        help='view config file & select config file')

    groups.add_argument('-v', '--version', action='version', version='%(prog)s Ver.1.1',
                        help='프로그램 버전을 출력합니다.')

    args = parser.parse_args()

    # installer execution
    if args.install:

        os.chdir(os.path.join(os.path.curdir, 'conf'))
        print(os.path.abspath(os.path.curdir))

        if args.config is None:
            args.config = 'default.ini'

        Installer(args.config).installer()

    # insert execution
    elif args.insert:

        row_count = args.insert
        commit_unit = None
        insert_method = None

        if args.commit is not None:
            commit_unit = args.commit
        elif args.commit is None:
            commit_unit = 1000

        if args.method is not None:
            insert_method = args.method
        elif args.method is None:
            insert_method = 'bulk'

        if insert_method == 'bulk':
            print('BULK INSERT')
            print(args)
        elif insert_method == 'row':
            print("ROW INSERT")
            print(args)

    # commit execution
    elif args.commit:
        print("args.commit")
        print('--commit and --method option is required --insert option.')
        print(args)

    # method execution
    elif args.method:
        print("args.method")
        print('--commit and --method option is required --insert option.')
        print(args)

    # update execution
    elif args.update:
        print('update.')
        print(args.update[0])

    # delete execution
    elif args.delete:
        print('delete.')

    # view config execution
    elif args.config:

        os.chdir(os.path.join(os.path.curdir, 'conf'))

        config.set_config_load(args.config)

        # -f/--config 옵션을 제외한 다른 옵션이 없을 경우 해당 config 내용을 출력
        if args.commit is None and args.delete is None and args.insert == 0 and \
           args.install is False and args.method is None and args.update is None:

            # print(args)
            print(" ########## " + str(args.config) + " ########## \n" +
                  config.view_setting_config() + ' \n' +
                  config.view_connection_config() + ' \n' +
                  config.view_init_data_config())

    else:
        print("Invalid options or arguments. Please check your command.")
        print(args)


if __name__ == "__main__":
    main()