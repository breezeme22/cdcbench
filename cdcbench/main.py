import logging
import sys
import time
import os
import argparse

from datetime import datetime

from cdcbench.installer import *
from cdcbench.manage_data import *
from cdcbench.config_load import *


def main():

    config = ConfigLoad()
    logger = LoggerManager().get_logger()

    # CLI argument parsing
    help_formatter = lambda prog: argparse.RawTextHelpFormatter(prog, max_help_position=8)

    parser = argparse.ArgumentParser(prog='cdcbench', usage='%(prog)s [options]',
                                     formatter_class=help_formatter)

    groups = parser.add_mutually_exclusive_group()

    groups.add_argument('-n', '--install', action="store_true",
                        help='create Object & Init models')

    groups.add_argument('-i', '--insert', action='store', metavar='<Data rows>', type=int, default=0,
                        help='models insert')

    parser.add_argument('-c', '--commit', action='store', metavar='[commit units]', type=int,
                        help='-i or --insert option is required. commit unit value input.')

    parser.add_argument('-m', '--method', action='store', choices=['bulk', 'row'],
                        help='-i or --insert option is required.')

    groups.add_argument('-u', '--update', action='append', nargs=2, metavar=('<start sep_col_val>', '<end sep_col_val>'),
                        help='models update')

    groups.add_argument('-d', '--delete', action='store', nargs=2, metavar=('<start sep_col_val>', '<end sep_col_val>'),
                        help='models delete.')

    parser.add_argument('-f', '--config', action='store', nargs='?', metavar='[config_file_name]',
                        const='conf/config.ini',
                        help='view config file & select config file')

    groups.add_argument('-v', '--version', action='version', version='%(prog)s Ver.1.1',
                        help='print program version.')

    args = parser.parse_args()

    # installer execution
    if args.install:

        print()
        # print(config.view_connection_config())

        while True:

            print("1. Create Object & Initialize Data \n"
                  "2. Drop Object \n"
                  "0. Exit \n")

            select = int(input(">> Select Operation: "))

            if select == 0:
                print(" Installer Exit.")
                sys.exit(1)

            elif select == 1:
                print("select 1")
                break

            elif select == 2:
                print("select 2")
                break

            else:
                print(" Invalid option selected. Please select again. \n")

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
            print('BBBBBBULLLLLKKKK')
            logger.info('BBBBBBULLLLLKKKK')
            print(args)
        elif insert_method == 'row':
            print("RRRRRRRRROWOWOWOWOOWOWOW")
            logger.info('BBBBBBULLLLLKKKK')
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

        config = ConfigLoad(args.config)

        print(args)
        print(" " +
              config.view_cdcbench_setting_config() + ' \n ' +
              config.view_connection_config() + ' \n ' +
              config.view_init_data_config())

    else:
        print("Invalid options or arguments. Please check your command.")
        print(args)


if __name__ == "__main__":
    main()