#!/usr/bin/env python3

import argparse
import json
import os
import sys

from datetime import datetime
from typing import NoReturn

from lib.common import (CustomHelpFormatter, get_version, get_elapsed_time_msg, print_error,
                        DatabaseMetaData, print_end_msg)
from lib.config import ConfigManager
from lib.connection import ConnectionManager
from lib.logger import LoggerManager
from lib.definition import SADeclarativeManager


def cli() -> NoReturn:

    parser_main = argparse.ArgumentParser(prog="cdcbench", formatter_class=CustomHelpFormatter)
    parser_main.add_argument("--version", action="version", version=get_version())

    parser_cdcbench = argparse.ArgumentParser(add_help=False)

    parser_command = parser_main.add_subparsers(dest="command", metavar="<Command>", required=True)

    command_insert = parser_command.add_parser("insert", aliases=["i"], parents=[],
                                               )

def insert():
    pass


def update():
    pass


def delete():
    pass


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    cli()