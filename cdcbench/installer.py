from .connection import *
from .manage_data import *
from .config_load import *
from models.oracle_models import *

import sys
import os


class Installer:

    def __init__(self, config_name):

        self.config = ConfigLoad(config_name)
        self.logger = LoggerManager().get_logger()

    def installer(self):

        print(self.config.view_connection_config() + "\n" +
              self.config.view_init_data_config() + "\n")

        while True:

            print(" 1. Create Object & Initialize Data \n"
                  " 2. Drop Object \n"
                  " 0. Exit \n")

            select = int(input(">> Select Operation: "))

            if select == 0:
                print(" Installer Exit.")
                return

            elif select == 1:
                print("select 1")
                break

            elif select == 2:
                print("select 2")
                break

            else:
                print(" Invalid option selected. Please select again. \n")
