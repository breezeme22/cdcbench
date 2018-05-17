from .connection import *
from .manage_data import *
from .config_load import ConfigLoad
# from .main import g_config
from models.oracle_models import *

import sys


class Installer:

    # def __init__(self, config_name='default.ini'):
    #     self.config = ConfigLoad(config_name)

    def installer(self):

        # print("\n" +
        #       g_config.view_connection_config() + "\n " +
        #       g_config.view_init_data_config() + "\n")

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