import datetime
import getopt
import sys
import time

from cdcbench.manage_data import *
from cdcbench.installer import *


def usage():
    print("Usage: cdcbench.py OPTION... [ARG1][ARG2][ARG3] \n"
          "Options: \n"
          "  -h, --help    \n"
          "         Print help message \n"
          "  -I, --installer \n"
          "         Initialize data creation or drop \n"
          "  -i, --insert \n"
          "         Data insert, insert row count (arg1) is essential, \n"
          "                      commit unit, default=1000 (arg2), "
          "                      separate_col start value (arg3) is optional \n"
          "  -u, --update \n"
          "         Data update, separate_col start value, default=1 (arg1), "
          "                      separate_col end value, default=200 (arg2) is optional \n"
          )


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:hi:s:u:I",
                                   ["help", "installer", "insert=", "update=", "delete=", "separate="])

    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(1)

    for opt, arg in opts:
        if opt == "-I" or opt == "--installer":

            print("\n"
                  "1. Create Object \n"
                  "2. Drop Object \n"
                  "0. Exit \n")

            select = int(input(">> Selection Operation: "))

            if select == 0:
                print("\n Installer Exit.")
                sys.exit(1)

            elif select == 1:
                create_table()
                print("\n Create Success.")

            elif select == 2:
                drop_table()
                print("\n Drop Success.")

        elif opt == "-i" or opt == "--insert":

            row_count = int(arg)
            sep_unit = None
            start_val = None

            if len(args) == 1:
                sep_unit = int(args[0])
            elif len(args) == 2:
                sep_unit = int(args[0])
                start_val = int(args[1])

            f = open('./insert_result.txt', 'a')

            if row_count > 0:

                start_time = time.time()    # 시간 측정 (초 단위)

                try:
                    if sep_unit is None:
                        insert_demo_test(row_count)
                    elif sep_unit is not None and start_val is None:
                        insert_demo_test(row_count, sep_unit)
                    elif sep_unit is not None and start_val is not None:
                        insert_demo_test(row_count, sep_unit, start_val)

                except Exception as error:
                    err = open('./error.log', 'a')
                    err.write("["+str(datetime.datetime.now())+"]\n")
                    err.write("Error: {} \n".format(error))
                    err.write("============================================\n")
                    raise

                end_time = time.time()
                print("\n Insert Data Success")
                print(" Running Time: %.02f sec." % (end_time-start_time))
                f.write("Running Time: %.02f sec.\n" % (end_time - start_time))

            else:
                print("Please input insert row count.")

        elif opt == "-u" or opt == "--update":

            start_val = int(arg)
            end_val = None

            if len(args) == 1:
                end_val = int(args[0])

            f = open('./update_result.txt', 'a')

            if start_val > 0:

                start_time = time.time()    # 시간 측정 (초 단위)

                try:
                    update_demo_test(start_val, end_val)

                except Exception as error:
                    err = open('./error.log', 'a')
                    err.write("["+str(datetime.datetime.now())+"]\n")
                    err.write("Error: {} \n".format(error))
                    err.write("============================================\n")
                    raise

                end_time = time.time()
                print("\n Update Data Success")
                print(" Running Time: %.02f sec." % (end_time-start_time))
                f.write("Running Time: %.02f sec.\n" % (end_time - start_time))

            else:
                # print("Please input insert row count.")
                pass

        elif opt == "-h" or opt == "--help":
            usage()


if __name__ == "__main__":
    main()