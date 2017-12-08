from source.installer import *
from source.manage_data import *

import datetime, getopt, sys, time


def usage():
    print("Usage: cdcbench OPTION... [ARG1][ARG2][ARG3] \n"
          "Options: \n"
          "  -h, --help    \n"
          "         Print help message \n"
          "  -I, --installer \n"
          "         Initialize data creation or drop \n"
          "  -i, --insert \n"
          "         Data insert, insert row count (arg1) is essential, \n"
          "                      commit unit, default=1000 (arg2), \n"
          "                      separate_col start value, default=1 (arg3) is optional \n"
          "  -u, --update \n"
          "         Data update, separate_col start value (arg1), \n"
          "                      separate_col end value (arg2) is essential \n"
          "  -d, --delete \n"
          "         Data delete, separate_col start value (arg1), \n"
          "                      separate_col end value (arg2) is essential \n"
          )


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:hi:s:u:I",
                                   ["help", "installer", "insert=", "update=", "delete=", "separate="])

    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(1)

    try:
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

                elif select == 2:
                    drop_table()

            elif opt == "-i" or opt == "--insert":

                row_count = int(arg)
                commit_unit = None
                start_val = None

                if len(args) == 1:
                    commit_unit = int(args[0])
                elif len(args) == 2:
                    commit_unit = int(args[0])
                    start_val = int(args[1])

                f = open('./result/insert_result.txt', 'a')

                if row_count > 0:

                    start_time = time.time()    # 시간 측정 (초 단위)

                    if commit_unit is None:
                        insert_test_core(row_count)
                    elif commit_unit is not None and start_val is None:
                        insert_test_core(row_count, commit_unit)
                    elif commit_unit is not None and start_val is not None:
                        insert_test_core(row_count, commit_unit, start_val)

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

                f = open('./result/update_result.txt', 'a')

                if start_val > 0:

                    start_time = time.time()    # 시간 측정 (초 단위)

                    # update_test(start_val, end_val)
                    update_test_core(start_val, end_val)

                    end_time = time.time()

                    print("\n Update Data Success")
                    print(" Running Time: %.02f sec." % (end_time-start_time))
                    f.write("Running Time: %.02f sec.\n" % (end_time - start_time))

                else:
                    # print("Please input insert row count.")
                    pass

            elif opt == "-d" or opt == "--delete":

                start_val = int(arg)
                end_val = None

                if len(args) == 1:
                    end_val = int(args[0])

                f = open('./result/delete_result.txt', 'a')

                if start_val > 0:

                    start_time = time.time()    # 시간 측정 (초 단위)

                    delete_test(start_val, end_val)
                    # delete_test_core(start_val, end_val)

                    end_time = time.time()

                    print("\n Delete Data Success")
                    print(" Running Time: %.02f sec." % (end_time-start_time))
                    f.write("Running Time: %.02f sec.\n" % (end_time - start_time))

                else:
                    # print("Please input insert row count.")
                    pass

            elif opt == "-h" or opt == "--help":
                usage()

    except Exception as error:
        err = open('./error.log', 'a')
        err.write("[" + str(datetime.datetime.now()) + "]\n")
        err.write("Error: {} \n".format(error))
        err.write("============================================\n")
        raise


if __name__ == "__main__":
    main()