from source.installer import *
from source.manage_data import *

import datetime, getopt, sys, time, os


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
        opts, args = getopt.getopt(sys.argv[1:], "d:hi:u:It:",
                                   ["help", "installer", "insert=", "update=", "delete=", "time="])

    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(1)

    try:
        
        # DML 수행시간을 저장할 result 폴더가 없을 경우 생성
        if not os.path.exists("./result"):
            os.makedirs("./result")

        for opt, arg in opts:
            # Installer 실행 분기
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
            
            # insert 실행 분기
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
                    
                    # insert 함수 호출. 인자가 존재하는 3가지 경우를 분기로 처리
                    if commit_unit is None:
                        insert_test_core(row_count)
                    elif commit_unit is not None and start_val is None:
                        insert_test_core(row_count, commit_unit)
                    elif commit_unit is not None and start_val is not None:
                        insert_test_core(row_count, commit_unit, start_val)

                    end_time = time.time()

                    print("\n Data insert success.")
                    print(" Running time: %.02f sec." % (end_time-start_time))
                    f.write("Running time: %.02f sec.\n" % (end_time - start_time))

                else:
                    print("Invalid parameter. See the usage.")
            
            # update 실행 분기
            elif opt == "-u" or opt == "--update":

                start_val = int(arg)
                end_val = None

                if len(args) == 1:
                    end_val = int(args[0])

                f = open('./result/update_result.txt', 'a')

                if start_val >= 0 and end_val is not None:

                    start_time = time.time()    # 시간 측정 (초 단위)
                    
                    # update 함수 호출
                    update_test(start_val, end_val)

                    end_time = time.time()

                    print("\n Data update success.")
                    print(" Running time: %.02f sec." % (end_time-start_time))
                    f.write("Running time: %.02f sec.\n" % (end_time - start_time))

                else:
                    print("Invalid parameter. See the usage.")

            # delete 실행 분기
            elif opt == "-d" or opt == "--delete":

                start_val = int(arg)
                end_val = None

                if len(args) == 1:
                    end_val = int(args[0])

                f = open('./result/delete_result.txt', 'a')

                if start_val >= 0 and end_val is not None:

                    start_time = time.time()    # 시간 측정 (초 단위)

                    # delete 함수 호출
                    delete_test(start_val, end_val)

                    end_time = time.time()

                    print("\n Data delete success.")
                    print(" Running time: %.02f sec." % (end_time-start_time))
                    f.write("Running time: %.02f sec.\n" % (end_time - start_time))

                else:
                    print("Invalid parameter. See the usage.")

            # time_count insert 실행 분기
            elif opt == "-t" or opt == "--time":

                runtime = int(arg)
                count = None
                data_row = None

                if runtime > 0:

                    if len(args) == 1:
                        count = int(args[0])
                    elif len(args) == 2:
                        count = int(args[0])
                        data_row = int(args[1])

                if row_count > 0:

                    start_time = time.time()  # 시간 측정 (초 단위)

                    # insert 함수 호출. 인자가 존재하는 3가지 경우를 분기로 처리
                    if commit_unit is None:
                        insert_test_core(row_count)
                    elif commit_unit is not None and start_val is None:
                        insert_test_core(row_count, commit_unit)
                    elif commit_unit is not None and start_val is not None:
                        insert_test_core(row_count, commit_unit, start_val)

                    end_time = time.time()

                    print("\n Data insert success.")
                    print(" Running time: %.02f sec." % (end_time - start_time))
                    f.write("Running time: %.02f sec.\n" % (end_time - start_time))

                else:
                    print("Invalid parameter. See the usage.")


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