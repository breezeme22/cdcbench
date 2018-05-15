from .connection import *
from .manage_data import *
from models.oracle_models import *

import configparser

config = configparser.ConfigParser()
config.read('../conf/config.ini')

# update_data = int(config["init_data"]["update_test_data"])
# update_commit = int(config["init_data"]["update_data_commit_unit"])
# delete_data = int(config["init_data"]["delete_test_data"])
# delete_commit = int(config["init_data"]["delete_data_commit_unit"])


# Installer Table 및 데이터 생성
def create_table():
    Base.metadata.create_all(bind=engine)
    print("\n All Object Created.")
    #data_init(UpdateTest, update_data, update_commit)
    print(" Update test table models was created.")
    #data_init(DeleteTest, delete_data, delete_commit)
    print(" Delete test table models was created.")
    print("\n Create Success.")


# 모든 Table 및 관련 데이터 모두 삭제
def drop_table():
    #Base.metadata.drop_all(bind=engine)
    print("\n All Object Deleted."
          "\n Drop Success.")

