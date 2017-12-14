from source.connection import Base, engine
from source.manage_data import *
from source.models import *


# Installer Table 및 데이터 생성
def create_table():
    Base.metadata.create_all(bind=engine)
    print("\n All Object Created.")
    data_init(UpdateTest, 300000)
    print(" Update test table data was created.")
    data_init(DeleteTest, 300000)
    print(" Delete test table data was created.")
    print("\n Create Success.")


# 모든 Table 및 관련 데이터 모두 삭제
def drop_table():
    Base.metadata.drop_all(bind=engine)
    print("\n All Object Deleted."
          "\n Drop Success.")