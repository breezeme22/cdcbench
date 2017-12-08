from source.connection import Base, engine
from source.manage_data import *
from source.models import *


def create_table():
    Base.metadata.create_all(bind=engine)
    print("\n All Object Created.")
    data_init(UpdateTest)
    print(" Update Test Table Data Created.")
    # data_init(DeleteTest)
    print(" Delete Test Table Data Created.")
    print("\n Create Success.")


def drop_table():
    Base.metadata.drop_all(bind=engine)
    print("\n"
          " All Object Deleted."
          "\n Drop Success.")


