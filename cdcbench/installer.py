from cdcbench.connection import Base, engine


def create_table():
    Base.metadata.create_all(bind=engine)


def drop_table():
    Base.metadata.drop_all(bind=engine)


