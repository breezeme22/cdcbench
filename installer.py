from connection import Base, engine


def create_table():
    import models
    Base.metadata.create_all(bind=engine)


def drop_table():
    import models
    Base.metadata.drop_all(bind=engine)


