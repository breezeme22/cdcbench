from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .main import g_config


class Connection:

    # DB Connection 정보 입력.
    engine = create_engine(g_config.get_connection_info(), convert_unicode=True)
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

    Base = declarative_base()
    Base.query = db_session.query_property()

