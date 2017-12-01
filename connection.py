from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('oracle://testsc:testsc@172.16.2.44:1521/ORA11', convert_unicode=True)
# engine = create_engine('oracle://test:test@192.168.56.21:1521/active', convert_unicode=True)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()