from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

rdbms = config["db_connection"]["rdbms"]
user_id = config["db_connection"]["user_id"]
user_password = config["db_connection"]["user_password"]
host_name = config["db_connection"]["host_name"]
port = config["db_connection"]["port"]
instance_name = config["db_connection"]["instance_name"]

conn_info = rdbms + "://" + user_id + ":" + user_password + "@" + host_name + ":" + port + "/" + instance_name

# DB Connection 정보 입력.
engine = create_engine(conn_info, convert_unicode=True)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()