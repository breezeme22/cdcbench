from commons.mgr_logger import LoggerManager
from commons.mgr_config import ConfigManager
from commons.constants import *

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


OracleMapperBase = declarative_base()
MysqlMapperBase = declarative_base()
SqlserverMapperBase = declarative_base()
PostgresqlMapperBase = declarative_base()


class ConnectionManager:

    def __init__(self):

        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config.log_level)

        self.logger.debug("Source Connection String: " + self.config.get_src_conn_string())
        self.logger.debug("Target Connection String: " + self.config.get_trg_conn_string())

        self.logger.info("Create Source and Target Engine")
        self.src_engine = create_engine(self.config.get_src_conn_string(), convert_unicode=True)
        self.trg_engine = create_engine(self.config.get_trg_conn_string(), convert_unicode=True)

        self.logger.info("Create Source and Target DB Session")
        self.src_db_session = scoped_session(sessionmaker(autocommit=False, bind=self.src_engine))
        self.trg_db_session = scoped_session(sessionmaker(autocommit=False, bind=self.trg_engine))

    def get_src_mapper(self):

        src_dbms_type = self.config.source_dbms_type

        if src_dbms_type == dialect_driver[ORACLE]:
            OracleMapperBase.query = self.src_db_session.query_property()
            return OracleMapperBase

        elif src_dbms_type == dialect_driver[MYSQL]:
            MysqlMapperBase.query = self.src_db_session.query_property()
            return MysqlMapperBase

        elif src_dbms_type == dialect_driver[SQLSERVER]:
            SqlserverMapperBase.query = self.src_db_session.query_property()
            return SqlserverMapperBase

        elif src_dbms_type == dialect_driver[POSTGRESQL]:
            PostgresqlMapperBase.query = self.src_db_session.query_property()
            return PostgresqlMapperBase

    def get_trg_mapper(self):

        trg_dbms_type = self.config.target_dbms_type

        if trg_dbms_type == dialect_driver[ORACLE]:
            OracleMapperBase.query = self.trg_db_session.query_property()
            return OracleMapperBase

        elif trg_dbms_type == dialect_driver[MYSQL]:
            MysqlMapperBase.query = self.trg_db_session.query_property()
            return MysqlMapperBase

        elif trg_dbms_type == dialect_driver[SQLSERVER]:
            SqlserverMapperBase.query = self.trg_db_session.query_property()
            return SqlserverMapperBase

        elif trg_dbms_type == dialect_driver[POSTGRESQL]:
            PostgresqlMapperBase.query = self.src_db_session.query_property()
            return PostgresqlMapperBase
