from commons.mgr_logger import LoggerManager
from commons.mgr_config import ConfigManager

from sqlalchemy import create_engine, sql
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


MapperBase = declarative_base()


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

        MapperBase.query = self.src_db_session.query_property()
