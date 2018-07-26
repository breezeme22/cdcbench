from commons.logger_manager import LoggerManager
from commons.config_manager import ConfigManager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

MapperBase = declarative_base()


class ConnectionManager:

    logger = None

    def __init__(self, connection_string):

        self.config = ConfigManager.get_config()
        self.logger = LoggerManager.get_logger(__name__, self.config .log_level)

        self.logger.debug("Connection String: " + connection_string)

        self.logger.info("Create Engine")
        self.engine = create_engine(connection_string, convert_unicode=True)
        self.logger.debug("Engine Check")

        self.logger.info("Create DB Session")
        self.db_session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))
        self.logger.debug("Engine Check")

        MapperBase.query = self.db_session.query_property()

    def get_engine(self):
        return self.engine

    def get_db_session(self):
        return self.db_session
