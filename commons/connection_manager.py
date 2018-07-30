from commons.logger_manager import LoggerManager
from commons.config_manager import ConfigManager

from sqlalchemy import create_engine, sql
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import DatabaseError


MapperBase = declarative_base()


class ConnectionManager:

    def __init__(self, connection_string):

        try:
            self.config = ConfigManager.get_config()
            self.logger = LoggerManager.get_logger(__name__, self.config .log_level)

            self.logger.debug("Connection String: " + connection_string)

            self.logger.info("Create Engine")
            self.engine = create_engine(connection_string, convert_unicode=True)

            self.logger.info("Create DB Session")
            self.db_session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))

            query = sql.select([1])
            self.engine.execute(query)
            self.logger.info("DB Connection Test: Success")

            MapperBase.query = self.db_session.query_property()

        except DatabaseError as dberr:
            print()
            self.logger.error("DB Connection Test: Fail")
            self.logger.error(dberr.args[0])
            self.logger.error(dberr.statement)
            self.logger.error(dberr.params)
            raise
