from commons.constants import *
from commons.mgr_logger import LoggerManager

from mappers.oracle_mappers import OracleMapperBase
from mappers.mysql_mappers import MysqlMapperBase
from mappers.sqlserver_mappers import SqlserverMapperBase
from mappers.postgresql_mappers import PostgresqlMapperBase

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class ConnectionManager:

    def __init__(self, config):

        self.config = config
        self.logger = LoggerManager.get_logger(__name__)

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

            # SQL Server의 경우 Table명 앞에 Schema명 붙임
            for table in SqlserverMapperBase.metadata.sorted_tables:
                table.schema = self.config.source_schema_name

            return SqlserverMapperBase

        elif src_dbms_type == dialect_driver[POSTGRESQL]:
            PostgresqlMapperBase.query = self.src_db_session.query_property()

            # PostgreSQL의 경우 Table명 앞에 Schema명 붙임
            for table in PostgresqlMapperBase.metadata.sorted_tables:
                table.schema = self.config.source_schema_name

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

            # SQL Server의 경우 Table명 앞에 Schema명 붙임
            for table in SqlserverMapperBase.metadata.sorted_tables:
                table.schema = self.config.taget_schema_name

            return SqlserverMapperBase

        elif trg_dbms_type == dialect_driver[POSTGRESQL]:
            PostgresqlMapperBase.query = self.src_db_session.query_property()

            # PostgreSQL의 경우 Table명 앞에 Schema명 붙임
            for table in PostgresqlMapperBase.metadata.sorted_tables:
                table.schema = self.config.taget_schema_name

            return PostgresqlMapperBase
