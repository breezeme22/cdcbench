import os
import jaydebeapi
import jpype

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from lib.common import print_error, connection_string_value_check
from lib.globals import ORACLE, MYSQL, SQLSERVER, POSTGRESQL, CUBRID, TIBERO, sa_unsupported_dbms
from lib.logger import LoggerManager

# Import for type hinting
from lib.config import DatabaseConfig


class ConnectionManager:

    def __init__(self, conn_info: DatabaseConfig):

        self.logger = LoggerManager.get_logger(__name__)

        connection_string_value_check(conn_info)

        self.conn_info = conn_info

        dialect_driver = {
            ORACLE: "oracle+cx_oracle",
            MYSQL: "mysql",
            SQLSERVER: "mssql+pyodbc",
            POSTGRESQL: "postgresql+psycopg2",
        }

        self.driver = dialect_driver[conn_info.dbms]

        conn_str: str
        if conn_info.dbms == ORACLE:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(conn_info.host, conn_info.port, service_name=conn_info.dbname)
            conn_str = f"{self.driver}://{conn_info.username}:{conn_info.password}@{dsn}"
        elif conn_info.dbms == MYSQL:
            conn_str = (f"{self.driver}://{conn_info.username}:{conn_info.password}@{conn_info.host}:{conn_info.port}/"
                        f"{conn_info.dbname}?charset=utf8mb4")
        elif conn_info.dbms == SQLSERVER:
            conn_str = (f"{self.driver}://{conn_info.username}:{conn_info.password}@{conn_info.host}:{conn_info.port}/"
                        f"{conn_info.dbname}?driver=SQL+SERVER")
        else:   # PostgreSQL
            conn_str = (f"{self.driver}://{conn_info.username}:{conn_info.password}@{conn_info.host}:{conn_info.port}/"
                        f"{conn_info.dbname}")

        self.logger.debug(f"Connection String: {conn_str}")

        self.engine = create_engine(conn_str, convert_unicode=True, max_identifier_length=128)
        self.Session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))


class SAUnsupportedConnectionManager:

    def __init__(self, conn_info: DatabaseConfig):

        self.logger = LoggerManager.get_logger(__name__)

        connection_string_value_check(conn_info)

        self.conn_info = conn_info

        dialect_driver = {
            TIBERO: "com.tmax.tibero.jdbc.TbDriver",
            CUBRID: "cubrid.jdbc.driver.CUBRIDDriver"
        }

        self.driver = dialect_driver[conn_info.dbms]
        self.dbms = conn_info.dbms
        self.host = conn_info.host
        self.port = conn_info.port
        self.dbname = conn_info.dbname
        self.username = conn_info.username
        self.password = conn_info.password

        self.conn_str: str
        if self.dbms == TIBERO:
            self.conn_str = f"jdbc:tibero:thin:@{self.host}:{self.port}:{self.dbname}"
        else:   # CUBRID
            self.conn_str = f"jdbc:CUBRID:{self.host}:{self.port}:{self.dbname}:public::?charSet=utf-8"

    def connect(self) -> jaydebeapi.Connection:

        dialect_driver_files = {
            TIBERO: "tibero6-jdbc.jar",
            CUBRID: "JDBC-10.2-latest-cubrid.jar"
        }

        self.logger.debug(
            f"driver={self.driver}, "
            f"url={self.conn_str}, "
            f"user={[self.username, self.password]}, "
            f"jar={os.path.join('driver', self.dbms.lower(), dialect_driver_files[self.dbms])}"
        )

        try:

            return jaydebeapi.connect(
                self.driver,
                self.conn_str,
                [self.username, self.password],
                os.path.join("driver", self.dbms.lower(), dialect_driver_files[self.dbms])
            )

        except jpype.JVMNotFoundException as jvm_nfe:
            print_error(jvm_nfe.args[1])
        except jpype.JException as java_err:
            print_error(java_err.args[0])
