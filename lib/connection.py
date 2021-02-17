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
            CUBRID: "cubrid.jdbc.driver.CUBRIDDriver",
            TIBERO: "com.tmax.tibero.jdbc.TbDriver"
        }

        self.dbms = conn_info.dbms
        self.driver = dialect_driver[conn_info.dbms]
        self.host = conn_info.host
        self.port = conn_info.port
        self.dbname = conn_info.dbname
        self.username = conn_info.username
        self.password = conn_info.password
        self.schema = conn_info.schema

        # if conn_info.dbms in sa_unsupported_dbms:
        #     self.engine = None
        #     self.db_session = None

        self.engine = None
        self.Session = None

        # else:
        if conn_info.dbms == ORACLE:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.dbname)
            conn_string = f"{self.driver}://{self.username}:{self.password}@{dsn}"
        elif conn_info.dbms == MYSQL:
            conn_string = f"{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/" \
                          f"{self.dbname}?charset=utf8mb4"
        elif conn_info.dbms == SQLSERVER:
            conn_string = f"{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/" \
                          f"{self.dbname}?driver=SQL+SERVER"
        else:
            conn_string = f"{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/" \
                          f"{self.dbname}"

        self.logger.debug(f"Connection String: {conn_string}")

        self.engine = create_engine(conn_string, convert_unicode=True, max_identifier_length=128)

        self.Session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))

    def sa_unsupported_get_connection(self) -> jaydebeapi.Connection:
        """
        SQLAlchemy 미지원 DBMS에서 connection을 맺을 때 사용
        :return: dbms별 connection
        """

        jar_file_name = {
            CUBRID: "JDBC-10.2-latest-cubrid.jar",
            TIBERO: "tibero6-jdbc.jar"
        }

        urls = {
            CUBRID: f"jdbc:CUBRID:{self.host}:{self.port}:{self.dbname}:public::?charSet=utf-8",
            TIBERO: f"jdbc:tibero:thin:@{self.host}:{self.port}:{self.dbname}"
        }

        try:
            self.logger.debug(
                f"driver={self.driver},"
                f"url={urls[self.dbms]}, "
                f"user={[self.username, self.password]}, "
                f"jar={os.path.join('lib', '../driver', self.dbms.lower(), jar_file_name[self.dbms])}"
            )

            return jaydebeapi.connect(
                self.driver,
                urls[self.dbms],
                [self.username, self.password],
                os.path.join("driver", self.dbms.lower(), jar_file_name[self.dbms])
            )

        except jpype.JVMNotFoundException as jvm_nfe:
            print_error(jvm_nfe.args[1])
        except jpype.JException as java_err:
            print_error(java_err.args[0])
