from commons.constants import ORACLE, MYSQL, SQLSERVER, POSTGRESQL, CUBRID, TIBERO, sa_unsupported_dbms
from commons.funcs_common import print_error_msg
from commons.mgr_logger import LoggerManager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

import os


class ConnectionManager:

    def __init__(self, conn_info):

        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.logger.debug("Call ConnectionManager")

        if conn_info["host_name"] == "" or conn_info["port"] == "" or conn_info["dbms_type"] == "" \
           or conn_info["db_name"] == "" or conn_info["user_name"] == "" or conn_info["user_password"] == "":
            print_error_msg("Not enough values are available to create the connection string. \n"
                            "  * Note. Please check the configuration file.")

        dialect_driver = {
            ORACLE: "oracle+cx_oracle",
            MYSQL: "mysql",
            SQLSERVER: "mssql+pyodbc",
            POSTGRESQL: "postgresql+psycopg2",
            CUBRID: "cubrid.jdbc.driver.CUBRIDDriver",
            TIBERO: "com.tmax.tibero.jdbc.TbDriver"
        }

        self.dbms_type = conn_info["dbms_type"]
        self.driver = dialect_driver[conn_info["dbms_type"]]
        self.host_name = conn_info["host_name"]
        self.port = conn_info["port"]
        self.db_name = conn_info["db_name"]
        self.user_name = conn_info["user_name"]
        self.user_password = conn_info["user_password"]
        self.schema_name = conn_info["schema_name"]

        if conn_info["dbms_type"] in sa_unsupported_dbms:
            self.engine = None
            self.db_session = None

        else:
            if conn_info["dbms_type"] == ORACLE:
                import cx_Oracle
                dsn = cx_Oracle.makedsn(self.host_name, self.port, service_name=self.db_name)
                conn_string = f"{self.driver}://{self.user_name}:{self.user_password}@{dsn}"
            elif conn_info["dbms_type"] == MYSQL:
                conn_string = f"{self.driver}://{self.user_name}:{self.user_password}@{self.host_name}:{self.port}/" \
                              f"{self.db_name}?charset=utf8"
            elif conn_info["dbms_type"] == SQLSERVER:
                conn_string = f"{self.driver}://{self.user_name}:{self.user_password}@{self.host_name}:{self.port}/" \
                              f"{self.db_name}?driver=SQL+SERVER"
            else:
                conn_string = f"{self.driver}://{self.user_name}:{self.user_password}@{self.host_name}:{self.port}/" \
                              f"{self.db_name}"

            self.logger.debug(f"Connection String: {conn_string}")

            self.logger.info("Create Engine")
            self.engine = create_engine(conn_string, convert_unicode=True, max_identifier_length=128)

            self.logger.info("Create DB Session")
            self.db_session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))

        self.logger.debug("END Call ConnectionManager Class")

    def sa_unsupported_get_connection(self):
        """
        SQLAlchemy 미지원 DBMS에서 connection을 맺을 때 사용
        :return: dbms별 connection
        """

        import jaydebeapi
        import jpype

        jar_file_name = {
            CUBRID: "JDBC-10.2-latest-cubrid.jar",
            TIBERO: "tibero6-jdbc.jar"
        }

        urls = {
            CUBRID: f"jdbc:CUBRID:{self.host_name}:{self.port}:{self.db_name}:public::?charSet=utf-8",
            TIBERO: f"jdbc:tibero:thin:@{self.host_name}:{self.port}:{self.db_name}"
        }

        try:
            self.logger.debug(
                f"driver={self.driver},"
                f"url={urls[self.dbms_type]}, "
                f"user={[self.user_name, self.user_password]}, "
                f"jar={os.path.join('commons', 'driver', self.dbms_type.lower(), jar_file_name[self.dbms_type])}"
            )

            return jaydebeapi.connect(
                self.driver,
                urls[self.dbms_type],
                [self.user_name, self.user_password],
                os.path.join("commons", "driver", self.dbms_type.lower(), jar_file_name[self.dbms_type])
            )

        except jpype.JVMNotFoundException as jvm_nfe:
            print_error_msg(jvm_nfe.args[1])
        except jpype.JException as java_err:
            print_error_msg(java_err.args[0])
