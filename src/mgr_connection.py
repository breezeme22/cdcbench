from src.constants import ORACLE, MYSQL, SQLSERVER, POSTGRESQL, CUBRID, TIBERO, sa_unsupported_dbms
from src.funcs_common import print_error_msg, exec_database_error
from src.mgr_logger import LoggerManager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class ConnectionManager:

    def __init__(self, conn_info):

        self.logger = LoggerManager.get_logger(__name__)
        self.log_level = LoggerManager.get_log_level()

        self.logger.debug("Call ConnectionManager")

        if conn_info["host_name"] == "" or conn_info["port"] == "" or conn_info["dbms_type"] == "" \
           or conn_info["db_name"] == "" or conn_info["user_name"] == "" or conn_info["user_password"] == "":
            print_error_msg("Not enough values are available to create the connection string. \n"
                            "  * Note. Please check the configuration file.")

        self.conn_info = conn_info
        self.conn_string = _get_conn_string(self.conn_info)

        self.logger.debug(f"Connection String: {self.conn_string}")

        if conn_info["dbms_type"] in sa_unsupported_dbms:
            self.engine = None
            self.db_session = None
        else:
            self.logger.info("Create Engine")
            self.engine = create_engine(self.conn_string, convert_unicode=True, max_identifier_length=128)

            self.logger.info("Create DB Session")
            self.db_session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))

        self.logger.debug("END Call ConnectionManager Class")

    def sa_unsupported_get_connection(self):
        """
        SQLAlchemy 미지원 DBMS에서 connection을 맺을 때 사용
        :return: dbms별 connection
        """

        if self.conn_info["dbms_type"] == CUBRID:
            import CUBRIDdb as cubrid
            try:
                return cubrid.connect(self.conn_string, self.conn_info["user_name"], self.conn_info["user_password"])
            except cubrid.DatabaseError as dberr:
                exec_database_error(self.logger, self.log_level, dberr)

        elif self.conn_info["dbms_type"] == TIBERO:
            import pyodbc
            try:
                return pyodbc.connect(self.conn_string)
            except pyodbc.DatabaseError as dberr:
                exec_database_error(self.logger, self.log_level, dberr)


def _get_conn_string(conn_info):
    """
    connection 정보에 관련된 값을 dbms별 connection string format에 맞게 변형하여 반환

    :return: DBMS별 Connection String
    """

    dialect_driver = {
        ORACLE: "oracle+cx_oracle",
        MYSQL: "mysql",
        SQLSERVER: "mssql+pyodbc",
        POSTGRESQL: "postgresql+psycopg2",
        CUBRID: "CUBRID",
        TIBERO: "Tibero 6 ODBC Driver"
    }

    driver = dialect_driver[conn_info["dbms_type"]]
    user_name = conn_info["user_name"]
    user_password = conn_info["user_password"]
    host_name = conn_info["host_name"]
    port = conn_info["port"]
    db_name = conn_info["db_name"]

    if conn_info["dbms_type"] == ORACLE:
        import cx_Oracle
        dsn = cx_Oracle.makedsn(host_name, port, service_name=db_name)
        return f"{driver}://{user_name}:{user_password}@{dsn}"
    elif conn_info["dbms_type"] == MYSQL:
        return f"{driver}://{user_name}:{user_password}@{host_name}:{port}/{db_name}?charset=utf8"
    elif conn_info["dbms_type"] == SQLSERVER:
        return f"{driver}://{user_name}:{user_password}@{host_name}:{port}/{db_name}?driver=SQL+SERVER"
    elif conn_info["dbms_type"] == CUBRID:
        return f"{driver}:{host_name}:{port}:{db_name}:::"
    elif conn_info["dbms_type"] == TIBERO:
        return f"DRIVER={{{driver}}};SERVER={host_name};PORT={port};DB={db_name};UID={user_name};PWD={user_password};"
    else:
        return f"{driver}://{user_name}:{user_password}@{host_name}:{port}/{db_name}"
