from src.constants import ORACLE, MYSQL, SQLSERVER, POSTGRESQL
from src.funcs_common import print_error_msg
from src.mgr_logger import LoggerManager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

import cx_Oracle


class ConnectionManager:

    def __init__(self, conn_info):

        self.logger = LoggerManager.get_logger(__name__)

        self.logger.debug("Call ConnectionManager")

        if conn_info["host_name"] == "" or conn_info["port"] == "" or conn_info["dbms_type"] == "" \
           or conn_info["db_name"] == "" or conn_info["user_id"] == "" or conn_info["user_password"] == "":
            print_error_msg("Not enough values are available to create the connection string. \n"
                            "  * Note. Please check the configuration file.")

        self.connection_info = conn_info
        conn_string = _get_conn_string(self.connection_info)

        self.logger.debug(f"Connection String: {conn_string}")

        self.logger.info("Create Engine")
        self.engine = create_engine(conn_string, convert_unicode=True)

        self.logger.info("Create DB Session")
        self.db_session = scoped_session(sessionmaker(autocommit=False, bind=self.engine))

        self.logger.debug("END Call ConnectionManager Class")


def _get_conn_string(conn_info):
    """
    connection 정보에 관련된 값을 SQLAlchemy connection string format에 맞게 변형하여 반환하는 함수

    :return: SQLAlchemy에서 사용되는 DB Connection String을 return
    """

    dialect_driver = {
        ORACLE: "oracle+cx_oracle",
        MYSQL: "mysql",
        SQLSERVER: "mssql+pyodbc",
        POSTGRESQL: "postgresql+psycopg2"
    }

    if conn_info["dbms_type"] == ORACLE:

        driver = dialect_driver[conn_info["dbms_type"]]
        user_id = conn_info["user_id"]
        user_password = conn_info["user_password"]
        dsn = cx_Oracle.makedsn(conn_info["host_name"], conn_info["port"], service_name=conn_info["db_name"])

        return f"{driver}://{user_id}:{user_password}@{dsn}"

    elif conn_info["dbms_type"] == MYSQL:

        driver = dialect_driver[conn_info["dbms_type"]]
        user_id = conn_info["user_id"]
        user_password = conn_info["user_password"]
        host_name = conn_info["host_name"]
        port = conn_info["port"]
        db_name = conn_info["db_name"]

        return f"{driver}://{user_id}:{user_password}@{host_name}:{port}/{db_name}?charset=utf8"

    elif conn_info["dbms_type"] == SQLSERVER:

        driver = dialect_driver[conn_info["dbms_type"]]
        user_id = conn_info["user_id"]
        user_password = conn_info["user_password"]
        host_name = conn_info["host_name"]
        port = conn_info["port"]
        db_name = conn_info["db_name"]

        return f"{driver}://{user_id}:{user_password}@{host_name}:{port}/{db_name}?driver=SQL+SERVER"

    else:

        driver = dialect_driver[conn_info["dbms_type"]]
        user_id = conn_info["user_id"]
        user_password = conn_info["user_password"]
        host_name = conn_info["host_name"]
        port = conn_info["port"]
        db_name = conn_info["db_name"]

        return f"{driver}://{user_id}:{user_password}@{host_name}:{port}/{db_name}"
