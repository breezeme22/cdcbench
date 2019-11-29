from commons.constants import ORACLE, MYSQL, SQLSERVER, POSTGRESQL, dialect_driver
from commons.mgr_logger import LoggerManager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

import cx_Oracle


class ConnectionManager:

    def __init__(self, conn_info):

        self.logger = LoggerManager.get_logger(__name__)

        self.logger.debug("Call ConnectionManager Class")
        self.connection_info = conn_info
        conn_string = _get_conn_string(self.connection_info)

        self.logger.debug("Connection String: " + conn_string)

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
    if conn_info["dbms_type"] == ORACLE:
        dsn = cx_Oracle.makedsn(conn_info["host_name"], conn_info["port"], service_name=conn_info["db_name"])
        return dialect_driver[conn_info["dbms_type"]] + "://" + conn_info["user_id"] + ":" + conn_info["user_password"] + "@" + dsn
    elif conn_info["dbms_type"] == MYSQL:
        return dialect_driver[conn_info["dbms_type"]] + "://" + conn_info["user_id"] + ":" + conn_info["user_password"] + "@" + \
               conn_info["host_name"] + ":" + conn_info["port"] + "/" + conn_info["db_name"] + "?charset=utf8"
    else:
        return dialect_driver[conn_info["dbms_type"]] + "://" + conn_info["user_id"] + ":" + conn_info["user_password"] + "@" + \
               conn_info["host_name"] + ":" + conn_info["port"] + "/" + conn_info["db_name"]