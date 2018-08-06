from sqlalchemy import String
from sqlalchemy.ext.compiler import compiles


class VARCHAR2Bytes(String):
    pass


@compiles(VARCHAR2Bytes)
def complie_varchar2_byte(type_, compiler, **kw):
    len = type_.length
    return "VARCHAR2(%i BYTE)" % len
