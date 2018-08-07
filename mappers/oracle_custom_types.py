from sqlalchemy import String
from sqlalchemy.ext.compiler import compiles


class VARCHAR2Byte(String):
    pass


@compiles(VARCHAR2Byte)
def complie_varchar2_byte(type_, compiler, **kw):
    type_len = type_.length
    return "VARCHAR2(%i BYTE)" % type_len
