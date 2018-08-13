from sqlalchemy import String, Binary, LargeBinary, types as sqltypes
from sqlalchemy.ext.compiler import compiles


class VARCHAR2Byte(String):
    pass


@compiles(VARCHAR2Byte)
def complie_varchar2_byte(type_, compiler, **kw):
    type_len = type_.length
    return "VARCHAR2(%i BYTE)" % type_len


class LONGRAW(LargeBinary):
    pass


@compiles(LONGRAW)
def complie_long_raw(type_, complier, **kw):
    return "LONG RAW"


class UROWID(sqltypes.TypeEngine):
    pass


@compiles(UROWID)
def complie_urowid(type_, complier, **kw):
    return "UROWID"



