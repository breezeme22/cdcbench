from sqlalchemy import String, Binary, LargeBinary, types as sqltypes
from sqlalchemy.ext.compiler import compiles


class VARCHAR2Byte(String):
    pass


@compiles(VARCHAR2Byte)
def complie_varchar2_byte(type_, compiler, **kw):
    type_len = type_.length
    return "VARCHAR2(%i BYTE)" % type_len


class INTERVALYearMonth(sqltypes.TypeEngine):

    def __init__(self,
                 year_precision=None):
        """Construct an INTERVAL.

            Note that only DAY TO SECOND intervals are currently supported.
            This is due to a lack of support for YEAR TO MONTH intervals
            within available DBAPIs (cx_oracle and zxjdbc).

            :param year_precision: the day precision value.  this is the number of
              digits to store for the day field.  Defaults to "2"

        """
        self.year_precision = year_precision


@compiles(INTERVALYearMonth)
def compile_interval_year_month(type_, compiler, **kw):
    return "INTERVAL YEAR{} TO MONTH".format(
        type_.year_precision is not None and
        "(%d)" % type_.year_precision or
        ""
    )


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



