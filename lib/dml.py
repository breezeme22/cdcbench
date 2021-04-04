
from dataclasses import dataclass, field
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import Table, Column
from sqlalchemy.sql.expression import func, bindparam, text
from typing import Any, TextIO, Dict, Union, Optional

from lib.common import get_separate_col_val, print_error, proc_database_error, DatabaseWorkInfo
from lib.globals import tqdm_bar_format, tqdm_ncols, tqdm_bench_postfix, sample_tables
from lib.logger import LoggerManager


@dataclass
class DMLDetail:
    insert: int = 0
    update: int = 0
    delete: int = 0


@dataclass
class DMLSummary:
    total_record: int = 0
    dml_count: int = 0
    detail: Dict[str, DMLDetail] = field(default_factory=dict)


@dataclass
class TCLSummary:
    tcl_count: int = 0
    commit: int = 0
    rollback: int = 0


@dataclass
class ResultSummary:
    dml: DMLSummary = field(default_factory=DMLSummary)
    tcl: TCLSummary = field(default_factory=TCLSummary)


class DML:

    def __init__(self, db_meta: DatabaseWorkInfo):

        self.logger = LoggerManager.get_logger(__file__)
        self.engine = db_meta.engine
        self.dbms = db_meta.conn_info.dbms

    def multi_insert(self):
        pass



