
ORACLE = "ORACLE"
MYSQL = "MYSQL"
SQLSERVER = "SQLSERVER"
POSTGRESQL = "POSTGRESQL"
SUPPORT_DBMS_LIST = [ORACLE, MYSQL, SQLSERVER, POSTGRESQL]

# Sample Table Name
INSERT_TEST = "INSERT_TEST"
UPDATE_TEST = "UPDATE_TEST"
DELETE_TEST = "DELETE_TEST"
STRING_TEST = "STRING_TEST"
NUMERIC_TEST = "NUMERIC_TEST"
DATETIME_TEST = "DATETIME_TEST"
BINARY_TEST = "BINARY_TEST"
LOB_TEST = "LOB_TEST"
ORACLE_TEST = "ORACLE_TEST"
SQLSERVER_TEST = "SQLSERVER_TEST"

SOURCE = "SOURCE"
TARGET = "TARGET"
BOTH = "BOTH"

tqdm_ncols = 70
tqdm_bar_format = "  {desc}[{n}/{total}] {bar} [{percentage:3.0f}%]{postfix}"
tqdm_time_bar_format = "  {desc}[{n:.2f}/{total_fmt}] {bar} [{percentage:3.0f}%]{postfix}"
tqdm_bench_postfix = lambda rollback: f"{'Rollback' if rollback else 'Commit'} "

report_file_name = lambda now: f"ranbench_{now:%Y-%m-%d}.rep"
