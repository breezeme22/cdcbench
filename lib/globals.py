
ORACLE = "ORACLE"
MYSQL = "MYSQL"
MARIADB = "MARIADB"
SQLSERVER = "SQLSERVER"
POSTGRESQL = "POSTGRESQL"
CUBRID = "CUBRID"
TIBERO = "TIBERO"
# cb (CB): cdcbench
cb_support_dbms = [ORACLE, MYSQL, MARIADB, SQLSERVER, POSTGRESQL, CUBRID, TIBERO]
# sa (SA): SQLAlchemy
sa_unsupported_dbms = [CUBRID, TIBERO]

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
sample_tables = [
    INSERT_TEST, UPDATE_TEST, DELETE_TEST,
    STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST,
    ORACLE_TEST, SQLSERVER_TEST
]

DEFAULT_CONFIG_FILE_NAME: str = "default.conf"

PRIMARY_KEY = "PRIMARY KEY"
UNIQUE = "UNIQUE"
NON_KEY = "NON KEY"

tqdm_ncols = 70
tqdm_bar_format = "  {desc}[{n}/{total}] {bar} [{percentage:3.0f}%]{postfix}"
tqdm_time_bar_format = "  {desc}[{n:.2f}/{total_fmt}] {bar} [{percentage:3.0f}%]{postfix}"
tqdm_bench_postfix = lambda rollback: f"{'Rollback' if rollback else 'Commit'} "

# End message
COMPLETE = "Complete"
COMMIT = "Commit"
ROLLBACK = "Rollback"
FAIL = "Fail"
