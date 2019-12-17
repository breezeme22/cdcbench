
ORACLE = "ORACLE"
MYSQL = "MYSQL"
SQLSERVER = "SQLSERVER"
POSTGRESQL = "POSTGRESQL"

dialect_driver = {
    ORACLE: "oracle+cx_oracle",
    MYSQL: "mysql",
    SQLSERVER: "mssql+pymssql",
    POSTGRESQL: "postgresql+psycopg2"
}

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
tqdm_bar_format = "  [{n_fmt}/{total_fmt}] {bar} [{percentage:3.0f}%]"

