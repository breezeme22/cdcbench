CDCBENCH
========
CDCBENCH는 Python 기반의 ArkCDC Project 테스트 프로그램입니다. 

> 현재 문서에서 CDCBENCH (upper)와 cdcbench (lower)가 혼용되어 사용되고 있습니다. <br>
> 대문자의 경우 프로젝트로, 소문자의 경우 기능의 명칭으로 사용되고 있습니다.

## 0. Supported Environment
* Windows
* Linux

## 1. Installation
### 1.1. Python Install
CDCBENCH는 다음의 Python 버전에서 동작합니다.
 
Version >= **3.6.3** (https://www.python.org/downloads/)

> 환경별 Python 설치방법은 검색을 활용해주시기 바랍니다. <br>

### 1.2. Database Client Install
CDCBENCH 실행에 필요한 Database별 Client 설치는 다음과 같은 절차를 따릅니다. <br>
> CDCBENCH가 설치될 PC 또는 Server에 해당 DBMS가 설치되어 있을 경우에는 Client 설치가 필요 없습니다.

#### 1.2.1. Oracle

1. 다음의 경로에서 OS에 맞는 대상 Database와 동일한 버전의 "Basic" Package를 다운로드합니다. <br>
   https://www.oracle.com/database/technologies/instant-client/downloads.html

##### &nbsp;&nbsp;&nbsp; Windows
1. 다운로드한 zip file을 적절한 위치에 압축해제 합니다. (ex. C:\util\oracle_instantclient_11_2)

2. 시스템 환경변수 "Path"에 압축해제한 경로를 추가합니다.

##### &nbsp;&nbsp;&nbsp; Linux
1. 다운로드한 zip file을 적절한 위치에 압축해제 합니다. 

2. profile의 LD_LIBRARY_PATH 환경변수에 압축해제한 경로를 추가합니다.

#### 1.2.2. MySQL

##### &nbsp;&nbsp;&nbsp; Linux (RHEL / CentOS)
1. root 계정에서 RPM을 통해 리눅스 배포판과 Database 버전에 맞는 Repository 설정 RPM 파일을 다운로드 받습니다.
   <pre>
   ex) rpm -ivh https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm
   </pre>
   
2. yum을 통해 MySQL Database와 관련한 패키지들을 설치할 수 있으며, 다음의 패키지 설치가 필요합니다.
   <pre>
   yum install -y mysql-community-devel
   </pre>

#### 1.2.3. SQL Server

##### &nbsp;&nbsp;&nbsp; Linux (RHEL / CentOS)
> 참고. https://docs.microsoft.com/ko-kr/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15#redhat-enterprise-server-and-oracle-linux

<pre>
#Download appropriate package for the OS version
#Choose only ONE of the following, corresponding to your OS version

#RedHat Enterprise Server 7
curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo

# 필요에 따라서 수행
# yum remove unixODBC-utf16 unixODBC-utf16-devel #to avoid conflicts

# ODBC Driver 설치
ACCEPT_EULA=Y yum install msodbcsql17

# ODBC 설정 CDCBENCH에 맞게 변경
# Driver 설치 후 설정된 Driver 이름 (ex. ODBC Driver 17 for SQL Server)을 주석처리하고, 지정된 이름으로 변경 (SQL Server)
vi /etc/odbcinst.ini
...
#[ODBC Driver 17 for SQL Server]  
[SQL Server]  
Description=Microsoft ODBC Driver 17 for SQL Server
Driver=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.5.so.2.1
UsageCount=1
...
</pre>

### 1.3. CDCBENCH Download
https://lab.idatabank.com/gitlab/sangcheolpark/cdcbench/tags
> Line Separator (CRLF, LF)와 관련해 실행에 문제가 있을 수 있으니, 실제 사용될 OS에서 다운로드 해주시기 바랍니다.


### 1.4. CDCBENCH Install
제공되는 압축파일을 해제한 후 Library 설치 스크립트 수행 후 설치가 완료됩니다. <br>
생성되는 디렉토리 구조는 다음과 같습니다.
<pre>
cdcbench ($CDCBENCH_HOME)
├─ bin              | 실행 파일이 저장된 디렉토리
├─ commons          | 기능을 구현한 python file이 저장된 디렉토리
├─ conf             | configuration file을 저장하는 디렉토리
├─ data             | 샘플 데이터 파일이 저장된 디렉토리
├─ definitions      | 테이블 구조를 저장하는 디렉토리
├─ install          | CDCBENCH 설치 스크립트 및 필요 파일이 저장된 디렉토리
└─ README.md        | README.md
</pre>
> 이후 설치경로를 편의상 CDCBENCH_HOME 이라고 칭합니다.

CDCBENCH 실행에 필요한 라이브러리를 다음의 절차로 설치합니다.
<pre>
> cd cdcbench/install
> py install_cdcbench
</pre>

<hr>

## 2. Configuration

### 2.1. Configuration File
* 모든 Configuration 파일은 **conf** 디렉토리에 저장되어야 합니다. 
* *--config* 옵션을 사용하지 않을 경우 기본적으로 **default.conf**을 인식합니다.
* *--config* 옵션을 통해 module 실행 시 configuration 파일을 변경할 수 있습니다.  

### 2.2. Configuration
Configuration은 크게 CDCBENCH 관련 설정 / Database 정보 / 초기화 데이터 설정으로 구분할 수 있습니다. <br>
정의 형식은 **Parameter = Value** 형식입니다. 모든 값은 생략 가능합니다. <br>
&nbsp;&nbsp; * 강조 표시된 값은 각 파라미터의 기본값입니다. 

* **[SETTING]**
  > CDCBENCH 동작과 관련된 파라미터로 다음과 같은 파라미터들을 가지고 있습니다.

  * LOG_LEVEL =  [ **ERROR** | INFO | DEBUG ] <br> 
    ::: 출력할 Log Level을 지정합니다.
  * SQL_LOGGING =  [ **None** | SQL | ALL ] 
    * None: SQL 관련 로깅을 남기지 않습니다.
    * SQL: 실행되는 SQL을 로깅합니다. 
    * ALL: 실행되는 SQL과 데이터를 로깅합니다.
  * NLS_LANG =  [ *$NLS_LANG* ]  &nbsp; (Default. **AMERICAN_AMERICA.AL32UTF8**) <br>
    ::: Oracle Server의 Character Set입니다. profile의 $NLS_LANG 값과 동일하게 입력하면 됩니다.
  <br>
* **[SOURCE(TARGET)_DATABASE]**
  > 사용할 데이터베이스의 연결정보를 입력합니다.
  >  * target_database 영역은 initializer 기능에서만 사용됩니다. 그 외의 cdcbench, ranbench 수행 시에는 source_database 정보만을 사용합니다. 

  * HOST_NAME = [ *IPv4 Format* ] <br> 
    ::: 연결할 데이터베이스의 IP를 입력합니다.
  * PORT = [ *1 ~ 65535* ] <br>
    ::: 연결할 데이터베이스와 통신할 Port를 입력합니다.
  * DBMS_TYPE = [ oracle | mysql | sqlserver | postgresql | cubrid | tibero ] <br> 
    ::: 연결할 데이터베이스의 타입을 입력합니다.
  * DB_NAME = [ *database name (instance_name)* ] <br> 
    ::: 연결할 데이터베이스의 이름을 입력합니다.
  * SCHEMA_NAME = [ *schema name* ] <br> 
    ::: 오브젝트를 소유한 스키마의 이름을 입력합니다. sqlserver, postgresql에서만 사용됩니다. 
  * USER_NAME = [ *user name* ] <br>
    ::: CDCBENCH를 사용할 데이터베이스 유저를 입력합니다.
  * USER_PASSWORD = [ *user password* ] <br>
    ::: CDCBENCH를 사용할 데이터베이스 유저의 패스워드를 입력합니다.
  <br>
* **[initial_update(delete)_test_data]**
  > CDCBENCH 초기화(initializer --create, --reset) 시, update_test (delete_test) 테이블에 생성할 데이터의 양과 Commit 단위를 지정합니다.
  
  * number_of_data = [ *number >= 1* ] (Default. **30000**)<br>
    ::: 테이블에 생성할 총 데이터의 양을 지정합니다. 
  * commit_unit = [ *number >= 1* ] (Default. **2000**) <br> 
    ::: 데이터 생성시 commit이 발생하는 기준을 지정합니다.
  
<hr>

## 3. Table Information
CDCBENCH에서 사용되는 테이블은 옵션에 따라 사용되는 테이블이 구분됩니다.

#### 3.1. INSERT_TEST / UPDATE_TEST / DELETE_TEST
| # | Column Name  | Oracle       | SQL Server  | MySQL       | PostgreSQL  | CUBRID      | Tibero       | 비고                                             |
|---|--------------|--------------|-------------|-------------|-------------|-------------|--------------|-------------------------------------------------|
| 1 | T_ID         | NUMBER       | INT         | INT         | INTEGER     | INT         | NUMBER       | Sequence(INSERT_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_NAME     | VARCHAR2(50) | VARCHAR(50) | VARCHAR(50) | VARCHAR(50) | VARCHAR(50) | VARCHAR2(50) |                                                 |
| 3 | COL_DATE     | DATE         | DATETIME    | DATETIME    | TIMESTAMP   | DATETIME    | DATE         |                                                 |
| 4 | SEPARATE_COL | NUMBER       | INT         | INT         | INTEGER     | INT         | NUMBER       |                                                 |
<br>

#### 3.2. STRING_TEST 

| # | Column Name   | Oracle              | SQL Server     | MySQL         | PostgreSQL    | CUBRID        | Tibero              | 비고                                             |
|---|---------------|---------------------|----------------|---------------|---------------|---------------|---------------------|-------------------------------------------------|
| 1 | T_ID          | NUMBER              | INT            | INT(11)       | INTEGER       | INT           | NUMBER              | Sequence(STRING_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_CHAR      | CHAR(50)            | CHAR(50)       | CHAR(50)      | CHAR(50)      | CHAR(50)      | CHAR(50)            |                                                 |
| 3 | COL_NCHAR     | NCHAR(50)           | NCHAR(50)      | CHAR(50)      | CHAR(50)      | CHAR(50)      | NCHAR(50)           |                                                 |
| 4 | COL_VARCHAR_B | VARCHAR2(4000)      | VARCHAR(4000)  | VARCHAR(4000) | VARCHAR(4000) | VARCHAR(4000) | VARCHAR2(4000)      |                                                 |
| 5 | COL_VARCHAR_C | VARCHAR2(4000 CHAR) | VARCHAR(4000)  | VARCHAR(4000) | VARCHAR(4000) | VARCHAR(4000) | VARCHAR2(4000 CHAR) |                                                 |
| 6 | COL_NVARCHAR  | NVARCHAR2(2000)     | NVARCHAR(2000) | VARCHAR(2000) | VARCHAR(2000) | VARCHAR(2000) | NVARCHAR2(2000)     |                                                 |
| 7 | COL_TEXT      | LONG                | VARCHAR(MAX)   | LONGTEXT      | TEXT          | STRING        | LONG                |                                                 |
<br>

#### 3.3. NUMERIC_TEST

| #  | Column Name    | Oracle        | SQL Server      | MySQL               | PostgreSQL       | CUBRID          | Tibero        | 비고                                              |
|----|----------------|---------------|-----------------|---------------------|------------------|-----------------|---------------|--------------------------------------------------|
| 1  | T_ID           | NUMBER        | INT             | INT(11)             | INTEGER          | INT             | NUMBER        | Sequence(NUMERIC_TEST_SEQ, Start=1, Increment=1) |
| 2  | COL_BIT        | NUMBER        | BIT             | TINYINT(4)          | SMALLINT         | SMALLINT        | NUMBER        |                                                  |
| 3  | COL_TINYINT    | NUMBER        | TINYINT         | TINYINT(3) unsigned | SMALLINT         | SMALLINT        | NUMBER        |                                                  |
| 4  | COL_SMALLINT   | NUMBER        | SMALLINT        | SMALLINT(6)         | SMALLINT         | SMALLINT        | NUMBER        |                                                  |
| 5  | COL_MEDIUMINT  | NUMBER        | INT             | MEDIUMINT(9)        | INTEGER          | INT             | NUMBER        |                                                  |
| 6  | COL_INT        | NUMBER        | INT             | INT(11)             | INTEGER          | INT             | NUMBER        |                                                  |
| 7  | COL_BIGINT     | NUMBER        | BIGINT          | BIGINT(20)          | BIGINT           | BIGINT          | NUMBER        |                                                  |
| 8  | COL_DECIMAL    | NUMBER        | DECIMAL(38, 20) | DECIMAL(38, 20)     | NUMERIC(38 ,20)  | DECIMAL(38, 20) | NUMBER        |                                                  |
| 9  | COL_NUMERIC    | NUMBER        | NUMERIC(38, 18) | DECIMAL(38, 18)     | NUMERIC(38, 18)  | NUMERIC(38, 18) | NUMBER        |                                                  |
| 10 | COL_FLOAT      | BINARY_FLOAT  | REAL            | FLOAT               | REAL             | FLOAT           | BINARY_FLOAT  |                                                  |
| 11 | COL_DOUBLE     | BINARY_DOUBLE | FLOAT           | DOUBLE              | DOUBLE PRECISION | DOUBLE          | BINARY_DOUBLE |                                                  |
| 12 | COL_SMALLMONEY | NUMBER        | SMALLMONEY      | DECIMAL(15, 4)      | MONEY            | DECIMAL(15, 4)  | NUMBER        |                                                  |
| 13 | COL_MONEY      | NUMBER        | MONEY           | DECIMAL(25, 6)      | MONEY            | DECIMAL(25, 6)  | NUMBER        |                                                  |
<br>

#### 3.4. DATETIME_TEST

| # | Column Name          | Oracle                       | SQL Server    | MySQL        | PostgreSQL | CUBRID       | Tibero                       | 비고                                               |
|---|----------------------|------------------------------|---------------|--------------|------------|--------------|------------------------------|---------------------------------------------------|
| 1 | T_ID                 | NUMBER                       | INT           | INT(11)      | INTEGER    | INT          | NUMBER                       | Sequence(DATETIME_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_DATETIME         | DATE                         | SMALLDATETIME | DATETIME     | TIMESTAMP  | TIMESTAMP    | DATE                         |                                                   |
| 3 | COL_TIMESTAMP        | TIMESTAMP(6)                 | DATETIME      | TIMESTAMP(6) | TIMESTAMP  | DATETIME     | TIMESTAMP(6)                 |                                                   |
| 4 | COL_TIMESTAMP2       | TIMESTAMP(6)                 | DATETIME2(7)  | DATETIME(6)  | TIMESTAMP  | DATETIME     | TIMESTAMP(6)                 |                                                   |
| 5 | COL_INTER_YEAR_MONTH | INTERVAL YEAR(9) TO MONTH    | VARCHAR(255)  | VARCHAR(255) | INTERVAL   | VARCHAR(255) | INTERVAL YEAR(9) TO MONTH    |                                                   |
| 6 | COL_INTER_DAY_SEC    | INTERVAL DAY(9) TO SECOND(9) | VARCHAR(255)  | VARCHAR(255) | INTERVAL   | VARCHAR(255) | INTERVAL DAY(9) TO SECOND(9) |                                                   |
<br>

#### 3.5. BINARY_TEST

| # | Column Name     | Oracle     | SQL Server      | MySQL    | PostgreSQL | CUBRID            | Tibero     |  비고                                                |
|---|-----------------|------------|-----------------|----------|------------|-------------------|------------|-------------------------------------------------|
| 1 | T_ID            | NUMBER     | INT             | INT(11)  | INTEGER    | INT               | NUMBER     | Sequence(BINARY_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_BINARY      | RAW (2000) | BINARY(2000)    | BLOB     | BYTEA      | BIT(2000)         | RAW (2000) |                                                 |
| 3 | COL_VARBINARY   | RAW (2000) | VARBINARY(2000) | BLOB     | BYTEA      | BIT VARYING(2000) | RAW (2000) |                                                 |
| 4 | COL_LONG_BINARY | LONG RAW   | VARBINARY(MAX)  | LONGBLOB | BYTEA      | BIT VARYING       | LONG RAW   |                                                 |
<br>

#### 3.6. LOB_TEST

| # | Column Name | Oracle | SQL Server     | MySQL    | PostgreSQL | CUBRID | Tibero | 비고                                          |
|---|-------------|--------|----------------|----------|------------|--------|--------|----------------------------------------------|
| 1 | T_ID        | NUMBER | INT            | INT(11)  | INTEGER    | INT    | NUMBER | Sequence(LOB_TEST_SEQ, Start=1, Increment=1) |
| 3 | COL_CLOB    | CLOB   | VARCHAR(MAX)   | LONGTEXT | TEXT       | CLOB   | CLOB   |                                              |
| 5 | COL_NCLOB   | NCLOB  | NVARCHAR(MAX)  | LONGTEXT | TEXT       | CLOB   | NCLOB  |                                              |
| 7 | COL_BLOB    | BLOB   | VARBINARY(MAX) | LONGBLOB | BYTEA      | BLOB   | BLOB   |                                              |

> ※ Oracle의 경우 DB에 default로 지정된 LOB option (BASICFILE, SECUREFILE)에 따라 LOB type을 생성하고 있습니다. <br>
> &nbsp;&nbsp;&nbsp; 12c의 경우 기본적으로 SECUREFILE 로 생성되기 때문에 BASICFILE 로 생성하려면 다음과 같이 database parameter 수정이 필요합니다. <br>
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **SQL> ALTER SYSTEM SET db_securefile=NEVER scope=spfile;** <br>
> &nbsp;&nbsp;&nbsp; Database 재시작 후 *initializer*를 통해 테이블을 생성하면 BASICFILE 옵션의 LOB type으로 생성됩니다.

<br>

#### 3.7. ORACLE_TEST

| # | Column Name | Data Type  | 비고                                             |
|---|-------------|------------|-------------------------------------------------|
| 1 | T_ID        | NUMBER     | Sequence(ORACLE_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_ROWID   | ROWID      |                                                 |
| 3 | COL_FLOAT   | FLOAT(126) |                                                 |
<br>

#### 3.8. SQLSERVER_TEST

| # | Column Name        | Data Type      | 비고                           |
|---|--------------------|----------------|-------------------------------|
| 1 | T_ID               | INT            | ID=True, Start=1, Increment=1 |
| 2 | COL_DATE           | DATE           |                               |
| 3 | COL_TIME           | TIME           |                               |
| 4 | COL_DATETIMEOFFSET | DATETIMEOFFSET |                               |
<br>

#### 3.9. User Defined Table
기존의 샘플 테이블뿐만 아니라 사용자가 원하는 테이블 구조를 생성하여 CDCBENCH에 활용할 수 있습니다. <br>
테이블 구조를 정의한 파일을 **Definition File**이라 하며, 다음의 포맷으로 구성됩니다.

<pre>
&lt;TABLE NAME&gt; (
    &lt;COLUMN NAME&gt; &lt;DATA TYPE [EXTRA Keyword ...]&gt; [SEQUENTIAL] [NOT NULL],
    [&lt;COLUMN NAME&gt; &lt;DATA TYPE [EXTRA Keyword ...]&gt; [SEQUENTIAL] [NOT NULL], ...]
    CONSTRAINT &lt;CONSTRAINT_NAME&gt; PRIMARY KEY (&lt;Key Column&gt;[, &lt;Key Column&gt;])
)

example.
TEST_TAB (
    A NUMBER SEQUENTIAL,
    B VARCHAR2(50) NOT NULL,
    CONSTRAINT TEST_TAB_PK PRIMARY KEY (A)
) 
</pre>

위 포맷으로 구성된 Definition File이 다음 경로에 위치해야 합니다.
* **definitions/&lt;DBMS_TYPE&gt;/&lt;table_name&gt;.def**

지원하는 DBMS별 데이터 타입 및 키워드의 경우 다음의 문서를 참조해주시기 바랍니다. (https://lab.idatabank.com/confluence/x/VYoOAQ)

기타
* CUBRID, Tibero의 경우 Definition File 원문 그대로 사용합니다. (DBMS 자체의 키워드만 사용 가능)
* SEQUENTIAL 키워드를 사용할 경우 해당 컬럼은 Oracle 기준 SEQUENCE를 사용하게 됩니다. (타 DBMS의 경우도 유사한 기능으로 동작)
* Definition 내용은 대소문자를 구분합니다.

## 4. Usage 
CDCBENCH는 다음의 3가지 기능으로 이루어진 프로그램입니다.
> * initializer: CDCBENCH에 사용될 Object 및 데이터를 초기화(생성, 삭제, 재생성)
> * cdcbench: DML 부하테스트를 수행
> * ranbench: 임의의 DML로 구성된 트랜잭션을 생성

> ※ 기능에 사용되는 Object 및 데이터의 상세 내용은 **3. Table Information** 및 **5. Data Configuration**을 참고

### 4.1. initializer
CDCBENCH에 사용될 Object 및 데이터를 초기화하는 **initializer**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: initializer [option...][argument...]

  -h, --help: 
      initializer의 도움말을 출력합니다.
      
  -c, --create
      CDCBENCH에 사용될 Object 및 데이터를 생성합니다. 
      * 테이블 및 데이터가 존재하는 상태에서 사용할 경우 데이터만 append 됩니다.
      
  -d, --drop
      CDCBENCH와 관련된 Object를 모두 삭제합니다.
      
  -r, --reset
      CDCBENCH와 관련된 Object 및 데이터를 재생성 합니다. (--drop 수행 후 --create를 수행하는 방식)
      
  -s, --source
      initializer 대상을 config file의 source_database 환경으로 지정합니다. 
      * --target(-t) / --both(-b) 옵션이 없을 경우 이 옵션이 사용됩니다.
      
  -t, --target
      initializer 대상을 config file의 target_database 환경으로 지정합니다.
  
  -b, --both
      initializer 대상을 config file의 source/target_database 모두로 지정합니다.
      * 초기 생성되는 데이터는 source와 target이 동일하게 됩니다.

  -p, --primary
      키 컬럼을 Primary Key로 설정합니다.
      --unique(-u) / --non-key(-n) 옵션이 없을 경우 이 옵션이 사용됩니다.
  
  -u, --unique
      키 컬럼을 Unique Key로 설정합니다.
      
  -n, --non-key
      제약조건을 설정하지 않습니다. 
  
  -w, --without-data
      데이터를 생성하지 않고, Table만 생성합니다.
  
  -o, --only-data
      Table을 생성하는 절차없이 데이터만 생성합니다.
  
  -y, --assumeyes
      작업을 진행할 것인지 묻는 질문을 'Y'로 답하고 진행합니다.
      
  -f, --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 initializer를 실행합니다.
      * -f/--config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.conf의 내용을 출력합니다.
      * 다른 옵션과 함께 -f/--config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다. 
  
  -v, --verbose
      작업 진행시 progress bar를 통해 진행상태를 나타냅니다.
  
  -V, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

#### 4.1.1. initializer example
<pre>
> py initializer --create
  → CDCBENCH에 사용될 Object 및 데이터를 생성
  
> py initializer --drop --config target
  → target.conf의 데이터베이스 정보를 바탕으로 CDCBENCH와 관련된 Object를 삭제
  
> py initializer --reset --both
  → default.conf의 source/target_database에 기입된 환경에 CDCBENCH와 관련된 Object 및 데이터를 삭제 후 재생성
</pre>



### 4.2. cdcbench
DML 부하테스트에 사용되는 **cdcbench**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: cdcbench [option...][argument...]

  -h, --help
      cdcbench의 도움말을 출력합니다.
  
  -S|N|D|B|L|O|Q, --string|numeric|datetime|binary|lob|oracle|sqlserver
      옵션에 해당하는 테이블을 지정합니다. DML 옵션과 함께 사용될 수 있습니다.
      * -O/--oracle, -Q/--sqlserver 옵션의 경우 config의 "[source_database] dbms_type"의 값이 해당 dbms로 설정되어 있어야 합니다.
  
  -U, --user-table &lt;User defined table name&gt;
      사용자가 정의한 테이블을 지정합니다. DML 옵션과 함께 사용될 수 있습니다.
      --string|numeric|datetime|binary|lob|oracle|sqlserver 옵션의 테이블은 지정할 수 없습니다.
      
  -i, --insert &lt;number of data&gt;
      number of data만큼 데이터를 insert 합니다.
      * -c/--commit 옵션 미사용시 commit 단위는 1000 입니다.
      * 테이블 지정 옵션을 사용하지 않을 경우 INSERT_TEST 테이블에 insert 합니다.  
      
  -u, --update [Start key value [End key value]]
      입력한 조건에 맞는 데이터를 update 합니다.
      * Start key value 값은 End key value 값보다 클 수 없습니다.
      * 테이블 지정 옵션을 사용하지 않을 경우 UPDATE_TEST 테이블에 update 합니다.
      * 인자유무에 따라 동작 방식이 달라지게 됩니다.
        - 인자없이 사용: where 조건 없이 update 수행
        - Start key value만 사용: where Key = Start key value
        - Start, End key value 모두 사용: where Start key value <= Key <= End key value 
      
  -d, --delete [Start key value [End key value]]
      입력한 조건에 맞는 데이터를 delete 합니다.
      * Start key value 값은 End key value 값보다 클 수 없습니다.
      * 테이블 지정 옵션을 사용하지 않을 경우 DELETE_TEST 테이블에 delete 합니다.
      * 인자유무에 따라 동작 방식이 달라지게 됩니다.
        - 인자없이 사용: where 조건 없이 delete 수행
        - Start key value만 사용: where Key = Start key value 조건으로 delete 수행
        - Start, End key value 모두 사용: where Start key value <= Key <= End key value 조건으로 delete 수행

  -c, --commit &lt;commit unit&gt;
      DML 수행 시 commit 단위를 지정합니다. 
      
  -s, --single
      insert를 single insert 방식으로 수행합니다 (기본적으로 multi insert). -i/--insert 옵션과 함께 사용할 수 있습니다.
  
  -r, --rollback
      발생한 트랜잭션을 Rollback 합니다.
  
  -C, --columns &lt;Column ID | Name&gt; [&lt;Column ID | Name&gt; ...]
      지정한 컬럼에만 DML을 발생시킵니다. 인자로는 Column ID 또는 Column Name을 입력합니다.
      ex) --columns 2 3 4 / --columns col_char col_nchar
      * Column ID와 Column Name 방식을 함께 사용할 수 없습니다. 
  
  -w, --where "&lt;Where Clause&gt;"
      update/delete시 사용자가 원하는 Where 조건을 지정합니다. (--update/--delete의 key value 인자가 무시됩니다.)
      ex) --update --where "1 <= separate_col and separate_col <= 3"
          → UPDATE UPDATE_TEST SET COL_NAME = '???' WHERE 1 <= SEPARATE_COL AND SEPARATE_COL <= 3;
  
  -sep, --separate-tx &lt;Column ID | Name&gt;
      --where 옵션 사용시 조건이 범위 조건에 해당할 경우 update/delete를 순차적으로 수행한다.
      인자는 where절의 조건 기준이 되는 Column의 ID 또는 Name을 입력합니다.
      ex) --update --where "1 <= separate_col and separate_col <= 3" --separate-tx separate_col
          → UPDATE UPDATE_TEST SET COL_NAME = '???' WHERE SEPARATE_COL = 1;
          → UPDATE UPDATE_TEST SET COL_NAME = '???' WHERE SEPARATE_COL = 2;
          → UPDATE UPDATE_TEST SET COL_NAME = '???' WHERE SEPARATE_COL = 3;
      
  -f, --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 cdcbench를 실행합니다.
      * -f/--config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.conf의 내용을 출력합니다.
      * 다른 옵션과 함께 -f/--config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 cdcbench를 수행합니다.
  
  -v, --verbose
      작업 진행시 progress bar를 통해 진행상태를 나타냅니다.
      
  -V, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

#### 4.2.1. cdcbench --insert example
<pre>
> py cdcbench --insert 10000
  → INSERT_TEST 테이블에 10000건의 데이터를 1000건씩 commit하여 insert 합니다.

> py cdcbench --insert 20000 --commit 2000
  → INSERT_TEST 테이블에 20000건의 데이터를 2000건씩 commit하여 insert 합니다.

> py cdcbench --insert 10000 --single
  → INSERT_TEST 테이블에 single insert 방식으로 10000건의 데이터를 1000건씩 commit하여 insert 합니다.
  
> py cdcbench --insert 10000 --config target
  → target.conf file의 데이터베이스 정보로 INSERT_TEST 테이블에 10000건의 데이터를 1000건씩 commit하여 insert 합니다.

> py cdcbench --string --insert 100
  → STRING_TEST 테이블에 100건의 데이터를 insert 합니다.

> py cdcbench --datetime --insert 10 --rollback
  → DATETIME_TEST 테이블에 10건의 데이터를 insert하고 Rollback 합니다.

> py cdcbench --numeric --insert 10 --columns 2 3
  (=py cdcbench --numeric --insert 10 --columns col_bit col_tinyint)
  → NUMERIC_TEST 테이블에 10건의 데이터를 insert하되, COL_BIT, COL_TINYINT 컬럼에만 데이터를 입력합니다. 
</pre>

#### 4.2.2. cdcbench --update example
<pre>
> py cdcbench --update 1 5
  → UPDATE_TEST 테이블에서 T_ID 컬럼의 값이 1 ~ 5인 row의 데이터를 update 합니다.
  
> py cdcbench --update 2
  → UPDATE_TEST 테이블에서 T_ID 컬럼의 값이 2인 row의 데이터를 update 합니다.
  
> py cdcbench --update 1 3 --config target
  → target.conf file의 데이터베이스 정보로 UPDATE_TEST 테이블의 T_ID 컬럼의 값이 1 ~ 3인 row의 데이터를 update 합니다.

> py cdcbench --numeric --update 1 10
  → NUMERIC_TEST 테이블에서 T_ID가 1 ~ 10인 데이터를 update 합니다.

> py cdcbench --string --update --where "COL_CHAR = 'CANNED JUICES'"
  → STRING_TEST 테이블에서 COL_CHAR의 값이 'CANNED JUICES'인 row의 데이터를 update 합니다.
</pre>

#### 4.2.3. cdcbench --delete example
<pre>
> py cdcbench --delete 1 5
  → DELETE_TEST 테이블에서 T_ID 컬럼의 값이 1 ~ 5인 row의 데이터를 delete 합니다.
  
> py cdcbench --delete 3
  → DELETE_TEST 테이블에서 T_ID 컬럼의 값이 3인 row의 데이터를 delete 합니다.

> py cdcbench --lob --delete 1 10
  → LOB_TEST 테이블에서 T_ID가 1 ~ 10인 데이터를 delete 합니다.

> py cdcbench --string --delete --where "COL_CHAR = 'CANNED JUICES'"
  → STRING_TEST 테이블에서 COL_CHAR의 값이 'CANNED JUICES'인 row의 데이터를 delete 합니다.
</pre>

### 4.3. ranbench
임의의 DML로 구성된 트랜잭션을 발생시키는 **ranbench**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: ranbench [option...][argument...]

  -h, --help
      ranbench의 도움말을 출력합니다.
      
  -C, --total-record &lt;count of total record&gt;
      총 record 수를 기준으로 임의의 DML로 구성된 트랜잭션을 생성합니다.
  
  -D, --dml-count &lt;count of DML&gt;
      발생시킬 DML 수를 기준으로 임의의 DML로 구성된 트랜잭션을 생성합니다.
  
  -T, --run-time &lt;running time (sec.)&gt;
      총 수행시간을 기준으로 임의의 DML로 구성된 트랜잭션을 생성합니다.
  
  -n, --range &lt;start count of record&gt; [&lt;end count of record&gt;]
      DML당 발생시킬 record 양의 범위를 지정합니다.
      * start count of record 값은 end count of record 값보다 클 수 없습니다.
      * 인자 개수에 따른 record 개수는 다음과 같습니다.
        - start count of record만 사용: DML당 start count of record 만큼의 record 변화가 발생
        - start, end count of record 모두 사용: DML 당 start count of record <= N <= end count of record 만큼의 record 변화가 발생
  
  -s, --sleep &lt;Idle time (sec.)&gt;
      DML 사이에 유휴시간을 줍니다.
      
  -t, --tables &lt;table name&gt; [&lt;table name&gt; ...]
      임의의 DML을 발생시킬 테이블을 지정합니다.
      * 해당 옵션을 사용하지 않을 경우 STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST 테이블이 지정됩니다.
  
  -d, --dml &lt;DML type&gt; [&lt;DML type&gt; ...]
      발생시킬 DML 유형을 지정합니다.
      * 해당 옵션을 사용하지 않을 경우 INSERT, UPDATE, DELETE가 지정됩니다.
  
  -r, --rollback
      발생한 트랜잭션을 Rollback 합니다.

  -f, --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 ranbench를 실행합니다.
      * -f/--config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.conf의 내용을 출력합니다.
      * 다른 옵션과 함께 -f/--config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 ranbench를 수행합니다.
  
  -v, --verbose
      작업 진행시 progress bar를 통해 진행상태를 나타냅니다.
      
  -V, --version
      CDCBENCH의 버전을 출력합니다.
  
</pre>
* 상세 수행결과는 reports/ranbench_&lt;YYYY_MM_DD&gt;.rep 파일에서 확인할 수 있습니다.

#### 4.3.1. ranbench example
<pre>
> py ranbench --total-record 100 --range 10
  → 임의의 테이블들(STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST 중)에 
    총 100건의 임의의 DML(INSERT, UPDATE, DELETE 중)들을 10건씩 발생시킵니다.
 
> py ranbench --dml-count 30 --range 10 15 --tables string_test datetime_test
  → 임의의 테이블들(STRING_TEST, DATETIME_TEST 중)에 
    총 30번의 임의의 DML(INSERT, UPDATE, DELETE 중)들을 10~15건씩 발생시킵니다.

> py ranbench --run-time 10 --range 100 200 --dml insert --sleep 2
  → 임의의 테이블들(STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST 중)에
    총 10초 동안 INSERT를 100~200건씩 2초간 쉬면서 발생시킵니다.
</pre> 

<hr>

## 5. Data Configuration
CDCBENCH에 사용되는 데이터는 데이터 파일을 수정함으로서 사용자가 원하는 데이터 집합을 만들 수 있습니다. 

<pre>
COL_NAME:
  - BAKING MIXES
  - BAKING NEEDS
  ...

COL_DATE:
  - 2025-11-11 20:23:23
  - 2073-08-22 15:51:04
  ...
</pre>

* 데이터 저장 방식은 YAML 포맷을 따르고 있습니다.
* 테이블의 컬럼 이름을 key 값으로, 컬럼에 사용할 데이터를 리스트로 관리하고 있습니다.
* 데이터 insert 및 update 시에는 컬럼에 해당하는 리스트에서 랜덤하게 값을 선택해 사용합니다.
* 해당 컬럼을 *null*로 입력하고 싶다면, 리스트를 비워두면 됩니다.
* 다음의 경우 올바르게 동작하지 않을 수 있습니다. 즉, 기존 형태는 유지하되, 리스트의 데이터만 수정해야 합니다.
  * 데이터 파일의 컬럼 자체가 삭제될 경우
  * Key 값 순서가 테이블 구조와 다를 경우
  * 데이터베이스의 컬럼명과 다를 경우

### 5.1 DML
*initializer, cdcbench --insert, --update*에서 사용되는 **dml.dat** 데이터는 위 예시의 포맷을 참고해주시고, 다음의 특징을 가지고 있습니다.

* **COL_DATE** 컬럼은 년-월-일 시:분:초 형태를 가져야 합니다. (ex. 2017-03-05 07:34:07)  

### 5.2 String
*cdcbench --string*에서 활용되는 **string.dat** 데이터는 다음과 같습니다.
<pre>
COL_CHAR:
  - BAKED BREAD/BUNS/ROLLS
  - DRY BN/VEG/POTATO/RICE
  ...
  - 카스캔맥주(355㎖)
  - 하이트 병맥주(500㎖)
  ...
  - 茄子
  - 播多

COL_NCHAR:
  ...

COL_VARCHAR_B:
  ...

COL_VARCHAR_C:
  ...

COL_NVARCHAR:
  ...

COL_TEXT:
  - t_eng_0512_utf8.txt
  - t_eng_1024_utf8.txt
  ...
</pre>

* Configuration 중 NLS_LANG 값이 Database의 Character Set과 다를 경우 값이 정상적으로 입력되지 않을 수 있습니다.
* **COL_TEXT** 데이터는 데이터 파일(string.dat)에 직접 데이터를 입력하는 것이 아닌 입력할 데이터가 있는 파일 (source file)의 이름을 입력합니다.
  source file은 **./lob_files** 디렉토리에 존재해야 합니다.  
* **COL_TEXT** 컬럼은 문자열로 이루어진 .txt 확장자만 정상적으로 insert 할 수 있습니다. 그리고 파일 인코딩은 **UTF-8 (without BOM)** 이어야 합니다.
* source file의 크기는 약 1GB 미만까지 허용됩니다. (파일 크기를 체크하지는 않습니다.)

### 5.3 Numeric
*cdcbench --numeric*에서 활용되는 **numeric.dat** 데이터는 다음과 같습니다.
<pre>
COL_BIT:
  - 0
  - 1

COL_TINYINT:
  - 0
  - 133
  ...

COL_SMALLINT:
  - -30017
  - 11388
  ...

COL_MEDIUMINT:
  - -896628
  - 7191502
  ...

COL_INT:
  - -1152240375
  - 1264534909
  ...

COL_BIGINT:
  - -3052433639043326093
  - 749812178543297333
  ...

COL_DECIMAL:
  - -34125621836211200.462098274783004690
  - 713522811779.4
  ...

COL_NUMERIC:
  - -987764698097.7364
  - 3883895742.203498463368822858
  ...

COL_FLOAT:
  - -3.6439738113652133408132617784345437208
  - 93367252537943150.232615823
  ...

COL_DOUBLE:
  - -7920248.687796348
  - 63457707886647236194153885158423666.0
  ...

COL_SMALLMONEY:
  - 77112.4
  - -27.205
  ...

COL_MONEY:
  - -846641696727418.75
  - 846334227.0
  ...
</pre>

### 5.4 DateTime
*cdcbench --datetime*에서 활용되는 **datetime.dat** 데이터는 다음과 같습니다.
<pre>
COL_DATETIME:
  - 2061-01-23 07:22:24
  - 1996-10-16 22:22:12
  ...

COL_TIMESTAMP:
  - 2005-02-12 23:26:21.508015
  - 2033-12-06 07:00:31.921650
  ...

COL_TIMESTAMP2:
  - 9788-03-03 18:45:45.202941
  - 6505-04-22 09:41:45.749505
  ...

COL_INTER_YEAR_MONTH:
  - [-149084120, 6]
  - [7, 11]
  ...

COL_INTER_DAY_SEC:
  - [59, 21, 34, 27, 6]
  - [270586765, 14, 32, 51, 950]
  ...
</pre>
* **COL_DATE** 컬럼은 연-월-일 시:분:초 의 형태를 가져야합니다.
* **COL_TIMESTAMP, COL_TIMESTAMP2** 컬럼은 연-월-일 시:분:초.밀리초 의 형태를 가져야합니다.
* **COL_INTER_YEAR_MONTH** 컬럼은 [연, 월] 의 형태를 가져야 합니다. (ex. [6, 3] → 6년 3개월)
* **COL_INTER_DAY_SEC** 컬럼은 [일, 시, 분, 초, 밀리초] 의 형태를 가져야 합니다. (ex. [99, 23, 59, 59, 999999] → 99일 23시간 59분 59초 99999 밀리초)

### 5.5 Binary
*cdcbench --binary*의 경우 별도의 데이터 파일이 존재하지 않고, 데이터 생성 함수를 사용합니다.
* **COL_BINARY, COL_VARBINARY** 컬럼은 1~1000 사이의 크기를 가진 임의의 이진 데이터를 생성합니다.
* **COL_LONG_BINARY** 컬럼은 1~2000 사이의 크기를 가진 임의의 이진 데이터를 생성합니다.

### 5.6 LOB
*cdcbench --lob*에서 활용되는 **lob.dat** 데이터는 다음과 같습니다.
<pre>
COL_CLOB:
  - t_eng_0512_utf8.txt
  - t_eng_1024_utf8.txt
  ...

COL_NCLOB:
  - t_eng_0512_utf8.txt
  - t_eng_1024_utf8.txt
  ...

COL_BLOB:
  - i_apple.png
  - i_blue_rose.jpg
  ...
</pre>
* lob 데이터는 데이터 파일(lob.dat)에 직접 데이터를 입력하는 것이 아닌 입력할 데이터가 있는 파일 (source file)의 이름을 입력합니다.
  source file은 **./lob_files** 디렉토리에 존재해야 합니다.  
* **COL_CLOB**, **COL_NCLOB** 컬럼은 문자열로 이루어진 .txt 확장자만 정상적으로 insert 할 수 있습니다. 그리고 파일 인코딩은 **UTF-8 (without BOM)** 이어야 합니다.
* source file의 크기는 약 1GB 미만까지 허용됩니다. (파일 크기를 체크하지는 않습니다.)

### 5.7 Oracle
*cdcbench --oracle*에서 활용되는 **oracle.dat** 데이터는 다음과 같습니다.
<pre>
COL_ROWID:
  - AAAShYAAFAAAAC9AiQ
  - AAAShYAAFAAAAC9AqG
  ...

COL_FLOAT:
  - -2682546578656.66829904582251874864
  - 863450387585623767262548820188856572.0
  ...
</pre>

### 5.8 SQL Server
*cdcbench --sqlserver*에서 활용되는 **sqlserver.dat** 데이터는 다음과 같습니다.
<pre>
COL_DATE:
  - 4338-02-03
  - 3839-04-06
  ...

COL_TIME:
  - "10:16:48.500675"
  - "15:41:24.661137"
  ...

COL_DATETIMEOFFSET:
  - 2908-12-23 17:29:53.121071 +09:00
  - 3554-05-20 02:41:58.090052 +09:00
  ...
</pre>
* **COL_DATE** 컬럼은 연-월-일 의 형태를 가져야합니다.
* **COL_TIME** 컬럼은 "시:분:초.밀리초" 의 형태를 가져야합니다. 
* **COL_DATETIMEOFFSET** 컬럼은 연-월-일 시:분:초.밀리초 {+|-}00~12:분 의 형태를 가져야 합니다.

### 5.9 User Defined Table
*cdcbench --user-table*에서 활용되는 **user.dat** 데이터는 다음과 같습니다.
<pre>
  ### Category. String ###

# GROUP.CHAR, Including CHAR, NCHAR, TINYTEXT
#   CHAR: Oracle, MySQL, SQL Server, PostgreSQL
#   NCHAR: Oracle, MySQL, SQL Server
#   TINYTEXT: MySQL
GROUP.CHAR:
  - BAKED BREAD/BUNS/ROLLS
  - DRY BN/VEG/POTATO/RICE
  ...

# GROUP.VARCHAR, Including VARCHAR2, NVARCHAR2, TEXT
#   VARCHAR: MySQL, SQL Server, PostgreSQL
#   NVARCHAR: MySQL, SQL Server
#   VARCHAR2: Oracle
#   NVARCHAR2: Oracle
#   TEXT: MySQL
GROUP.VARCHAR:
  - REFRGRATD DOUGH PRODUCTS
  - CANNED JUICES
  ...

...
# GROUP.TINYINT, Including TINYINT
#   TINYINT: MySQL, SQL Server
GROUP.TINYINT:
  - 0
  - 133
  ...
  
...
</pre>
<hr>

각 DBMS에서 유사한 유형의 데이터 타입을 그룹으로 묶어 그룹에 해당하는 DBMS별로 지정된 특정 데이터 타입일 경우 해당 그룹의 데이터를 사용하게 됩니다. <br>
DBMS에 따라 동일한 이름의 데이터 타입이 존재할 수 있지만, 주석으로 데이터타입 상에 명시된 DBMS일 경우에만 해당 그룹 데이터를 사용합니다.
<pre>
# GROUP.VARCHAR, Including VARCHAR2, NVARCHAR2, TEXT
#   VARCHAR: MySQL, SQL Server, PostgreSQL
#   NVARCHAR: MySQL, SQL Server
#   VARCHAR2: Oracle
#   NVARCHAR2: Oracle
#   TEXT: MySQL
</pre>
예제로 GROUP.VARCHAR의 경우 VARCHAR, NVARCHAR, VARCHAR2, NVARCHAR2, TEXT 타입을 포함합니다. <br>
- MySQL, SQL Server, PostgreSQL의 VARCHAR 타입의 경우 해당 그룹의 데이터를 사용하게 됩니다.
- MySQL, SQL Server의 NVARCHAR 타입의 경우 해당 그룹의 데이터를 사용하게 됩니다.
- Oracle의 VARCHAR2, NVARCHAR2 타입의 경우 해당 그룹의 데이터를 사용하게 됩니다.
- TEXT 타입의 경우 MySQL, PostgreSQL 모두 존재하지만, MySQL의 TEXT의 경우에만 해당 데이터 그룹을 사용하게 됩니다.

## 6. Log
CDCBENCH의 log는 logs/cdcbench.log에 저장됩니다.

Config의 "sql_logging" 값에 따라 logs/sql.log에 실행되는 SQL과 데이터값이 남게 됩니다.
 * 현재 라이브러리 제약에 의해 대량의 데이터의 경우 일부 데이터값 로깅이 생략될 수 있습니다.
 