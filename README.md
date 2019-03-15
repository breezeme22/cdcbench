CDCBENCH
========
CDCBENCH는 Python 기반의 ArkCDC Project 테스트 프로그램입니다. 

> 현재 문서에서 CDCBENCH (upper)와 cdcbench (lower)가 혼용되어 사용되고 있습니다. <br>
> 대문자의 경우 프로젝트로, 소문자의 경우 기능의 명칭으로 사용되고 있습니다.

## 0. Supported Environment
* Windows
* Linux

## 1. Installation
### 1.1 Python Install
CDCBENCH는 다음의 Python 버전에서 동작합니다.
 
Version >= **3.6.3** (https://www.python.org/downloads/)

> 환경별 Python 설치방법은 검색을 활용해주시기 바랍니다. <br>
> 그리고 이후 설치과정은 C:\Python36\; C:\Python36\Scripts; 두 경로를 환경변수 PATH에 추가했다는 전제하에 진행됩니다.

설치를 완료하고 나면 Python Package 관리도구인 pip를 최신버전으로 업그레이드합니다.
<pre>
# Windows
> python -m pip install --upgrade pip

# Linux
> pip install --upgrade pip
</pre>

### 1.2 Database Client Install
Database별 Client 설치는 다음과 같은 절차를 따릅니다.

#### 1.2.1 Oracle

##### &nbsp;&nbsp;&nbsp; Windows
1. 다음의 경로에서 대상 Database와 동일한 버전의 "Basic" Package를 다운로드합니다. <br>
   https://www.oracle.com/technetwork/topics/winx64soft-089540.html

2. 다운로드한 zip file을 적절한 위치에 압축해제 합니다. (ex. C:\util\oracle_instantclient_11_2)

3. 시스템 환경변수 "Path"에 압축해제한 경로를 추가합니다.

</pre>

#### 1.2.2 MySQL

##### &nbsp;&nbsp;&nbsp; Linux (RHEL / CentOS)
1. root 계정에서 RPM을 통해 리눅스 배포판과 Database 버전에 맞는 Repository 설정 RPM 파일을 다운로드 받습니다.
   <pre>
   rpm -ivh https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm
   </pre>
   
2. yum을 통해 MySQL 5.7 Database와 관련한 패키지들을 설치할 수 있으며, 다음의 패키지 설치가 필요합니다.
   <pre>
   yum install -y mysql-community-devel
   </pre>
   
##### &nbsp;&nbsp;&nbsp; Windows
1. Windows의 경우 pip 저장소 상의 mysqlclient를 설치할 경우 VisualStudio와 MySQL Connector를 설치하라는 에러가 발생하므로
   별도의 whl 파일을 통해 설치합니다. <br>
   다음의 경로에서 Python 버전과 Architecture에 맞는 whl 파일을 다운로드 받아줍니다. <br>
   https://www.lfd.uci.edu/~gohlke/pythonlibs/#mysqlclient
   
2. 다운로드 받은 whl 파일을 통해 mysqlclient 패키지를 설치합니다.
   <pre>
   pip install ".whl file 경로 및 파일명"
   </pre>

### 1.3 CDCBENCH Download
https://lab.idatabank.com/gitlab/sangcheolpark/cdcbench/tags
> Line Separator (CRLF, LF)와 관련해 실행에 문제가 있을 수 있으니, 실제 사용될 OS에서 다운로드 해주시기 바랍니다.


### 1.4 CDCBENCH Install
Script 기반으로 별도의 설치과정은 없으며, 제공되는 압축파일을 해제하면 설치가 완료됩니다. 생성되는 디렉토리 구조는 다음과 같습니다.
<pre>
cdcbench ($CDCBENCH_HOME)
├─ bin              | CDCBENCH 기능 실행 스크립트가 저장된 디렉토리.
├─ commons          | 기능을 구현한 python file 디렉토리.
├─ conf             | configration file들을 저장하는 디렉토리. 
│   └─ default.ini  | 별도의 configuration file를 지정하지 않을 경우 사용되는 configuration file.
│                      * 이름이 변경될 경우 --config 옵션을 통해서 사용할 수 있습니다.
├─ data             | CDCBENCH에서 사용되는 데이터 파일들을 저장하는 디렉토리. 
├─ mappers          | 데이터베이스 Object와 매핑되는 Mapper 파일이 존재하는 디렉토리.
├─ README.md        | README.md
└─ requirements.txt | CDCBENCH 동작에 요구되는 패키지 리스트
</pre>
> 이후 설치경로를 편의상 $CDCBENCH_HOME 이라고 칭합니다.

### 1.5 Python Library Install
설치가 완료된 후 CDCBENCH 실행에 필요한 라이브러리를 다음의 절차로 설치합니다.
<pre>
> cd $CDCBENCH_HOME
> pip install -r requirements.txt
</pre>
이미 요구되는 패키지가 설치되어 있다면, 다음의 명령으로 업그레이드 할 수 있습니다.
<pre>
> pip install -r requirements.txt --upgrade
</pre>

<hr>

## 2. Configuration

### 2.1 Configuration File
* 모든 Configuration 파일은 **$CDCBENCH_HOME/conf** 디렉토리에 저장되어야 합니다. 
* *-f/--config* 옵션을 사용하지 않을 경우 기본적으로 **default.ini**을 인식합니다.
* **default.ini** 파일과 동일한 포맷을 갖추고 있다면, *-f/--config* 옵션을 통해 CDCBENCH 실행 시 configuration 파일을 변경할 수 있습니다.  

### 2.2 Configuration
Configuration은 크게 CDCBENCH 관련 설정 / Database 정보 / 초기화 데이터 설정으로 구분할 수 있습니다. 
정의 형식은 **Parameter = Value** 형식이며, **모든 파라미터는 값을 필수적으로 입력해야 합니다.**

* **[setting]**
  > CDCBENCH 동작과 관련된 파라미터로 다음과 같은 파라미터들을 가지고 있습니다.

  * log_level = [ ERROR | INFO | DEBUG ] &nbsp; (출력할 Log Level을 지정합니다.)
  * sql_logging = [ yes (y) | no (n) ] &nbsp; (설정 여부에 따라 수행되는 SQL이 log로 남게됩니다.)
  * nls_lang = [ *$NLS_LANG* ] &nbsp; (Oracle Server의 Character Set입니다. profile의 $NLS_LANG 값과 동일하게 입력하면 됩니다. Oracle이 아닌 경우 아무 값을 넣어주시면 됩니다.)
  <br>
* **[source(target)_database]**
  > 사용할 데이터베이스의 연결정보를 입력합니다.
  >  * target_database 영역은 initializer 기능에서만 사용됩니다. 그 외의 cdcbench, typebench 수행 시에는 source_database 정보만을 사용합니다.
  >  * target_database가 필요없을 경우 source_database 영역과 동일하게 입력해주시면 됩니다. 

  * host_name = [ *IPv4 Format* ] &nbsp; (연결할 데이터베이스의 IP를 입력합니다.)
  * port = [ *1024 ~ 65535* ] &nbsp; (연결할 데이터베이스와 통신할 Port를 입력합니다.)
  * dbms_type = [ oracle | mysql | sqlserver | postgresql] &nbsp; (연결할 데이터베이스의 타입을 입력합니다.)
  * db_name = [ *database name (instance_name)* ] &nbsp; (연결할 데이터베이스의 이름을 입력합니다.)
  * schema_name = [ *schema name* ] &nbsp; (오브젝트를 소유한 스키마의 이름을 입력합니다. oracle/mysql의 경우 user_id와 동일하게 입력하면 됩니다.)
  * user_id = [ *user name* ] &nbsp; (CDCBENCH를 사용할 데이터베이스 유저를 입력합니다.)
  * user_password = [ *user_password* ] &nbsp; (CDCBENCH를 사용할 데이터베이스 유저의 패스워드를 입력합니다.)
  <br>
* **[initial_update(delete)_test_data]**
  > CDCBENCH 초기화(initializer --create, --reset) 시, update_test (delete_test) 테이블에 생성할 데이터의 양과 Commit 단위를 지정합니다.
  
  * number_of_data = [ *number >= 1* ] &nbsp; (테이블에 생성할 총 데이터의 양을 지정합니다.)
  * commit_unit = [ *number >= 1* ] &nbsp; (데이터 생성시 commit이 발생하는 기준을 지정합니다.)
  
<hr>

## 3. Table Information
CDCBENCH에서 사용되는 테이블은 총 8개로 기능에 따라 사용되는 테이블이 구분됩니다.
> * cdcbench: INSERT_TEST, UPDATE_TEST, DELETE_TEST
> * typebench: STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST, ORACLE_TEST, SQLSERVER_TEST

### 3.1 cdcbench
cdcbench 기능에 사용되는 테이블들 ( INSERT_TEST / UPDATE_TEST / DELETE_TEST )은 다음의 동일한 구조를 가집니다.

##### &nbsp;&nbsp;&nbsp; INSERT_TEST / UPDATE_TEST / DELETE_TEST
| # | Column Name  | Oracle       | SQL Server  | MySQL       | PostgreSQL  | 비고                                                |
|---|--------------|--------------|-------------|-------------|-------------|-----------------------------------------------------|
| 1 | PRODUCT_ID   | NUMBER       | INT         | INT         | INTEGER     | PK, Sequence(INSERT_TEST_SEQ, Start=1, Increment=1) |
| 2 | PRODUCT_NAME | VARCHAR2(50) | VARCHAR(50) | VARCHAR(50) | VARCHAR(50) |                                                     |
| 3 | PRODUCT_DATE | DATE         | DATETIME    | DATETIME    | TIMESTAMP   |                                                     |
| 4 | SEPARATE_COL | NUMBER       | INT         | INT         | INTEGER     |                                                     |

### 3.2 typebench
이후 설명되는 테이블은 typebench 기능에서 사용됩니다.

#### 3.2.1 STRING_TEST 

| # | Column Name   | Oracle              | SQL Server     | MySQL         | PostgreSQL    | 비고                                            |
|---|---------------|---------------------|----------------|---------------|---------------|-------------------------------------------------|
| 1 | T_ID          | NUMBER              | INT            | INT(11)       | INTEGER       | Sequence(STRING_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_CHAR      | CHAR(50)            | CHAR(50)       | CHAR(50)      | CHAR(50)      |                                                 |
| 3 | COL_NCHAR     | NCHAR(50)           | NCHAR(50)      | CHAR(50)      | CHAR(50)      |                                                 |
| 4 | COL_VARCHAR_B | VARCHAR2(4000)      | VARCHAR(4000)  | VARCHAR(4000) | VARCHAR(4000) |                                                 |
| 5 | COL_VARCHAR_C | VARCHAR2(4000 CHAR) | VARCHAR(4000)  | VARCHAR(4000) | VARCHAR(4000) |                                                 |
| 6 | COL_NVARCHAR  | NVARCHAR2(2000)     | NVARCHAR(2000) | VARCHAR(2000) | VARCHAR(2000) |                                                 |
| 7 | COL_TEXT      | LONG                | VARCHAR(MAX)   | LONGTEXT      | TEXT          |                                                 |
<br>

#### 3.2.2 NUMERIC_TEST

| #  | Column Name    | Oracle        | SQL Server      | MySQL               | PostgreSQL       | 비고                                             |
|----|----------------|---------------|-----------------|---------------------|------------------|--------------------------------------------------|
| 1  | T_ID           | NUMBER        | INT             | INT(11)             | INTEGER          | Sequence(NUMERIC_TEST_SEQ, Start=1, Increment=1) |
| 2  | COL_BIT        | NUMBER        | BIT             | TINYINT(4)          | SMALLINT         |                                                  |
| 3  | COL_TINYINT    | NUMBER        | TINYINT         | TINYINT(3) unsigned | SMALLINT         |                                                  |
| 4  | COL_SMALLINT   | NUMBER        | SMALLINT        | SMALLINT(6)         | SMALLINT         |                                                  |
| 5  | COL_MEDIUMINT  | NUMBER        | INT             | MEDIUMINT(9)        | INTEGER          |                                                  |
| 6  | COL_INT        | NUMBER        | INT             | INT(11)             | INTEGER          |                                                  |
| 7  | COL_BIGINT     | NUMBER        | BIGINT          | BIGINT(20)          | BIGINT           |                                                  |
| 8  | COL_DECIMAL    | NUMBER        | DECIMAL(38, 20) | DECIMAL(38, 20)     | NUMERIC(38 ,20)  |                                                  |
| 9  | COL_NUMERIC    | NUMBER        | NUMERIC(38, 18) | DECIMAL(38, 18)     | NUMERIC(38, 18)  |                                                  |
| 10 | COL_FLOAT      | BINARY_FLOAT  | REAL            | FLOAT               | REAL             |                                                  |
| 11 | COL_DOUBLE     | BINARY_DOUBLE | FLOAT           | DOUBLE              | DOUBLE PRECISION |                                                  |
| 12 | COL_SMALLMONEY | NUMBER        | SMALLMONEY      | DECIMAL(15, 4)      | MONEY            |                                                  |
| 13 | COL_MONEY      | NUMBER        | MONEY           | DECIMAL(25, 6)      | MONEY            |                                                  |
<br>

#### 3.2.3 DATETIME_TEST

| # | Column Name          | Oracle                       | SQL Server    | MySQL        | PostgreSQL | 비고                                              |
|---|----------------------|------------------------------|---------------|--------------|------------|---------------------------------------------------|
| 1 | T_ID                 | NUMBER                       | INT           | INT(11)      | INTEGER    | Sequence(DATETIME_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_DATETIME         | DATE                         | SMALLDATETIME | DATETIME     | TIMESTAMP  |                                                   |
| 3 | COL_TIMESTAMP        | TIMESTAMP(6)                 | DATETIME      | TIMESTAMP(6) | TIMESTAMP  |                                                   |
| 4 | COL_TIMESTAMP2       | TIMESTAMP(6)                 | DATETIME2(7)  | DATETIME(6)  | TIMESTAMP  |                                                   |
| 5 | COL_INTER_YEAR_MONTH | INTERVAL YEAR(9) TO MONTH    | VARCHAR(255)  | VARCHAR(255) | INTERVAL   |                                                   |
| 6 | COL_INTER_DAY_SEC    | INTERVAL DAY(9) TO SECOND(9) | VARCHAR(255)  | VARCHAR(255) | INTERVAL   |                                                   |
<br>

#### 3.2.4 BINARY_TEST

| # | Column Name     | Oracle     | SQL Server      | MySQL    | PostgreSQL | 비고                                            |
|---|-----------------|------------|-----------------|----------|------------|-------------------------------------------------|
| 1 | T_ID            | NUMBER     | INT             | INT(11)  | INTEGER    | Sequence(BINARY_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_BINARY      | RAW (2000) | BINARY(2000)    | BLOB     | BYTEA      |                                                 |
| 3 | COL_VARBINARY   | RAW (2000) | VARBINARY(2000) | BLOB     | BYTEA      |                                                 |
| 4 | COL_LONG_BINARY | LONG RAW   | VARBINARY(MAX)  | LONGBLOB | BYTEA      |                                                 |
<br>

#### 3.2.5 LOB_TEST

| # | Column Name     | Oracle       | SQL Server     | MySQL       | PostgreSQL  | 비고                                         |
|---|-----------------|--------------|----------------|-------------|-------------|----------------------------------------------|
| 1 | T_ID            | NUMBER       | INT            | INT(11)     | INTEGER     | Sequence(LOB_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_CLOB_ALIAS  | VARCHAR2(50) | VARCHAR(50)    | VARCHAR(50) | VARCHAR(50) |                                              |
| 3 | COL_CLOB_DATA   | CLOB         | VARCHAR(MAX)   | LONGTEXT    | TEXT        |                                              |
| 4 | COL_NCLOB_ALIAS | VARCHAR2(50) | VARCHAR(50)    | VARCHAR(50) | VARCHAR(50) |                                              |
| 5 | COL_NCLOB_DATA  | NCLOB        | NVARCHAR(MAX)  | LONGTEXT    | TEXT        |                                              |
| 6 | COL_BLOB_ALIAS  | VARCHAR2(50) | VARCHAR(50)    | VARCHAR(50) | VARCHAR(50) |                                              |
| 7 | COL_BLOB_DATA   | BLOB         | VARBINARY(MAX) | LONGBLOB    | BYTEA       |                                              |


> ※ Oracle의 경우 DB에 default로 지정된 LOB option (BASICFILE, SECUREFILE)에 따라 LOB type을 생성하고 있습니다. <br>
> &nbsp;&nbsp;&nbsp; 12c의 경우 기본적으로 SECUREFILE 로 생성되기 때문에 BASICFILE 로 생성하려면 다음과 같이 database parameter 수정이 필요합니다. <br>
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **SQL> ALTER SYSTEM SET db_securefile=NEVER scope=spfile;** <br>
> &nbsp;&nbsp;&nbsp; Database 재시작 후 *initializer*를 통해 테이블을 생성하면 BASICFILE 옵션의 LOB type으로 생성됩니다.
<br>

#### 3.2.6 ORACLE_TEST

| # | Column Name | Data Type  | 비고                                            |
|---|-------------|------------|-------------------------------------------------|
| 1 | T_ID        | NUMBER     | Sequence(ORACLE_TEST_SEQ, Start=1, Increment=1) |
| 2 | COL_ROWID   | ROWID      |                                                 |
| 3 | COL_FLOAT   | FLOAT(126) |                                                 |
<hr>

#### 3.2.7 SQLSERVER_TEST

| # | Column Name        | Data Type      | 비고                          |
|---|--------------------|----------------|-------------------------------|
| 1 | T_ID               | INT            | ID=True, Start=1, Increment=1 |
| 2 | COL_DATE           | DATE           |                               |
| 3 | COL_TIME           | TIME           |                               |
| 4 | COL_DATETIMEOFFSET | DATETIMEOFFSET |                               |
<br>

## 4. Usage 
CDCBENCH는 다음의 4가지 기능으로 이루어진 프로그램입니다.
> * initializer: CDCBENCH에 사용될 Object 및 데이터를 초기화(생성, 삭제, 재생성)하는 기능
> * cdcbench: DML 부하테스트를 수행하는 기능
> * typebench: 데이터 타입을 테스트할 수 있는 기능

> ※ 기능에 사용되는 Object 및 데이터의 상세 내용은 **3. Table Information** 및 **5. Data Configuration**을 참고

### 4.1 initializer
CDCBENCH에 사용될 Object 및 데이터를 초기화하는 **initializer**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: initializer [option...][argument...]

  -h, --help: 
      initializer의 도움말을 출력합니다.
      
  -c, --create
      CDCBENCH에 사용될 Object 및 데이터를 생성합니다. 
      테이블 및 데이터가 존재하는 상태에서 사용할 경우 데이터만 append 됩니다.
      
  -d, --drop
      CDCBENCH와 관련된 Object를 모두 삭제합니다.
      
  -r, --reset
      CDCBENCH와 관련된 Object 및 데이터를 재생성 합니다. (--drop 수행 후 --create를 수행하는 방식)
      
  -s, --source
      initializer 대상을 config file의 source_database 환경으로 지정합니다. 
      --target(-t) / --both(-b) 옵션이 없을 경우 이 옵션이 사용됩니다.
      
  -t, --target
      initializer 대상을 config file의 target_database 환경으로 지정합니다.
  
  -b, --both
      initializer 대상을 config file의 source/target_database 모두로 지정합니다.
      초기 생성되는 데이터는 source와 target이 동일하게 됩니다.
      
  -f, --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 initializer를 실행합니다.
      -f/--config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 -f/--config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다. 
  
  -v, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

#### 4.1.1 initializer example
<pre>
> py initializer --create
  → CDCBENCH에 사용될 Object 및 데이터를 생성
  
> py initializer --drop --config target.ini
  → target.ini의 데이터베이스 정보를 바탕으로 CDCBENCH와 관련된 Object를 삭제
  
> py initializer --reset --both
  → default.ini의 source/target_database에 기입된 환경에 CDCBENCH와 관련된 Object 및 데이터를 삭제 후 재생성
</pre>



### 4.2 cdcbench
DML 부하테스트에 사용되는 **cdcbench**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: cdcbench [option...][argument...]

  -h, --help
      cdcbench의 도움말을 출력합니다.
      
  -i, --insert < number of data >
      < number of data >만큼 데이터를 insert 합니다.
      -c/--commit 옵션 미사용시 commit 단위는 1000 입니다.
      
  -c, --commit < commit units >
      insert 수행 시 commit 단위를 지정합니다. --insert 옵션과 함께 사용할 수 있습니다. 
      
  -s, --single
      insert를 single insert 방식으로 수행합니다 (기본적으로 multi insert). -i/--insert 옵션과 함께 사용할 수 있습니다.  
      
  -u, --update < start separate_col > < end separate_col >
      separate_col 컬럼의 값이 < start separate_col >에서 < end separate_col >까지인 데이터를 update 합니다.
      < start separate_col > 값은 < end separate_col > 값보다 클 수 없습니다.
      
  -d, --delete < start separate_col > < end separate_col >
      separate_col 컬럼의 값이 < start separate_col >에서 < end separate_col >까지인 데이터를 delete 합니다.
      < start separate_col > 값은 < end separate_col > 값보다 클 수 없습니다.
      
  -f, --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 cdcbench를 실행합니다.
      -f/--config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 -f/--config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다.
      
  -v, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

#### 4.2.1 cdcbench --insert example
<pre>
> py cdcbench --insert 10000
  → 10000건의 데이터를 1000건씩 commit하여 insert 합니다.

> py cdcbench --insert 20000 --commit 2000
  → 20000건의 데이터를 2000건씩 commit하여 insert 합니다.

> py cdcbench --insert 10000 --single
  → single insert 방식으로 10000건의 데이터를 1000건씩 commit하여 insert 합니다.
  
> py cdcbench --insert 10000 --config target.ini
  → target.ini file의 데이터베이스 정보로 10000건의 데이터를 1000건씩 commit하여 insert 합니다.
</pre>

#### 4.2.2 cdcbench --update example
<pre>
> py cdcbench --update 1 5
  → separate_col 컬럼의 값이 1 ~ 5인 row의 데이터를 update 합니다.
  
> py cdcbench --update 2 2
  → separate_col 컬럼의 값이 2인 row의 데이터를 update 합니다.
  
> py cdcbench --update 1 3 --config target.ini
  → target.ini file의 데이터베이스 정보로 separate_col 컬럼의 값이 1 ~ 3인 row의 데이터를 update 합니다.
</pre>

#### 4.2.3 cdcbench --delete example
<pre>
> py cdcbench --delete 1 5
  → separate_col 컬럼의 값이 1 ~ 5인 row의 데이터를 delete 합니다.
  
> py cdcbench --delete 3 3
  → separate_col 컬럼의 값이 3인 row의 데이터를 delete 합니다.
</pre>

### 4.3 typebench
데이터 타입 테스트에 사용되는 **typebench**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: typebench [option...][argument...]

  -h, --help
      typebench의 도움말을 출력합니다.
      
  -S|N|D|B|L|O|Q, --string|numeric|datetime|binary|lob|oracle|sqlserver
      옵션에 해당하는 테이블을 지정합니다. DML 옵션과 함께 사용될 수 있습니다.
      -O/--oracle, -Q/--sqlserver 옵션의 경우 config의 "[source_database] dbms_type"의 값이 
      해당 dbms로 설정되어 있어야 합니다.
      
  -i, --insert < number of data >
      지정한 카테고리 테이블에 < number of data >만큼의 데이터를 insert 합니다.
      -c/--commit 옵션 미사용시 commit 단위는 100 입니다.
      
  -c, --commit < commit units >
      insert 수행 시 commit 단위를 지정합니다. -i/--insert 옵션과 함께 사용할 수 있습니다.
      
  -u, --update < start t_id > < end t_id >
      지정한 카테고리 테이블에 t_id가 < start t_id > ~ < end t_id >인 데이터를 update 합니다.
      < start t_id > 값은 < end t_id > 값보다 클 수 없습니다.
      
  -d, --delete < start t_id > < end t_id >
      지정한 카테고리 테이블에 t_id가 < start t_id > ~ < end t_id >인 데이터를 delete 합니다.
      < start t_id > 값은 < end t_id > 값보다 클 수 없습니다.
      
  -f, --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 cdcbench를 실행합니다.
      -f/--config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 -f/--config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다.
      
  -v, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

#### 4.3.1 typebench example
<pre>
> typebench --string --insert 100
  → string_test 테이블에 100건의 데이터를 insert 합니다.
  
> typebench --numeric --update 1 10
  → numeric_test 테이블에서 t_id가 1 ~ 10인 데이터를 update 합니다. 
  
> typebench --lob --delete 1 10
  → lob_test 테이블에서 t_id가 1 ~ 10인 데이터를 delete 합니다.
</pre> 

<hr>

## 5. Data Configuration
CDCBENCH에 사용되는 데이터는 데이터 파일을 수정함으로서 사용자가 원하는 데이터 집합을 만들 수 있습니다. 

*initializer, cdcbench*에서 사용되는 **dml.dat** 데이터 파일을 통해 데이터 구성방법을 살펴보도록 하겠습니다.
<pre>
{
    "PRODUCT_NAME": [
        "BAKING MIXES",
        "BAKING NEEDS",
        ...
    ],
    "PRODUCT_DATE": [
        
    ] → (null)
}
</pre>

* 데이터 저장 방식은 JSON 포맷을 따르고 있습니다.
* 테이블의 컬럼 이름을 key 값으로, 컬럼에 사용할 데이터를 리스트로 관리하고 있습니다.
* 데이터 insert 및 update 시에는 컬럼에 해당하는 리스트에서 랜덤하게 값을 선택해 사용합니다.
* 해당 컬럼을 *null*로 입력하고 싶다면, 리스트를 비워두면 됩니다.
* 다음의 경우 올바르게 동작하지 않을 수 있습니다. 즉, 기존 형태는 유지하되, 리스트의 데이터만 수정해야 합니다.
  * 데이터 파일의 컬럼 자체가 삭제될 경우
  * Key 값 순서가 테이블 구조와 다를 경우
  * 데이터베이스의 컬럼명과 다를 경우

### 5.1 DML
DML (cdcbench) 데이터는 위 예시의 포맷을 참고해주시고, 다음의 특징을 가지고 있습니다.

* **PRODUCT_DATE** 컬럼은 "년-월-일 시:분:초" 형태를 가져야 합니다. (ex. "2017-03-05 07:34:07")  

### 5.2 String
*typebench --string*에서 활용되는 **string.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "COL_CHAR": [
        "BAKED BREAD/BUNS/ROLLS",
        "DRY BN/VEG/POTATO/RICE",
        ..., 
        "안성탕면(봉지)(125g)",
        "삼양라면(봉지)(120g)",
        ...,
        "寶貝",
        "朱四位"
    ],
    "COL_NCHAR": [
        ... 
    ],
    "COL_VARCHAR_B": [
        ...
    ],
    "COL_VARCHAR_C": [
        ...
    ],
    "COL_NVARCHAR": [
        ...
    ],
    "COL_TEXT": [
        "t_eng_0512_utf8.txt",
        "t_eng_1024_utf8.txt",
        ...
    ]
}
</pre>

* Configuration 중 NLS_LANG 값이 Database의 Character Set과 다를 경우 값이 정상적으로 입력되지 않을 수 있습니다.
* **COL_TEXT** 데이터는 데이터 파일(string.dat)에 직접 데이터를 입력하는 것이 아닌 입력할 데이터가 있는 파일 (source file)의 이름을 입력합니다.
  source file은 **./lob_files** 디렉토리에 존재해야 합니다.  
* **COL_TEXT** 컬럼은 문자열로 이루어진 .txt 확장자만 정상적으로 insert 할 수 있습니다. 그리고 파일 인코딩은 **UTF-8 (without BOM)** 이어야 합니다.
* source file의 크기는 약 1GB 미만까지 허용됩니다. (파일 크기를 체크하지는 않습니다.)

### 5.3 Numeric
*typebench -N/--numeric*에서 활용되는 **numeric.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "COL_BIT": [
        0,
        1,
        ...
    ],
    "COL_TINYINT": [
        0,
        133,
        ...
    ],
    "COL_SMALLINT": [
        -30017,
        -27709,
        ...
    ],
    "COL_MEDIUMINT": [
        -896628,
        7191502,
        ...
    ],
    "COL_INT": [
        -1152240375,
        -105989599,
        ...
    ],
    "COL_BIGINT": [
        -3052433639043326093,
        -3709709648893001583,
        ...
    ],
    "COL_DECIMAL": [
        -34125621836211200.462098274783004690,
        -982750.3220050934,
        ...
    ],
    "COL_NUMERIC": [
        -1287.2,
        -6193012803259243.57038339500467737089,
        ...
    ],
    "COL_FLOAT": [
        -896888184706.5061695683601565661,
        -50044720459540178012016988719806135631.0,
        ...
    ], 
    "COL_DOUBLE": [
        -7920248.687796348,
        -59800597928379.4473180290,
        ...
    ], 
    "COL_SMALLMONEY": [
        77112.4,
        -27.205,
        ...
    ],
    "COL_MONEY": [
        -846641696727418.75,
        846334227.0,
        ...
    ]
}
</pre>

### 5.4 DateTime
*typebench -D/--datetime*에서 활용되는 **datetime.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "COL_DATETIME": [
        "2061-01-23 07:22:24",
        "1996-10-16 22:22:12",
        ...
    ],
    "COL_TIMESTAMP": [
        "2005-02-12 23:26:21.508015",
        "2033-12-06 07:00:31.921650",
        ...
    ],
    "COL_TIMESTAMP2": [
        "9788-03-03 18:45:45.202941",
        "6505-04-22 09:41:45.749505",
        ...
    ],
    "COL_INTER_YEAR_MONTH": [
        [-149084120, 6],
        [7, 11],
        ...
    ],
    "COL_INTER_DAY_SEC": [
        [59, 21, 34, 27, 6],
        [270586765, 14, 32, 51, 950],
        ...
    ]
}
</pre>
* **COL_DATE** 컬럼은 "연-월-일 시:분:초" 의 형태를 가져야합니다. (ex. "2017-02-09 06:43:21")
* **COL_TIMESTAMP, COL_TIMESTAMP2** 컬럼은 "연-월-일 시:분:초.밀리초" 의 형태를 가져야합니다. (ex. "2017-02-12 14:23:39.48562")
* **COL_INTER_YEAR_MONTH** 컬럼은 [연, 월] 의 형태를 가져야 합니다. (ex. [6, 3] → 6년 3개월)
* **COL_INTER_DAY_SEC** 컬럼은 [일, 시, 분, 초, 밀리초] 의 형태를 가져야 합니다. (ex. [99, 23, 59, 59, 999999] → 99일 23시간 59분 59초 99999 밀리초)

### 5.5 Binary
*typebench -B/--binary*의 경우 별도의 데이터 파일이 존재하지 않고, 데이터 생성 함수를 사용합니다.
* **COL_BINARY, COL_VARBINARY** 컬럼은 1~1000 사이의 크기를 가진 임의의 이진 데이터를 생성합니다.
* **COL_LONG_BINARY** 컬럼은 1~2000 사이의 크기를 가진 임의의 이진 데이터를 생성합니다.

### 5.6 LOB
*typebench -L/--lob*에서 활용되는 **lob.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "COL_CLOB": [
        "t_eng_0512_utf8.txt",
        "t_eng_1024_utf8.txt",
        ..., 
        "t_kor_3964_utf8.txt",
        "t_kor_3965_utf8.txt"
    ],
    "COL_NCLOB": [
        "t_eng_0512_utf8.txt",
        "t_eng_1024_utf8.txt",
        ..., 
        "t_kor_3964_utf8.txt",
        "t_kor_3965_utf8.txt"
    ],
    "COL_BLOB": [
        "i_apple.png",
        "i_blue_rose.jpg",
        ...
    ]
}
</pre>
* lob 데이터는 데이터 파일(lob.dat)에 직접 데이터를 입력하는 것이 아닌 입력할 데이터가 있는 파일 (source file)의 이름을 입력합니다.
  source file은 **./lob_files** 디렉토리에 존재해야 합니다.  
* **COL_CLOB**, **COL_NCLOB** 컬럼은 문자열로 이루어진 .txt 확장자만 정상적으로 insert 할 수 있습니다. 그리고 파일 인코딩은 **UTF-8 (without BOM)** 이어야 합니다.
* source file의 크기는 약 1GB 미만까지 허용됩니다. (파일 크기를 체크하지는 않습니다.)

### 5.7 Oracle
*typebench -O/--oracle*에서 활용되는 **oracle.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
    "COL_FLOAT": [
        -2682546578656.66829904582251874864,
        863450387585623767262548820188856572.0,
        ...
    ]
</pre>
* **COL_ROWID** 컬럼은 'AAAShYAAFAAAAC9A'의 16자리 문자 뒤에, [0-9a-zA-Z]의 문자 중 랜덤으로 2글자를 덧붙입니다.

### 5.8 SQL Server
*typebench -Q/--sqlserver*에서 활용되는 **sqlserver.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
    "COL_DATE": [
        "4338-02-03",
        "3839-04-06",
        ...
    ],
    "COL_TIME": [
        "10:16:48.500675",
        "15:41:24.661137",
        ...
    ],
    "COL_DATETIMEOFFSET": [
        "2908-12-23 17:29:53.121071 +09:00",
        "3554-05-20 02:41:58.090052 +09:00",
        ...
    ]
</pre>

<hr>

## 6. Log
CDCBENCH의 log는 $CDCBENCH_HOME/logs/cdcbench.log에 저장됩니다. 기본적인 프로그램 동작뿐만 아니라 다음의 내용들을 포함하고 있습니다.
> * initializer 수행 시 초기 데이터 구성 정보
> * cdcbench 수행 결과 (DML 수행 정보, 소요 시간)
> * typebench 수행 결과 (DML 수행 정보, 소요 시간)

Config의 "sql_logging" 값에 따라 $CDCBENCH_HOME/logs/sql.log에 실행되는 SQL과 데이터값이 남게 됩니다.
 * 현재 구현상의 제약에 의해 대량의 데이터의 경우 일부 데이터값 로깅이 생략될 수 있습니다.
 