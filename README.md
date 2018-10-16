CDCBENCH
========
CDCBENCH는 Python 기반의 ArkCDC Project 테스트 프로그램입니다. 

DML 부하테스트, 다양한 Data Type 테스트를 좀 더 편리하게 할 수 있었으면 합니다.

> 주의! 현재 문서에서 CDCBENCH (upper)와 cdcbench (lower)가 혼용되어 사용되고 있습니다. <br>
> 대문자의 경우 프로젝트로서 CDCBENCH, 소문자의 경우 기능으로서의 cdcbench로 알고 계시면 좋을 것 같습니다.  

## 0. Supported Environment
* Windows
* Linux

## 1. Installation
### 1.1 Python Install
CDCBENCH는 다음의 Python 버전에서 동작합니다.
 
Version >= **3.6.3** (https://www.python.org/downloads/)

> 환경별 Python 설치방법은 검색을 활용해주시기 바랍니다. <br>
> 그리고 이후 설치과정은 C:\Python36\; C:\Python36\Scripts; 두 경로를 환경변수 PATH에 추가했다는 전제하에 진행됩니다.

설치를 완료하고 나면 Python Library 관리도구인 pip를 최신버전으로 업그레이드합니다.
<pre>
# Windows
> python -m pip install --upgrade pip

# Linux
> pip install --upgrade pip
</pre>

### 1.2 Oracle Instant Client Install
Oracle Instant Client 설치는 다음과 같은 절차를 따릅니다.

#### Windows
1. 다음의 경로에서 대상 Database와 동일한 버전의 "Basic" Package를 다운로드합니다. <br>
   https://www.oracle.com/technetwork/topics/winx64soft-089540.html

2. 다운로드한 zip file을 적절한 위치에 압축해제 합니다. (ex. C:\util\oracle_instantclient_11_2)

3. 시스템 환경변수 "Path"에 압축해제한 경로를 추가합니다.

4. (선택) CDCBENCH 실행 후 TNS 관련 에러 발생시 Instant Client 디렉토리에 network\admin\tnsnames.ora 파일을 추가합니다.
   (network\admin 디렉토리는 기본적으로 포함되어 있지 않습니다.)
   <pre>
   # tnsnames.ora Network Configuration File: /data/db/product/network/admin/tnsnames.ora
   # Generated by Oracle configuration tools.
   ORA11 =
     (DESCRIPTION =
       (ADDRESS_LIST =
         (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.0.170)(PORT = 1521))
     )
     (CONNECT_DATA =
       (SERVICE_NAME = ORA11)
     )
   )
</pre>

### 1.3 CDCBENCH Download
https://lab.idatabank.com/gitlab/sangcheolpark/cdcbench/tags
> Line Separator (CRLF, LF)와 관련해 메시지가 올바르게 출력되지 않을 수도 있으니, CDCBENCH 설치 OS에서 다운로드 해주시기 바랍니다.


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
└─ requirements.txt | CDCBENCH 동작에 요구되는 라이브러리 목록
</pre>
> 이후 설치경로를 편의상 $CDCBENCH_HOME 이라고 칭합니다.

### 1.5 Python Library Install
설치가 완료된 후 CDCBENCH 실행에 필요한 Library를 다음의 절차로 설치합니다.
<pre>
> cd $CDCBENCH_HOME
> pip install -r requirements.txt
</pre>

<hr>

## 2. Configuration

### 2.1 Configuration File
* 모든 Configuration 파일은 **$CDCBENCH_HOME/conf** 디렉토리에 저장되어야 합니다. 
* *--config* 옵션을 사용하지 않을 경우 기본적으로 **default.ini**을 인식합니다.
* **default.ini** 파일과 동일한 포맷을 갖추고 있다면, *--config* 옵션을 통해 CDCBENCH 실행 시 configuration 파일을 변경할 수 있습니다.  

### 2.2 Configuration
Configuration은 크게 CDCBENCH 관련 설정 / Database 정보 / 초기화 데이터 설정으로 구분할 수 있습니다. 
정의 형식은 **Parameter = Value** 형식이며, **모든 파라미터는 값을 필수적으로 입력해야 합니다.**

* **[setting]**
  > CDCBENCH 동작과 관련된 파라미터로 다음과 같은 파라미터들을 가지고 있습니다.

  * log_level = [ ERROR | INFO | DEBUG ] &nbsp; (출력할 Log Level을 지정합니다.)    
  * nls_lang = [ *$NLS_LANG* ] &nbsp; (Oracle Server의 Character Set입니다. profile의 $NLS_LANG 값과 동일하게 입력하면 됩니다.)
  * lob_save = [ yes (y) | no (n) ] &nbsp; (datachecker --lob 옵션 사용시, DB에 저장된 LOB Data를 파일로 저장할지 여부를 결정합니다. 대소문자를 구분하지 않습니다.)
  <br>
* **[source(target)_database]**
  > 사용할 데이터베이스의 연결정보를 입력합니다.
  >  * target_database 영역은 datachecker 기능에서만 사용됩니다. 그 외의 initializer, cdcbench, typebench 수행 시에는 source_database 정보만을 사용합니다.
  >  * target_database가 필요없을 경우 source_database 영역과 동일하게 입력해주시면 됩니다. 

  * host_name = [ *IPv4 Format* ] &nbsp; (연결할 데이터베이스의 IP를 입력합니다.)
  * port = [ *1024 ~ 65535* ] &nbsp; (연결할 데이터베이스와 통신할 Port를 입력합니다.)
  * db_type = [ oracle ] &nbsp; (연결할 데이터베이스의 타입을 입력합니다.)
  * db_name = [ *db_name (instance_name)* ] &nbsp; (연결할 데이터베이스의 이름을 입력합니다.)
  * user_id = [ *user_name* ] &nbsp; (CDCBENCH를 사용할 데이터베이스 유저를 입력합니다.)
  * userpassword = [ *user_password* ] &nbsp; (CDCBENCH를 사용할 데이터베이스 유저의 패스워드를 입력합니다.)
  <br>
* **[initial_update(delete)_test_data]**
  > CDCBENCH 초기화(initializer --create, --reset) 시, update_test (delete_test) 테이블에 생성할 데이터의 양과 Commit 단위를 지정합니다.
  
  * total_num_of_data = [ *number >= 1* ] &nbsp; (테이블에 생성할 총 데이터의 양을 지정합니다.)
  * commit_unit = [ *number >= 1* ] &nbsp; (데이터 생성시 commit이 발생할 범위를 지정합니다.)
  
<hr>

## 3. Table Information
CDCBENCH에서 사용되는 테이블은 총 8개로 기능에 따라 사용되는 테이블이 다릅니다.
> * cdcbench: INSERT_TEST, UPDATE_TEST, DELETE_TEST
> * typebench: STRING_TEST, NUMERIC_TEST, DATETIME_TEST, BINARY_TEST, LOB_TEST

### 3.1 cdcbench
cdcbench 기능에 사용되는 테이블들은 다음의 동일한 구조를 가집니다.
#### 3.1.1 INSERT_TEST / UPDATE_TEST / DELETE_TEST
> * product_id ( NUMBER, PK, {TABLE_NAME}_SEQ(1001) ): PK 역할을 하는 컬럼. 시퀀스를 사용해 값을 증가시킵니다.
> * product_name ( VARCHAR2(30) ): 문자열 데이터를 저장하는 컬럼
> * product_date ( DATE ): 날짜형 데이터를 저장하는 컬럼
> * separate_col ( NUMBER ): 1로 시작하여 Commit이 발생할 때마다 1씩 증가되는 값을 가진 컬럼. --update/--delete 옵션 사용시 조건으로 사용됩니다.

### 3.2 typebench
이후 설명되는 테이블은 typebench 기능에서 사용되며, 다음의 컬럼을 PK로 동일하게 사용하고 있습니다.
> * t_id ( NUMBER, PK, {TABLE_NAME}_SEQ(1001) ): PK 컬럼. 시퀀스를 사용해 값을 증가시킵니다. 
--update/--delete 옵션 사용시 조건값으로 사용됩니다.

#### 3.2.1 STRING_TEST 
> * col_char ( CHAR(50) )
> * col_nchar ( NCHAR(50) )
> * col_varchar2_byte ( VARCHAR2(4000) )
> * col_varchar2_char ( VARCHAR2(4000 CHAR) )
> * col_nvarchar2 ( NVARCHAR2(2000) )

#### 3.2.2 NUMERIC_TEST
> * col_number ( NUMBER )
> * col_binary_float ( BINARY_FLOAT )

#### 3.2.3 DATETIME_TEST
> * col_date ( DATE )
> * col_timestamp ( TIMESTAMP )
> * col_inter_year_month ( INTERVAL YEAR(2) TO MONTH )
> * col_inter_day_sec ( INTERVAL DAY(2) TO SECOND(6) )

#### 3.2.4 BINARY_TEST
> * col_rowid ( ROWID )
> * col_urowid ( (U)ROWID )
> * col_raw ( RAW(2000 BYTE) )
> * col_long_raw ( LONG RAW )

#### 3.2.5 LOB_TEST
> * col_long_alias ( VARCHAR2(50) ): col_long_data에 저장된 데이터의 source file 이름 
> * col_long_data ( LONG )
> * col_clob_alias ( VARCHAR2(50) ): col_clob_data에 저장된 데이터의 source file 이름
> * col_clob_data ( CLOB )
> * col_nclob_alias ( VARCHAR2(50) ): col_nclob_data에 저장된 데이터의 source file 이름
> * col_nclob_data ( NCLOB )
> * col_blob_alias ( VARCHAR2(50) ): col_blob_data에 저장된 데이터의 source file 이름
> * col_blob_data ( BLOB )

<hr>

## 4. Usage 
CDCBENCH는 다음의 4가지 기능으로 이루어진 프로그램입니다.
> * initializer: CDCBENCH에 사용될 Object 및 데이터를 초기화(생성, 삭제, 재생성)하는 기능
> * cdcbench: DML 부하테스트를 수행하는 기능
> * typebench: 데이터 타입을 테스트할 수 있는 기능
> * datachecker: Source, Target Database에 있는 데이터를 비교하여 보고서로 생성하는 기능

> ※ 기능에 사용되는 Object 및 데이터의 상세 내용은 **3. Table Information** 및 **5. Data Configuration**을 참고

### 4.1 initializer
CDCBENCH에 사용될 Object 및 데이터를 초기화하는 **initializer**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: initializer [option...][argument...]

  -h, --help: 
      initializer의 도움말을 출력합니다.
      
  --create
      CDCBENCH에 사용될 Object 및 데이터를 생성합니다. 
      테이블 및 데이터가 존재하는 상태에서 사용할 경우 데이터만 append 됩니다.
      
  --drop
      CDCBENCH와 관련된 Object를 모두 삭제합니다.
      
  --reset
      CDCBENCH와 관련된 Object 및 데이터를 재생성 합니다. (--drop 수행 후 --create를 수행하는 방식)
      
  --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 initializer를 실행합니다.
      --config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 --config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다. 
  
  -v, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

#### 4.1.1 initializer example
<pre>
> py initializer --create
  → CDCBENCH에 사용될 Object 및 데이터를 생성
  
> py initializer --drop --config target.ini
  → target.ini의 데이터베이스 정보를 바탕으로 CDCBENCH와 관련된 Object를 삭제  
</pre>



### 4.2 cdcbench
DML 부하테스트에 사용되는 **cdcbench**는 다음의 옵션들을 통해 사용할 수 있습니다.
<pre>
usage: cdcbench [option...][argument...]

  -h, --help
      cdcbench의 도움말을 출력합니다.
      
  --insert < number of data>
      < number of data>만큼 데이터를 insert 합니다.
      --commit 옵션 미사용시 commit 단위는 1000 입니다.
      
  --commit < commit units>
      insert 수행 시 commit 단위를 지정합니다. --insert 옵션과 함께 사용할 수 있습니다. 
      
  --single
      insert를 single insert 방식으로 수행합니다 (기본적으로 multi insert). --insert 옵션과 함께 사용할 수 있습니다.  
      
  --update < start separate_col> < end separate_col>
      separate_col 컬럼의 값이 < start separate_col>에서 < end separate_col>까지인 데이터를 update 합니다.
      < start separate_col> 값은 < end separate_col> 값보다 클 수 없습니다.
      
  --delete < start separate_col> < end separate_col>
      separate_col 컬럼의 값이 < start separate_col>에서 < end separate_col>까지인 데이터를 delete 합니다.
      < start separate_col> 값은 < end separate_col> 값보다 클 수 없습니다.
      
  --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 cdcbench를 실행합니다.
      --config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 --config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다.
      
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
      
  --string | numeric | datetime | binary | lob
      옵션에 해당하는 데이터 타입을 지정합니다. DML 옵션과 함께 사용될 수 있습니다.
      
  --insert < number of data>
      지정한 카테고리 테이블에 < number of data>만큼의 데이터를 insert 합니다.
      --commit 옵션 미사용시 commit 단위는 100 입니다.
      
  --commit < commit units>
      insert 수행 시 commit 단위를 지정합니다. --insert 옵션과 함께 사용할 수 있습니다.
      
  --update < start t_id> < end t_id>
      지정한 카테고리 테이블에 t_id가 < start t_id> ~ < end t_id>인 데이터를 update 합니다.
      < start t_id> 값은 < end t_id> 값보다 클 수 없습니다.
      
  --delete < start t_id> < end t_id>
      지정한 카테고리 테이블에 t_id가 < start t_id> ~ < end t_id>인 데이터를 delete 합니다.
      < start t_id> 값은 < end t_id> 값보다 클 수 없습니다.
      
  --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 cdcbench를 실행합니다.
      --config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 --config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다.
      
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

### 4.4 datachecker
Source, Target Database에 있는 데이터를 조회하여 비교하고 보고서를 생성하는 기능입니다. 다음과 같은 옵션으로 사용할 수 있습니다. 
<pre>
usage: datachecker [option...][argument...]

  -h, --help
      datachecker의 도움말을 출력한다.
      
  --string | numeric | datetime | binary | lob
      지정한 category의 데이터 타입으로 datachecker를 수행합니다.
      
  --config [config_file_name]
      config file을 조회하거나 지정한 config file을 사용하여 cdcbench를 실행합니다.
      --config 옵션만 사용될 경우 해당 config file의 내용을 출력합니다. [config_file_name]을 지정하지 않을 경우 default.ini의 내용을 출력합니다.
      다른 옵션과 함께 --config 옵션을 사용할 경우 [config_file_name]의 설정을 가지고 initializer를 수행합니다.
      
  -v, --version
      CDCBENCH의 버전을 출력합니다.
</pre>

datachecker는 다음의 절차로 수행됩니다.
1. Source와 Target의 지정한 카테고리 테이블 컬럼 개수를 비교 합니다. 일치할 경우 다음 단계로, 불일치할 경우 비교 내용을 출력하고 종료합니다.
2. Source와 Target에서 지정한 카테고리 테이블의 row count를 조회하여 비교합니다. 일치할 경우 다음 단계로, 불일치할 경우 비교 내용을 출력하고 종료합니다.
3. Source와 Target에서 지정한 카테고리 테이블의 데이터를 전체 조회합니다.
4. Non-LOB와 LOB에 따라 비교 방법이 달라집니다. 
   1. Non-LOB 카테고리의 경우 Source와 Target의 동일한 컬럼끼리 데이터를 비교합니다.
   2. LOB 카테고리의 경우 Source와 Target의 동일한 컬럼을 Hash 값으로 변환하여 데이터를 비교합니다. <br>
      그리고 config "lob_save" 여부에 따라 비교한 데이터를 $CDCBENC_HOME/datachecker_report/lob-{timestamp}/에 실제 파일로 저장합니다.
5. Step 4를 전체 데이터에 대해 수행한 후 비교 결과를 $CDCBENCH_HOME/datachecker_report/{category}-{timestamp}.rpt에 저장합니다. 

<hr>

## 5. Data Configuration
CDCBENCH에 사용되는 데이터는 데이터 파일을 수정함으로서 사용자가 원하는 데이터 집합을 만들 수 있습니다. 

*initializer, cdcbench*에서 사용되는 **dml.dat** 데이터 파일을 통해 데이터 구성방법을 살펴보도록 하겠습니다.
<pre>
{
    "product_name": [
        "First Aid Kit",
        "Skin Brightening Gel",
        ...
    ],
    "product_date": [
        
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

* **product_date** 컬럼은 "년-월-일-시-분-초" 형태를 가져야 합니다. (ex. "2017-03-05-07-34-07")  

### 5.2 String
*typebench --string*에서 활용되는 **string.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "CHAR": [
        "First Aid Kit",
        "Skin Brightening Gel",
        ..., 
        "甁Bottle병",
        "t3tㅅ5"
    ],
    "NCHAR": [
        "First Aid Kit",
        ..., 
        "t3tㅅ5"
    ],
    "VARCHAR2_BYTE": [
        ...
    ],
    "VARCHAR2_CHAR": [
        ...
    ],
    "NVARCHAR2": [
        ...
    ]
}
</pre>

* Configuration 중 NLS_LANG 값이 Database의 Character Set과 다를 경우 값이 정상적으로 입력되지 않습니다.

### 5.3 Numeric
*typebench --numeric*에서 활용되는 **numeric.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "NUMBER": [
        1,
        99999999999999999,
        0,
        ..., 
        9999999999999999999999999999999999999999,
        999999999999999999999999999999999999999999
    ],
    "BINARY_FLOAT": [
        0.1,
        ..., 
        13.123678,
        113.1234568
    ]
}
</pre>

### 5.4 DateTime
*typebench --datetime*에서 활용되는 **datetime.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "DATE": [
        "2017-02-09 06:43:21",
        ..., 
        "2016-10-06 05:21:12",
        "2017-09-26 22:36:25"
    ],
    "TIMESTAMP": [
        "2017-02-12 14:23:39.48562",
        ...,
        "2017-01-07 07:03:32.15476",
        "2017-03-13 04:05:11.38731"
    ],
    "INTER_YEAR_MONTH": [
        [10, 0],
        ..., 
        [6, 3],
        [-2, 8]
    ],
    "INTER_DAY_SEC": [
        [10, 0, 0, 0, 0],
        ..., 
        [-99, 23, 59, 59, 999999],
        [99, 23, 59, 59, 999999]
    ]
}
</pre>
* **DATE** 컬럼은 "연-월-일 시:분:초" 의 형태를 가져야합니다. (ex. "2017-02-09 06:43:21")
* **TIMESTAMP** 컬럼은 "연-월-일 시:분:초.밀리초" 의 형태를 가져야합니다. (ex. "2017-02-12 14:23:39.48562")
* **INTER_YEAR_MONTH** 컬럼은 [연, 월] 의 형태를 가져야 합니다. (ex. [6, 3] → 6년 3개월)
* **INTER_DAY_SEC** 컬럼은 [일, 시, 분, 초, 밀리초] 의 형태를 가져야 합니다. (ex. [99, 23, 59, 59, 999999] → 99일 23시간 59분 59초 99999 밀리초)

### 5.5 Binary
*typebench --binary*의 경우 별도의 데이터 파일이 존재하지 않고, 데이터 생성 함수를 사용합니다.
* (U)ROWID 컬럼은 'AAAShYAAFAAAAC9A'의 16자리 문자 뒤에, [0-9a-zA-Z]의 문자 중 랜덤으로 2글자를 덧붙입니다.
* RAW 컬럼은 1~1000 사이의 크기를 가진 이진 데이터를 생성합니다.
* LONG RAW 컬럼은 1~2000 사이의 크기를 가진 이진 데이터를 생성합니다.

### 5.6 LOB
*typebench --lob*에서 활용되는 **lob.dat** 데이터는 다음의 형태와 특징을 가지고 있습니다.
<pre>
{
    "LONG": [
        "512_byte.txt",
        "1024_byte.txt",
        ..., 
        "korean_4.txt",
        "korean_5.txt"
    ],
    "CLOB": [
        "512_byte.txt",
        "1024_byte.txt",
        ..., 
        "korean_4.txt",
        "korean_5.txt"
    ],
    "NCLOB": [
        "512_byte.txt",
        "1024_byte.txt",
        ..., 
        "korean_4.txt",
        "korean_5.txt"
    ],
    "BLOB": [
        "apple.png",
        "rawtest2.jpg",
        "rawtest3.jpg"
    ]
}
</pre>
* lob 데이터는 데이터 파일(lob.dat)에 직접 데이터를 입력하는 것이 아닌 입력할 데이터가 있는 파일 (source file)의 이름을 입력합니다.
  source file은 **./lob_files** 디렉토리에 존재해야 합니다.  
* **LONG**, **CLOB**, **NCLOB** 컬럼은 문자열로 이루어진 .txt 확장자만 정상적으로 insert 할 수 있습니다. 그리고 파일 인코딩은 **UTF-8 (without BOM)** 이어야 합니다.
* source file의 크기는 약 1GB 미만까지 허용됩니다. (파일 크기를 체크하지는 않습니다.)

<hr>

## 6. Log
CDCBENCH의 log는 $CDCBENCH_HOME/cdcbench.log에 저장됩니다. 기본적인 프로그램 동작뿐만 아니라 다음의 내용들을 포함하고 있습니다.
> * initializer 수행 시 초기 데이터 구성 정보
> * cdcbench 수행 결과 (DML 수행 정보, 소요 시간)
> * typebench 수행 결과 (DML 수행 정보, 소요 시간)
> * datachecker 수행 결과 (Column & Row count 비교 정보, 전체 데이터 비교 결과, 상세 데이터 비교 결과 보고서 파일명, 소요 시간)