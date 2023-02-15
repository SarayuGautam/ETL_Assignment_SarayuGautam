from setup_connection import bhatbhateniWHConnection, bhatbhateniWHCursor

bhatbhateniWHCursor.execute('Create or replace database BHATBHATENI_WH;')
bhatbhateniWHCursor.execute('Create or replace schema DW_STG;')
bhatbhateniWHCursor.execute('Create or replace schema DW_TMP;')
bhatbhateniWHCursor.execute('Create or replace schema DW_TGT;')

bhatbhateniWHCursor.execute('use database BHATBHATENI_WH')

bhatbhateniWHCursor.execute('use schema DW_STG')

bhatbhateniWHCursor.execute(
    '''create or replace table
    STG_D_BHATBHATENI_CNTRY_T ( id NUMBER, country_desc VARCHAR(256),PRIMARY KEY (id));''')
bhatbhateniWHCursor.execute(
    '''create or replace table
    STG_D_BHATBHATENI_RGN_T(id NUMBER,country_id NUMBER,region_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (country_id) references STG_D_BHATBHATENI_CNTRY_T(id) );''')
bhatbhateniWHCursor.execute(
    '''create or replace table
    STG_D_BHATBHATENI_LOCN_T(id NUMBER,region_id NUMBER,store_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (region_id) references STG_D_BHATBHATENI_RGN_T(id) );''')
bhatbhateniWHCursor.execute(
    '''create or replace table
    STG_D_BHATBHATENI_CTGRY_T(id NUMBER,category_desc VARCHAR(1024),PRIMARY KEY (id));''')
bhatbhateniWHCursor.execute(
    '''create or replace table
    STG_D_BHATBHATENI_SUB_CTGRY_T(id NUMBER,category_id NUMBER,subcategory_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (category_id) references STG_D_BHATBHATENI_CTGRY_T(id) );''')
bhatbhateniWHCursor.execute(
    '''create or replace table
     STG_D_BHATBHATENI_PDT_T(id NUMBER,subcategory_id NUMBER,product_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (subcategory_id) references STG_D_BHATBHATENI_SUB_CTGRY_T(id));''')
bhatbhateniWHCursor.execute(
    '''create or replace table
    STG_D_BHATBHATENI_CUSTOMER_T(id NUMBER,customer_first_name VARCHAR(256),customer_middle_name VARCHAR(256),customer_last_name VARCHAR(256),customer_address VARCHAR(256) ,primary key (id));''')
bhatbhateniWHCursor.execute('''create or replace table
STG_F_BHATBHATENI_SLS_T(id NUMBER,store_id NUMBER NOT NULL,product_id NUMBER NOT NULL,customer_id NUMBER,transaction_time TIMESTAMP,quantity NUMBER,amount NUMBER(20,2),discount NUMBER(20,2),primary key (id),FOREIGN KEY (store_id) references STG_D_BHATBHATENI_LOCN_T(id),FOREIGN KEY (product_id) references STG_D_BHATBHATENI_PDT_T(id),FOREIGN KEY (customer_id) references STG_D_BHATBHATENI_CUSTOMER_T(id));''')

######################################################################################################################################

bhatbhateniWHCursor.execute("use schema DW_TMP;")

bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_D_BHATBHATENI_CNTRY_T
(
  CNTRY_KY NUMBER NOT NULL AUTOINCREMENT,
  CNTRY_ID NUMBER,
  CNTRY_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT CNTRY_PK PRIMARY key (CNTRY_KY)
);""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_D_BHATBHATENI_RGN_T
(

  RGN_KY NUMBER NOT NULL AUTOINCREMENT,
  RGN_ID NUMBER,
  CNTRY_KY NUMBER,
  RGN_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT RGN_PK PRIMARY key (RGN_KY),
  CONSTRAINT CNTRY_FK FOREIGN key (CNTRY_KY) REFERENCES TMP_D_BHATBHATENI_CNTRY_T(CNTRY_KY)
);
""")

bhatbhateniWHCursor.execute("""
create or replace TABLE TMP_D_BHATBHATENI_LOCN_T (
	LOCN_KY NUMBER(38,0) NOT NULL AUTOINCREMENT,
	LOCN_ID NUMBER(38,0),
	RGN_KY NUMBER(38,0),
	LOCN_DESC VARCHAR(50),
	OPEN_CLOSE_CD VARCHAR(1),
	ROW_INSRT_TMS TIMESTAMP_NTZ(9),
	ROW_UPDT_TMS TIMESTAMP_NTZ(9),
	constraint LOCN_PK primary key (LOCN_KY),
	constraint RGN_FK foreign key (RGN_KY) references TMP_D_BHATBHATENI_RGN_T(RGN_KY)
);
""")
#
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_D_BHATBHATENI_CTGRY_T
(

  CTGRY_KY NUMBER AUTOINCREMENT,
  CTGRY_ID NUMBER,
  CTGRY_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT CTGRY_PK PRIMARY KEY (CTGRY_KY)
);
""")
#
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_D_BHATBHATENI_SUB_CTGRY_T
(

  SUB_CTGRY_KY NUMBER AUTOINCREMENT,
  SUB_CTGRY_ID NUMBER,
  CTGRY_KY NUMBER,
  SUB_CTGRY_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT SUB_CTGRY_PK PRIMARY KEY (SUB_CTGRY_KY),
  CONSTRAINT CTGRY_FK FOREIGN KEY (CTGRY_KY) REFERENCES TMP_D_BHATBHATENI_CTGRY_T(CTGRY_KY)
);
""")
#


bhatbhateniWHCursor.execute("""
CREATE  OR REPLACE TABLE TMP_D_BHATBHATENI_PDT_T
(

  PDT_KY NUMBER AUTOINCREMENT,
  PDT_ID NUMBER,
  SUB_CTGRY_KY NUMBER,
  PDT_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT PDT_PK PRIMARY KEY (PDT_KY),
  CONSTRAINT SUB_CTGRY_FK FOREIGN KEY (SUB_CTGRY_KY) REFERENCES TMP_D_BHATBHATENI_SUB_CTGRY_T(SUB_CTGRY_KY)
);
""")

bhatbhateniWHCursor.execute("""
CREATE or replace TABLE  TMP_D_BHATBHATENI_CUSTOMER_T
(

  CUSTOMER_KY NUMBER AUTOINCREMENT,
  CUSTOMER_ID NUMBER,
  CUSTOMER_FST_NM VARCHAR(20),
  CUSTOMER_MID_NM VARCHAR(20),
  CUSTOMER_LST_NM VARCHAR(20),
  CUSTOMER_ADDR VARCHAR(20),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT CUSTOMER_PK PRIMARY KEY (CUSTOMER_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_F_BHATBHATENI_SLS_T
(
  SLS_ID NUMBER,
  LOCN_KY NUMBER,
  PDT_KY NUMBER,
  CUSTOMER_KY NUMBER,
  TRANSACTION_TIME TIMESTAMP_NTZ,
  QTY NUMBER,
  AMT NUMBER(10,2),
  DSCNT NUMBER(10,2),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT SLS_PK PRIMARY KEY (SLS_ID),
  CONSTRAINT LOCN_FK FOREIGN KEY (LOCN_KY) REFERENCES TMP_D_BHATBHATENI_LOCN_T(LOCN_KY),
  CONSTRAINT PDT_FK FOREIGN KEY (PDT_KY) REFERENCES TMP_D_BHATBHATENI_PDT_T(PDT_KY),
  CONSTRAINT CUSTOMER_FK FOREIGN KEY (CUSTOMER_KY) REFERENCES TMP_D_BHATBHATENI_CUSTOMER_T(CUSTOMER_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE TABLE IF NOT EXISTS TMP_D_BHATBHATENI_TIME_YEAR_T
(
  ID NUMBER,
  YEAR_KY NUMBER NOT NULL,
  YEAR_START_DATE DATE NOT NULL,
  YEAR_END_DATE DATE NOT NULL,
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT YEAR_PK PRIMARY KEY (YEAR_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_D_BHATBHATENI_TIME_HALF_YEAR_T
(
  ID NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_START_DATE DATE NOT NULL,
  HALF_YEAR_END_DATE DATE NOT NULL,
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT HALF_YEAR_PK PRIMARY KEY (HALF_YEAR_KY),
  CONSTRAINT YEAR_FK FOREIGN KEY (YEAR_KY) REFERENCES TMP_D_BHATBHATENI_TIME_YEAR_T(YEAR_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_D_BHATBHATENI_TIME_QUARTER_T
(
  ID NUMBER NOT NULL,
  QUARTER_KY NUMBER,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER NOT NULL,
  QUARTER_START_DATE DATE NOT NULL,
  QUARTER_END_DATE DATE NOT NULL,
  OPEN_CLOSE_CD VARCHAR(1),  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT QUARTER_PK PRIMARY KEY (QUARTER_KY),
  CONSTRAINT HALF_YEAR_FK FOREIGN KEY (HALF_YEAR_KY) REFERENCES TMP_D_BHATBHATENI_TIME_HALF_YEAR_T(HALF_YEAR_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE TABLE IF NOT EXISTS TMP_D_BHATBHATENI_TIME_MONTH_T
(
  ID NUMBER NOT NULL,
  MONTH_KY NUMBER,
  QUARTER_KY NUMBER NOT NULL,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER NOT NULL,
  MONTH_START_DATE DATE NOT NULL,
  MONTH_END_DATE DATE NOT NULL,
  CONSTRAINT MONTH_PK PRIMARY KEY (MONTH_KY),
  CONSTRAINT QUARTER_FK FOREIGN KEY (QUARTER_KY) REFERENCES TMP_D_BHATBHATENI_TIME_QUARTER_T(QUARTER_KY));
""")
bhatbhateniWHCursor.execute("""
CREATE TABLE IF NOT EXISTS TMP_D_BHATBHATENI_TIME_DAY_T
(
  ID NUMBER NOT NULL,
  DAY_KY NUMBER,
  MONTH_KY NUMBER,
  QUARTER_KY NUMBER NOT NULL,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER NOT NULL,
  DAY_START_TIME TIMESTAMP_NTZ NOT NULL,
  DAY_END_TIME TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT DAY_PK PRIMARY KEY (DAY_KY),
  CONSTRAINT WEEK_PK FOREIGN KEY (MONTH_KY) REFERENCES TMP_D_BHATBHATENI_TIME_MONTH_T(MONTH_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TMP_F_BHATBHATENI_SLS_T
(
  SLS_ID NUMBER,
  LOCN_KY NUMBER,
  PDT_KY NUMBER,
  CUSTOMER_KY NUMBER,
  TRANSACTION_TIME TIMESTAMP_NTZ,
  QTY NUMBER,
  AMT NUMBER(10,2),
  DSCNT NUMBER(10,2),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT SLS_PK PRIMARY KEY (SLS_ID),
  CONSTRAINT LOCN_FK FOREIGN KEY (LOCN_KY) REFERENCES TMP_D_BHATBHATENI_LOCN_T(LOCN_KY),
  CONSTRAINT PDT_FK FOREIGN KEY (PDT_KY) REFERENCES TMP_D_BHATBHATENI_PDT_T(PDT_KY),
  CONSTRAINT CUSTOMER_FK FOREIGN KEY (CUSTOMER_KY) REFERENCES TMP_D_BHATBHATENI_CUSTOMER_T(CUSTOMER_KY)
);
""")

bhatbhateniWHCursor.execute('''
CREATE OR REPLACE TABLE TMP_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T
(
	PDT_KY NUMBER NOT NULL,
	LOCN_KY NUMBER NOT NULL,
  CTGRY_KY NUMBER NOT NULL,
	MONTH_KY NUMBER NOT NULL,
	TOTAL_QTY NUMBER,
	TOTAL_AMT NUMBER(10,2),
	TOTAL_DSCNT NUMBER(10,2),
	ROW_INSRT_TMS TIMESTAMP_NTZ NOT NULL,
	ROW_UPDT_TMS TIMESTAMP_NTZ NOT NULL,
	CONSTRAINT PDT_FK FOREIGN KEY (PDT_KY) REFERENCES TMP_D_BHATBHATENI_PDT_T(PDT_KY),
  CONSTRAINT LOC_FK FOREIGN KEY (LOCN_KY) REFERENCES TMP_D_BHATBHATENI_LOCN_T(LOCN_KY),
	CONSTRAINT CAT_FK FOREIGN KEY (CTGRY_KY) REFERENCES TMP_D_BHATBHATENI_CTGRY_T(CTGRY_KY),
	CONSTRAINT MONTH_FK FOREIGN KEY (MONTH_KY) REFERENCES TMP_D_BHATBHATENI_TIME_MONTH_T(MONTH_KY)
);''')


######################################################################################################################
bhatbhateniWHCursor.execute("use schema DW_TGT")

bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_D_BHATBHATENI_CNTRY_T
(
  CNTRY_KY NUMBER NOT NULL AUTOINCREMENT,
  CNTRY_ID NUMBER,
  CNTRY_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT CNTRY_PK PRIMARY key (CNTRY_KY)
);""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_D_BHATBHATENI_RGN_T
(

  RGN_KY NUMBER NOT NULL AUTOINCREMENT,
  RGN_ID NUMBER,
  CNTRY_KY NUMBER,
  RGN_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT RGN_PK PRIMARY key (RGN_KY),
  CONSTRAINT CNTRY_FK FOREIGN key (CNTRY_KY) REFERENCES TGT_D_BHATBHATENI_CNTRY_T(CNTRY_KY)
);
""")

bhatbhateniWHCursor.execute("""
create or replace TABLE TGT_D_BHATBHATENI_LOCN_T (
	LOCN_ID NUMBER(38,0),
	LOCN_KY NUMBER(38,0) NOT NULL,
	RGN_KY NUMBER(38,0),
	LOCN_DESC VARCHAR(50),
	OPEN_CLOSE_CD VARCHAR(1),
	ROW_INSRT_TMS TIMESTAMP_NTZ(9),
	ROW_UPDT_TMS TIMESTAMP_NTZ(9),
	constraint LOCN_PK primary key (LOCN_KY),
	constraint RGN_FK foreign key (RGN_KY) references TGT_D_BHATBHATENI_RGN_T(RGN_KY)
);
""")
#
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_D_BHATBHATENI_CTGRY_T
(

  CTGRY_KY NUMBER AUTOINCREMENT,
  CTGRY_ID NUMBER,
  CTGRY_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT CTGRY_PK PRIMARY KEY (CTGRY_KY)
);
""")
#
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_D_BHATBHATENI_SUB_CTGRY_T
(

  SUB_CTGRY_KY NUMBER AUTOINCREMENT,
  SUB_CTGRY_ID NUMBER,
  CTGRY_KY NUMBER,
  SUB_CTGRY_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT SUB_CTGRY_PK PRIMARY KEY (SUB_CTGRY_KY),
  CONSTRAINT CTGRY_FK FOREIGN KEY (CTGRY_KY) REFERENCES TGT_D_BHATBHATENI_CTGRY_T(CTGRY_KY)
);
""")
#


bhatbhateniWHCursor.execute("""
CREATE  OR REPLACE TABLE TGT_D_BHATBHATENI_PDT_T
(

  PDT_KY NUMBER AUTOINCREMENT,
  PDT_ID NUMBER,
  SUB_CTGRY_KY NUMBER,
  PDT_DESC VARCHAR(50),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT PDT_PK PRIMARY KEY (PDT_KY),
  CONSTRAINT SUB_CTGRY_FK FOREIGN KEY (SUB_CTGRY_KY) REFERENCES TGT_D_BHATBHATENI_SUB_CTGRY_T(SUB_CTGRY_KY)
);
""")
#
bhatbhateniWHCursor.execute("""
CREATE or replace TABLE  TGT_D_BHATBHATENI_CUSTOMER_T
(

  CUSTOMER_KY NUMBER AUTOINCREMENT,
  CUSTOMER_ID NUMBER,
  CUSTOMER_FST_NM VARCHAR(20),
  CUSTOMER_MID_NM VARCHAR(20),
  CUSTOMER_LST_NM VARCHAR(20),
  CUSTOMER_ADDR VARCHAR(20),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT CUSTOMER_PK PRIMARY KEY (CUSTOMER_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_F_BHATBHATENI_SLS_T
(
  SLS_ID NUMBER,
  LOCN_KY NUMBER,
  PDT_KY NUMBER,
  CUSTOMER_KY NUMBER,
  TRANSACTION_TIME TIMESTAMP_NTZ,
  QTY NUMBER,
  AMT NUMBER(10,2),
  DSCNT NUMBER(10,2),
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT SLS_PK PRIMARY KEY (SLS_ID),
  CONSTRAINT LOCN_FK FOREIGN KEY (LOCN_KY) REFERENCES TGT_D_BHATBHATENI_LOCN_T(LOCN_KY),
  CONSTRAINT PDT_FK FOREIGN KEY (PDT_KY) REFERENCES TGT_D_BHATBHATENI_PDT_T(PDT_KY),
  CONSTRAINT CUSTOMER_FK FOREIGN KEY (CUSTOMER_KY) REFERENCES TGT_D_BHATBHATENI_CUSTOMER_T(CUSTOMER_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE  or replace  TABLE TGT_D_BHATBHATENI_TIME_YEAR_T
(
  ID NUMBER,
  YEAR_KY NUMBER NOT NULL,
  YEAR_START_DATE DATE NOT NULL,
  YEAR_END_DATE DATE NOT NULL,
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT YEAR_PK PRIMARY KEY (YEAR_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_D_BHATBHATENI_TIME_HALF_YEAR_T
(
  ID NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_START_DATE DATE NOT NULL,
  HALF_YEAR_END_DATE DATE NOT NULL,
  OPEN_CLOSE_CD VARCHAR(1),
  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT HALF_YEAR_PK PRIMARY KEY (HALF_YEAR_KY),
  CONSTRAINT YEAR_FK FOREIGN KEY (YEAR_KY) REFERENCES TGT_D_BHATBHATENI_TIME_YEAR_T(YEAR_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE OR REPLACE TABLE TGT_D_BHATBHATENI_TIME_QUARTER_T
(
  ID NUMBER NOT NULL,
  QUARTER_KY NUMBER,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER NOT NULL,
  QUARTER_START_DATE DATE NOT NULL,
  QUARTER_END_DATE DATE NOT NULL,
  OPEN_CLOSE_CD VARCHAR(1),  ROW_INSRT_TMS TIMESTAMP_NTZ,
  ROW_UPDT_TMS TIMESTAMP_NTZ,
  CONSTRAINT QUARTER_PK PRIMARY KEY (QUARTER_KY),
  CONSTRAINT HALF_YEAR_FK FOREIGN KEY (HALF_YEAR_KY) REFERENCES TGT_D_BHATBHATENI_TIME_HALF_YEAR_T(HALF_YEAR_KY)
);
""")
bhatbhateniWHCursor.execute("""
CREATE TABLE IF NOT EXISTS TGT_D_BHATBHATENI_TIME_MONTH_T
(
  ID NUMBER NOT NULL,
  MONTH_KY NUMBER,
  QUARTER_KY NUMBER NOT NULL,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER NOT NULL,
  MONTH_START_DATE DATE NOT NULL,
  MONTH_END_DATE DATE NOT NULL,
  CONSTRAINT MONTH_PK PRIMARY KEY (MONTH_KY),
  CONSTRAINT QUARTER_FK FOREIGN KEY (QUARTER_KY) REFERENCES TGT_D_BHATBHATENI_TIME_QUARTER_T(QUARTER_KY));
""")
bhatbhateniWHCursor.execute("""
CREATE or replace TABLE TGT_D_BHATBHATENI_TIME_DAY_T
(
  ID NUMBER NOT NULL,
  DAY_KY NUMBER,
  MONTH_KY NUMBER,
  QUARTER_KY NUMBER NOT NULL,
  YEAR_KY NUMBER NOT NULL,
  HALF_YEAR_KY NUMBER NOT NULL,
  DAY_START_TIME TIMESTAMP_NTZ NOT NULL,
  DAY_END_TIME TIMESTAMP_NTZ NOT NULL,
  CONSTRAINT DAY_PK PRIMARY KEY (DAY_KY),
  CONSTRAINT WEEK_PK FOREIGN KEY (MONTH_KY) REFERENCES TGT_D_BHATBHATENI_TIME_MONTH_T(MONTH_KY)
);
""")

bhatbhateniWHConnection.close()
