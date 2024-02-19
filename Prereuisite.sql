--Create Log Table:
--EDWH_DEV.WS_EA_DNATA_DEV.GEN_LOG definition

create or replace TABLE GEN_LOG (

LOG_ID NUMBER(38,0) autoincrement,

"number of rows inserted" NUMBER(38,0),

"number of rows updated" NUMBER(38,0),

PROC_NAME VARCHAR(100),

FINISHED TIMESTAMP_NTZ(9),

USER_NAME VARCHAR(100),

USER_ROLE VARCHAR(100),

STATUS VARCHAR(50),

MESSAGE VARCHAR(2000)

);

--Data is loaded based on an existing table structure which must match source file columns count.
--Example:

--EDWH_DEV.WS_EA_DNATA_DEV.AIRLINES definition

create or replace TABLE AIRLINES (

CONSOLIDATED_AIRLINE_CODE VARCHAR(80),

POSSIBLE_CUSTOMER_NAME VARCHAR(100),

CUSTOMER_TYPE VARCHAR(70),

CONSOLIDATED_AIRLINE_NAME VARCHAR(90),

constraint CONSOLIDATED_AIRLINE_CODE unique (CONSOLIDATED_AIRLINE_CODE),

constraint CUSTOMER_TYPE unique (CUSTOMER_TYPE)

);



--File in stage is AIRLINES.CSV has same column number in same order, not necessary has to have same headers as they will be aliased automatically to created table column names as above.



--Make sure you have required file format set or use default ones(refer to SF documentation)

--ALTER FILE FORMAT "EDWH_DEV"."WS_EA_DNATA_DEV".CSV SET COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = ----TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

--Tables must be appended to have constraints which then will be used for MERGE ON clause in merge statement. Constraint name must match Column name.


ALTER TABLE AIRLINES ADD CONSTRAINT CONSOLIDATED_AIRLINE_CODE UNIQUE (CONSOLIDATED_AIRLINE_CODE);

ALTER TABLE AIRLINES ADD CONSTRAINT CUSTOMER_TYPE UNIQUE (CUSTOMER_TYPE);

--You have stage set up and you can view files in it.
list @my_stage;

CREATE OR REPLACE VIEW CONSTRAINS_VW AS 
SELECT 
        tbl.table_schema, 
        tbl.table_name, 
        con.constraint_name,
        col.data_type
 FROM   EDWH_DEV.information_schema.table_constraints con 
        INNER JOIN EDWH_DEV.information_schema.tables tbl 
                ON con.table_name = tbl.table_name 
                  AND con.constraint_schema = tbl.table_schema 
        INNER JOIN EDWH_DEV.information_schema.columns col 
                ON tbl.table_name = col.table_name 
                   AND con.constraint_name = col.column_name 
                   AND con.constraint_schema = col.table_schema 
--usage --- need to name contrains same as column names then you can use them in table
--ALTER TABLE EDWH_DEV.WS_EA_DNATA_DEV.AIRLINES ADD CONSTRAINT CONSOLIDATED_AIRLINE_CODE UNIQUE (CONSOLIDATED_AIRLINE_CODE); 
--ALTER TABLE EDWH_DEV.WS_EA_DNATA_DEV.AIRLINES ADD CONSTRAINT CUSTOMER_TYPE UNIQUE (CUSTOMER_TYPE);
--ALTER TABLE EDWH_DEV.WS_EA_DNATA_DEV.AIRLINES DROP CONSTRAINT CONSOLIDATED_AIRLINE_CODE ;
--ALTER TABLE EDWH_DEV.WS_EA_DNATA_DEV.AIRLINES DROP CONSTRAINT CUSTOMER_TYPE ;
                   
 WHERE  con.constraint_type  in ('PRIMARY KEY', 'UNIQUE');	