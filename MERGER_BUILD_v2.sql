CREATE OR REPLACE PROCEDURE "MERGER_BUILDER_GEN"("TABLE_NAME" VARCHAR(200), "SCHEMA_NAME" VARCHAR(200), "STAGE_NAME" VARCHAR(200))
RETURNS VARCHAR(32000)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
var result;
snowflake.execute( {sqlText: "begin transaction;"});
var my_sql_command = `SELECT 
	0 AS "number of rows inserted"
	, 0 as "number of rows updated"
	,'` + TABLE_NAME + `' AS proc_name
	,CURRENT_TIMESTAMP() AS FINISHED
	,CURRENT_USER() AS USER_NAME 
	,CURRENT_ROLE() USER_ROLE
	,'Failed' as status`;
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command} );
    var result_set1 = statement1.execute();
  result_set1.next();
	var column1 = result_set1.getColumnValue(1);
	var column2 = result_set1.getColumnValue(2);
	var column3 = result_set1.getColumnValue(3);
	var column4 = result_set1.getColumnValue(4);
	var column5 = result_set1.getColumnValue(5);
	var column6 = result_set1.getColumnValue(6);
	var column7 = result_set1.getColumnValue(7);

try {
	var v_sql_stmt = `CREATE OR REPLACE temporary TABLE vars_of_merger_dyn00 AS 
					SELECT  
					COL_NAMES_SELECT	
					,REPLACE(listagg (distinct' nvl(tgt."'||cons.constraint_name||'",'
					||CASE  WHEN cons.data_type ='FLOAT' THEN '0' 
							WHEN cons.data_type ='NUMBER' THEN '0'
							WHEN cons.data_type ='DATE' THEN '''1900-12-01'''
							WHEN cons.data_type ='TIMESTAMP_NTZ' THEN '''1900-12-01 00:00:00'''
							ELSE '-999999' END||') = nvl(src."' 
							||cons.constraint_name ||'",'
					||CASE  WHEN cons.data_type ='FLOAT' THEN '0' 
							WHEN cons.data_type ='NUMBER' THEN '0'
							WHEN cons.data_type ='DATE' THEN '''1900-12-01'''
							WHEN cons.data_type ='TIMESTAMP_NTZ' THEN '''1900-12-01 00:00:00'''
							ELSE '-999999' END  ,') and \n') ||')','-999999','''''') AS dd
					,REPLACE(COL_NAMES_WHEN,'-999999','''''') AS COL_NAMES_WHEN
					,COL_NAMES_SET
					,COL_NAMES_INS
					,COL_NAMES_INS1
					FROM (
					SELECT 
					 InTab.TABLE_NAME              
					,listagg (decode("COMMENT",'PII','to_varchar(encrypt(','')||' cast($'   ||InTab.ORDINAL_POSITION || ' as ' || intab.DATA_TYPE ||')'||decode("COMMENT",'PII',', ''JohnConnorMustDie'' , ''skycargo_adhc_SELECT_DEV'', ''aes-gcm''))','')||' as "' ||InTab.COLUMN_NAME,'", \n') WITHIN GROUP ( ORDER BY ORDINAL_POSITION asc ) ||'"'  AS Col_Names_select
					,listagg (' nvl(tgt."'  || CASE WHEN intab.CM IS null and "COMMENT" is NULL THEN InTab.COLUMN_NAME ELSE NULL end  || '", '
					||CASE  WHEN intab.data_type ='FLOAT' THEN '0' 
							WHEN intab.data_type ='NUMBER' THEN '0'
							WHEN intab.data_type ='DATE' THEN '''1900-12-01'''
							WHEN intab.data_type ='TIMESTAMP_NTZ' THEN '''1900-12-01 00:00:00''' ELSE '-999999' END
					||') != nvl(src."' ||InTab.COLUMN_NAME||'",'||
					  CASE  WHEN intab.data_type ='FLOAT' THEN '0' 
							WHEN intab.data_type ='NUMBER' THEN '0'
							WHEN intab.data_type ='DATE' THEN '''1900-12-01'''
							WHEN intab.data_type ='TIMESTAMP_NTZ' THEN '''1900-12-01 00:00:00''' ELSE '-999999' END 
					,') OR\n') WITHIN GROUP ( ORDER BY ORDINAL_POSITION asc ) ||')' AS Col_Names_when
					,listagg (' tgt."'  ||CASE WHEN intab.CM IS NULL THEN InTab.COLUMN_NAME ELSE NULL end || '"= src."' ||InTab.COLUMN_NAME , '",\n') WITHIN GROUP ( ORDER BY ORDINAL_POSITION asc ) ||'"' AS Col_Names_set
					,listagg ( '"'||InTab.COLUMN_NAME,'",\n') WITHIN GROUP ( ORDER BY ORDINAL_POSITION asc ) ||'"' AS Col_Names_ins
					,listagg ( ' src."'  ||InTab.COLUMN_NAME,'",\n') WITHIN GROUP ( ORDER BY InTab.ORDINAL_POSITION asc ) ||'"' AS Col_Names_ins1 
					,listagg (ORDINAL_POSITION,',') WITHIN GROUP ( ORDER BY ORDINAL_POSITION asc ) ORDINAL_POSITION
					FROM (
					SELECT 
					"COMMENT"
					,InTab.TABLE_NAME              
					,InTab.COLUMN_NAME
					,InTab.ORDINAL_POSITION
					,intab.DATA_TYPE
					,cons.CONSTRAINT_NAME AS CM
					FROM INFORMATION_SCHEMA.COLUMNS InTab 
					LEFT JOIN constrains_vw cons ON cons.table_name = intab.table_name AND InTab.COLUMN_NAME = cons.CONSTRAINT_NAME
					where intab.TABLE_SCHEMA = '`+ SCHEMA_NAME +`'
					AND intab.TABLE_NAME = '`+ TABLE_NAME +`'
					GROUP BY 
					"COMMENT"
					,InTab.TABLE_NAME
					,InTab.COLUMN_NAME 
					,InTab.COLUMN_NAME
					,InTab.ORDINAL_POSITION
					,intab.DATA_TYPE
					,CONSTRAINT_NAME
					ORDER BY InTab.TABLE_NAME,InTab.ORDINAL_POSITION ) InTab
					GROUP BY TABLE_NAME
					ORDER BY TABLE_NAME,ORDINAL_POSITION
					) tt
					LEFT JOIN constrains_vw cons ON cons.table_name = tt.table_name
					GROUP BY
					COL_NAMES_SELECT	
					,COL_NAMES_WHEN
					,COL_NAMES_SET
					,COL_NAMES_INS
					,COL_NAMES_INS1;` ; 
	
    var rs_clip_name = snowflake.execute ({sqlText: v_sql_stmt});
   
    var my_sql_command1 = `SELECT Col_Names_select,dd,Col_Names_when,Col_Names_set,Col_Names_ins,Col_Names_ins1 FROM vars_of_merger_dyn00;`; 
    
    var statement2 = snowflake.createStatement( {sqlText: my_sql_command1} );
    var result_set = statement2.execute();
  	result_set.next();
	var Col_Names_select = result_set.getColumnValue(1);
	var dd = result_set.getColumnValue(2);
	var Col_Names_when = result_set.getColumnValue(3);
	var Col_Names_set = result_set.getColumnValue(4);
	var Col_Names_ins = result_set.getColumnValue(5);
	var Col_Names_ins1 = result_set.getColumnValue(6);

if (Col_Names_set == '"') 
{ 
var my_sql_command2 = `MERGE INTO EDWH_DEV.`+ SCHEMA_NAME +`.`+ TABLE_NAME +` AS tgt
USING 
( select
`+ Col_Names_select +`
from 
@` + STAGE_NAME + `/` + TABLE_NAME + `.csv  (file_format => 'CSV') )
AS src

ON ( `+ dd +`
	 )

WHEN NOT MATCHED
THEN INSERT ( `+ Col_Names_ins +`)
VALUES 
(`+ Col_Names_ins1 +`); `; 
    var rs_clip_name2 = snowflake.execute ({sqlText: my_sql_command2});

snowflake.createStatement( { sqlText: `INSERT INTO GEN_LOG
("number of rows inserted", "number of rows updated", proc_name , FINISHED, USER_NAME, USER_ROLE, STATUS, MESSAGE)
 SELECT "number of rows inserted", 0 as "number of rows updated", '` + TABLE_NAME + `' AS proc_name  , sysdate(), CURRENT_USER() ,CURRENT_ROLE(),'done' as status ,'' AS message
        FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()));`} ).execute();

} 
else 
{
var my_sql_command2 = `MERGE INTO EDWH_DEV.`+ SCHEMA_NAME +`.`+ TABLE_NAME +` AS tgt
USING 
( select
`+ Col_Names_select +`
from 
@` + STAGE_NAME + `/` + TABLE_NAME + `.csv  (file_format => 'CSV') )
AS src
ON ( `+ dd +`
	 )
WHEN MATCHED
AND `+ Col_Names_when +`
THEN UPDATE SET
`+ Col_Names_set +`
WHEN NOT MATCHED
THEN INSERT ( `+ Col_Names_ins +`)
VALUES 
(`+ Col_Names_ins1 +`); `; 
    var rs_clip_name2 = snowflake.execute ({sqlText: my_sql_command2});

snowflake.createStatement( { sqlText: `INSERT INTO GEN_LOG
("number of rows inserted", "number of rows updated", proc_name , FINISHED, USER_NAME, USER_ROLE, STATUS, MESSAGE)
 SELECT "number of rows inserted","number of rows updated", '` + TABLE_NAME + `' AS proc_name  , sysdate(), CURRENT_USER() ,CURRENT_ROLE(),'done' as status ,'' AS message
        FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()));`} ).execute();   

}
     snowflake.execute( {sqlText: "commit;"} );
    result = "Succeeded" + my_sql_command2 ;
} catch (err) {
  snowflake.execute({
      sqlText: `insert into GEN_LOG VALUES (DEFAULT,?,?,?,?,?,?,?,?)`
      ,binds: [column1, column2, column3 ,column4 , column5 , column6 ,column7 , err.code + " | State: " + err.state + "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt ]
      });
     snowflake.execute( {sqlText: "commit;"} );
     return 'Failed.' + my_sql_command2 ;
}
return result;

$$;