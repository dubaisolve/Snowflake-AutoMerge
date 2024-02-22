# MERGER_BUILDER_GEN and POC_REFRESH Procedures 🚀

## MERGER_BUILDER_GEN Procedure
### Scalability and Flexibility 🌍
- **Dynamic SQL Generation**: Effortlessly scales to handle a large number of tables (like 1000), avoiding the need for writing individual merge scripts for each. 📈
- **Adaptability**: Easily accommodates new tables or schema changes without updating each script. 🔄

### Advantages 🔥
- **Reduced Maintenance**: A single, dynamic script is much easier to maintain than hundreds or thousands of individual scripts. 🛠️
- **Uniform Process**: Ensures consistency in data handling across various tables. 🧩

### Considerations 🤔
- **Performance**: Keep an eye on the performance, especially with large datasets. ⏱️
- **Testing**: Ensure thorough testing for handling different table structures and data types. 🧪
- **Documentation**: Essential for future maintenance and clarity for new team members. 📚

## POC_REFRESH Procedure
### Automation and Efficiency 🏗️
- **Automated Table Processing**: Iterates over a preloaded list of tables for automated merging, reducing manual effort. 🔄

## Conclusion 🌟
- **Practical and Necessary**: For managing a vast number of tables, this dynamic and automated approach is not just practical but essential for efficiency. 🛠️
- **Smart Database Programming**: A savvy use of database scripting to handle large-scale data operations in Snowflake. 💡


## Simplification of data load for Snowflake.
This piece of work describes how data load form files in ADLS folder storage to physical tables in SF Warehouse can be simplified by using developed procedure. As a growing need for importing data from files grows especially add-hock scenarios and non-regular load there is a need for data load method that would avoid duplication and provide adequate flexibility in code development. Something that can be reused and could be executed in future automation's.

Procedure is developed in JavaScript and compiled on Snowflake.

Other reasons to consider using Auto merge procedure.
As many of you will be using ADF to sink data with SF data-warehouse you would soon find out that ADF does not support data sync without staging in blob storage on top of that Snowflake connector does not support gen2 adls storage in above configuration. 

Example:
![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/b6620242-8691-4619-af3f-70cbec81c3a8)


Error when trying to run Pipe with mentioned configuration:
![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/1bef05b4-357c-415b-b8f9-f47f39dcb74b)



SOURCE

![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/c8460f77-8077-4163-a6d4-188145180f36)

SYNC
![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/680f3599-33e5-4ca3-ae84-029a424f951c)


MAPPING

![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/6aae926c-c157-42e8-9ff1-e6bab3af2119)



Settings to allow to stage files (required for table sync from source to target) 


So you either left with loading data to HUB (datalake) storage deduped and then import to SF where required. If you have this approach then COPY INTO command can be executed as part of custom procedure and another procedure run form ADF LookUP activity which is now supported. See bellow.

![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/6aabb8da-c3f8-4990-b4e7-425aef6c5390)



Alternatively to COPY INTO you can use "Auto Merge Builder" which takes care of accidental reinsert, logging and is easier to replicate.

Main features of Procedure.
Data load from file (ADLS folder) to (physical table in SF) table without duplication.
Error Handling and Process Log in to separate table to monitor data load.
## Look up code in this repository : 

Automerger v2 is more recent version that offers Auto encryption on fields commented as 'PII' in comment of column and better tmiestamp conversion from text to timestamp.  (1900-12-01 00:00:00) format

like so : COMMENT [IF EXISTS] ON COLUMN <table_name>.<column_name> IS 'PII'; -- will be encrypted using static but if required variable parameters ''JohnConnorMustDie'' , ''skycargo_adhc_SELECT_DEV'', ''aes-gcm''  ( add another input var to proceure and edit in code where appropriate) 

## Code Flow explanation
1. Reads supplied variables such as Table_name, Schema_name and Stage_name.
2. According to supplied parameters uses them in select statement so to select correct data from correct files from the stage and to merge in to corresponding table in DB in to specified schema.
3. Refers to Constraints_VW to look up constraints of the target tables and to use them for "ON" clause in merge.
4. Creates vars for log table.
5. Creates temp table to insert variables for dynamic SQL statement of merge procedure for passed parameter (table_name, schema_name and stage_name).
   *Does it in correct ordinal position order so you insert correct column from correct column
   *prepares select statement with CAST function that looks up target table datatype and adjusts accordingly. Currently supports text, float and number, date is not supported so please in staging tables use date as text. (potential to enhance code to WH standard date type)
   *pads NLV function to "on and when" clause columns parameters to be able to treat nulls in comparison of the merge.
   *ON clause columns are not put into where clause of merge statement to avoid constant update. 
6. Puts table output to vars and use them in dynamic SQL MERGE. This part can be simplified to just pass parameter to memory not temp table.
7. If number of columns in ON clause is equal to total number of columns meaning every column in the table is contributing to unique row then other merge statements without where comparison is used instead to construct Dynamic SQL merge statement. ( if {} else {} )
8. If successfully merged then write to log table and write inserted and updated rows count.
9. If not successful then throw error and write full error to log table also write 0 into inserted and updated columns.


Used case for Using AutoMerge Builder Diagram.

![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/143a5f47-e9e5-45c2-9451-d5b9b7506f4a)


## Used Case solution flow explanation.
Data factory refreshes data from MFR Oracle DB (ex BO reports) by appending files on ADLS. SharePoint sites are scanned for appended files and sync them with files on adls folder. Once files are updated ADF kicks off SF procedure.
POC_REFRESH procedure checks the list of files available in the stage and tables via control view. It then passes list of VARIABLES per (table_name,schema_name,stage_name) to MERGER_BUILDER_GEN in a while Loop for execution.
View is an inner join between list of files in external stage which is on (ADLS) and a list of tables set by user in PROC_LIST table that user wants to load in required sequence. If physical table is missing in DB but exists in PROC_LIST error will be logged into GEN_LOG table. 
MERGER_BUILDER_GEN Checks CONSTRAINT_VW which holds information for each table columns and unique constrains to use them per table to creates dynamic SQL VAR's for "Select , ON, when matched , when not matched and insert" parts of Generic Merge function. This creates a fully qualified Merge code per each table provided from the list of previous step. Errors will be logged to GEN_LOG table as well as number of records inserted or updated.
FACT views contain extra calculations, data type conversions and joins applied to get required aggregation as per business requirement. These views are source for BI tools. Currently no Security views are applied but solution is available and documented in RBAC section.

## Reading the Log
Looking back on the error log table and reflecting on the errors to fix them. 1ST run was importing data, 2nd run on the same dateset(files) to see if we are reinserting dubs or not. 

LOG_ID	number of rows inserted	number of rows updated	PROC_NAME	FINISHED	USER_NAME	USER_ROLE	STATUS	MESSAGE
952	0	0	MASTER_PL_ACCOUNTS	18/10/2020 12:15	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
951	0	0	SAFETY_LEADING	18/10/2020 12:10	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
950	0	0	SAFETY_LAGGING	18/10/2020 12:10	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
949	0	0	SAFETY_INCIDENTS	18/10/2020 12:10	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
948	0	0	SAFETY_BUSINESS	18/10/2020 12:09	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
947	0	16	PL_LC	18/10/2020 12:09	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
946	0	2	PENALTIES	18/10/2020 12:09	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
945	0	50	OPS_GH	18/10/2020 12:09	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
944	0	16	OPS_CARGO	18/10/2020 12:09	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
943	0	18	NPS	18/10/2020 12:08	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
942	0	0	MPR_KPI	18/10/2020 12:08	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
941	0	0	MASTER_SHAREHOLDING	18/10/2020 12:08	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
940	0	0	MASTER_SAFETY_TARGETS	18/10/2020 12:08	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
939	0	0	MASTER_SAFETY_COMPANY	18/10/2020 12:08	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
937	0	0	MASTER_MPR_CITY	18/10/2020 12:07	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
936	0	0	MASTER_KPI_TARGETS	18/10/2020 12:07	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
935	0	0	MASTER_GEOGRAPHY	18/10/2020 12:07	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
934	0	0	MASTER_EXCHANGE_RATES	18/10/2020 12:07	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
933	0	0	MASTER_DATE	18/10/2020 12:07	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
932	0	0	DEBTOR	18/10/2020 12:06	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
931	0	0	DASHBOARD_KPIS	18/10/2020 12:06	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
930	0	2	CONTRACTS	18/10/2020 12:06	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
929	0	0	COMPANY	18/10/2020 12:06	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
928	0	0	CASH_FLOW	18/10/2020 12:06	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
927	0	0	CAPEX_BUDGET	18/10/2020 12:05	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
926	0	2	CAPEX_ACTUAL_UNBUDGETED	18/10/2020 12:05	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
925	0	0	CAPEX_ACTUAL_BUDGETED	18/10/2020 12:05	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
924	0	0	ASSET_REGISTER	18/10/2020 12:05	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
923	0	0	AIRLINES	18/10/2020 12:05	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
922	140	0	DEBTOR	18/10/2020 12:03	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	done	
921	0	0	DEBTOR	18/10/2020 16:00	EACOMPANY_X__GENERIC_DEV	EA_COMPANY_X__DEVELOPER_DEV	Failed	100183 | State: P0000\\n  Message: SQL compilation error:
syntax error line 17 at position 4 unexpected ')'.
syntax error line 24 at position 15 unexpected 'MONTH_ID'.
syntax error line 24 at position 23 unexpected ',  (fixed)


