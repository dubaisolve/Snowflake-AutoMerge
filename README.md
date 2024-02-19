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


![image](https://github.com/dubaisolve/Snowflake-AutoMerge/assets/130452748/c2a80511-62f6-4f59-ae62-dcd55838ce34)


