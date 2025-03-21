{% macro ALIM_FACT_DATAQUALITY_RESULTS() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;
CREATE OR REPLACE PROCEDURE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_FACT_DATAQUALITY_RESULTS("RUN_ID" NUMBER(38,0), "DB" VARCHAR(16777216), "SCH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE

    var_table_name_param VARCHAR DEFAULT  ''DIM_PARAM_DATAQUALITY_QUERIES''; 
    var_table_name_param2 VARCHAR DEFAULT  concat (:DB,''.'',:SCH, ''.'', ''DIM_PARAM_DATAQUALITY_QUERIES'');     
    var_table_name_fact_temp_dq   VARCHAR DEFAULT  ''TMP_DQ_RESULT''; 
    var_table_name_bu    VARCHAR DEFAULT ''DIM_ORGANIZATION'' ; 
    var_table_name_pf    VARCHAR DEFAULT ''DIM_PROFESSIONAL_FIELD'' ; 


    var_result_set_query_dq RESULTSET DEFAULT (
    SELECT  
      RULE_ID
    , RLS AS SUBJECT_DOMAIN 
    , CONCAT (''SELECT * FROM ( '',FORMULA_CODE_SQL , '') WHERE DQ_FLAG_TECH = 1 '') AS FORMULA_CODE_SQL
    FROM  
      HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.DIM_PARAM_DATAQUALITY_QUERIES 
     -- WHERE RULE_ID= 13
      ORDER BY 1 
 );
    TABLE_CURSOR CURSOR FOR var_result_set_query_dq;
 
var_run_id INTEGER DEFAULT 0; 
var_columnslist_select String;
var_columnslist_pivot String;
var_rule_id String;
var_select_statement String;
var_dq_sql_query String;
var_result_set resultset;
RULE_ID INT;
FORMULA_CODE_SQL VARCHAR;  
SUBJECT_DOMAIN VARCHAR;  


BEGIN

CREATE OR REPLACE TEMPORARY TABLE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.TMP_DQ_RESULT AS 
SELECT TOP 0 * FROM HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.FACT_DATAQUALITY_RESULTS;
 OPEN TABLE_CURSOR;
  LOOP
    FETCH TABLE_CURSOR INTO RULE_ID, SUBJECT_DOMAIN, FORMULA_CODE_SQL ;
    IF(FORMULA_CODE_SQL <> '''') THEN
   var_select_statement :=   
	    '' WITH QUERY_TEXT AS (
            SELECT * FROM ( '' ||  FORMULA_CODE_SQL  ||'' ) q)  
         , QUERY_COLUMN_NAME AS (
            SELECT object_construct_keep_null(*) obj_row FROM QUERY_TEXT )
         , QUERY_RESULT_SELECT AS ( SELECT distinct key COL_NAME FROM QUERY_COLUMN_NAME , LATERAL FLATTEN (input => obj_row)
         
         ), QUERY_RESULT_PIVOT AS (

         SELECT distinct key COL_NAME FROM QUERY_COLUMN_NAME , LATERAL FLATTEN (input => obj_row)
         WHERE COL_NAME NOT IN (''''PF_CODE_TECH'''',''''BU_CODE_TECH'''',''''DQ_FLAG_TECH'''', ''''USER_ID_TECH'''')
         ) 
         SELECT  
              (SELECT LISTAGG(CONCAT(''''IFNULL ("'''',COL_NAME,''''"::STRING, '''''''''''''''' )'''','''' AS '''',''''"'''',COL_NAME,''''"''''), '''','''') FROM QUERY_RESULT_SELECT ) AS columnslist_select 
            , (SELECT LISTAGG(CONCAT(''''"'''',COL_NAME,''''"'''' ), '''','''') FROM QUERY_RESULT_PIVOT) as columnslist_pivot
            , (select FORMULA_CODE_SQL from  '' || :DB ||''.'' ||:SCH ||''.'' || :var_table_name_param ||'' where RULE_ID= ''||RULE_ID||'') as sql_query
            ,''||RULE_ID||'' AS rule_id
             ;''; 
 
 
var_result_set := (execute immediate :var_select_statement);

  let c1 cursor for var_result_set;
      for row_variable in c1 do
      var_columnslist_select := row_variable.columnslist_select;
      var_columnslist_pivot := row_variable.columnslist_pivot;
      var_rule_id := row_variable.rule_id;
      var_dq_sql_query := concat (''select * from ( '', row_variable.sql_query, '') WHERE  DQ_FLAG_TECH = 1 '');
 
   End For; 
           
--CREATE OR REPLACE SEQUENCE seq_01 START = 1 INCREMENT = 1;

 IF (var_columnslist_select <> '''') THEN
 
  EXECUTE IMMEDIATE   
          ''INSERT INTO '' || :DB ||''.'' ||:SCH ||''.'' || :var_table_name_fact_temp_dq ||''
            (RUN_ID
            , RULE_ID
            , SUBJECT_DOMAIN
            , BUSINESS_UNIT_CODE  
            , PROFESSIONAL_FIELD_CODE  
            , USER_ID
            , BUSINESS_UNIT_PK  
            , PROFESSIONAL_FIELD_PK     
            , ROW_ID
            , COLUMN_NAME
            , COLUMN_VALUE
            ) 
            
           WITH INIT AS 
           ( SELECT 
              row_number () over (order by current_timestamp) AS ROW_ID
             ,''|| var_columnslist_select 
               || '' FROM (''|| var_dq_sql_query  || ''))
               , PIVOT AS (
               
               SELECT ''
               ||:RUN_ID 
               || '' AS RUN_ID, ''
               || var_rule_id 
               || '' AS RULE_ID , ''|| '''''''' || TRIM (:SUBJECT_DOMAIN) || ''''''   AS SUBJECT_DOMAIN   
               , BU_CODE_TECH AS BUSINESS_UNIT_CODE 
               , PF_CODE_TECH AS PROFESSIONAL_FIELD_CODE
               , USER_ID_TECH AS USER_ID
               , ROW_ID AS ROW_ID     
                   , column_name AS COLUMN_NAME
                   , column_value COLUMN_VALUE
               FROM INIT 
               UNPIVOT 
               (column_value for column_name in ( ''|| var_columnslist_pivot ||''  )) AS u)
               select 
               P.RUN_ID
             , P.RULE_ID
             , P.SUBJECT_DOMAIN
             , P.BUSINESS_UNIT_CODE  
             , P.PROFESSIONAL_FIELD_CODE  
             , P.USER_ID
             , ORG.BUSINESS_UNIT_PK  
             , PF.PROFESSIONAL_FIELD_PK     
             , ROW_ID
             , P.COLUMN_NAME
             , P.COLUMN_VALUE
               from PIVOT P
               LEFT OUTER JOIN '' || :DB ||''.'' ||:SCH ||''.'' || :var_table_name_pf ||'' PF ON P.PROFESSIONAL_FIELD_CODE = PF.PROFESSIONAL_FIELD_CODE
               LEFT OUTER JOIN '' || :DB ||''.'' ||:SCH ||''.'' || :var_table_name_bu ||'' ORG ON P.BUSINESS_UNIT_CODE = ORG.BUSINESS_UNIT_CODE; '';

 END IF; 

 ELSE
   BREAK;
 END IF;
 END LOOP;
 CLOSE table_cursor;
 RETURN ''OK'';
   EXCEPTION
        WHEN statement_error THEN 
    RETURN OBJECT_CONSTRUCT(''Error type'', ''STATEMENT_ERROR'',
                            ''SQLCODE'', sqlcode,
                            ''SQLERRM'', sqlerrm,
                            ''SQLSTATE'', sqlstate);
        WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(''Error type'', ''Other error'',
                            ''SQLCODE'', sqlcode,
                            ''SQLERRM'', sqlerrm,
                            ''SQLSTATE'', sqlstate); 
END';
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
