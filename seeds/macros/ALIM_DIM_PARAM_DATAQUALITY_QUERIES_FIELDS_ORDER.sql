{% macro ALIM_DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;
CREATE OR REPLACE PROCEDURE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER()
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE 
var_select_statement String;
var_result_set resultset;  
var_insert_statement String;
var_result_set2 resultset;
var_RULE_ID INT;
var_FORMULA_CODE_SQL VARCHAR;  
var_RLS VARCHAR; 
var_STATUS VARCHAR;
TABLE_CURSOR CURSOR FOR 
      SELECT  
      RULE_ID
    , RLS  
    , STATUS
    , CONCAT (''SELECT TOP 0 * from( '',FORMULA_CODE_SQL,  '')  '') AS FORMULA_CODE_SQL
      FROM  
      HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.DIM_PARAM_DATAQUALITY_QUERIES 
     -- WHERE STATUS= ''A''
      ORDER BY 1 ;
 
BEGIN
   CREATE OR REPLACE TABLE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER (
	RULE_ID NUMBER(38,0), RLS VARCHAR(100) , COLUMN_NAME VARCHAR(100) , ORDINAL_POSITION NUMBER(38,0), STATUS VARCHAR(100) , CREATION_DATE TIMESTAMP_LTZ(9) );

  OPEN TABLE_CURSOR;
   loop
    FETCH TABLE_CURSOR INTO var_RULE_ID, var_RLS, var_STATUS, var_FORMULA_CODE_SQL ;
    IF(var_FORMULA_CODE_SQL <> '''') THEN   
    var_select_statement := ''CREATE OR REPLACE TABLE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.TABLE_COULMNORDERLIST AS ( ''||var_FORMULA_CODE_SQL ||'') ;'';
     var_result_set := (execute immediate :var_select_statement);
 
     
     var_insert_statement :=''INSERT INTO HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER  
     SELECT  distinct ''||var_RULE_ID ||'',''''''||var_RLS||'''''' ,  COLUMN_NAME,  ORDINAL_POSITION , ''''''||var_STATUS || '''''' ,  current_timestamp  
     FROM HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.information_schema.columns where table_name = ''''TABLE_COULMNORDERLIST''''   order by ORDINAL_POSITION '';  
     var_result_set2 := (execute immediate :var_insert_statement);
    ELSE
      BREAK;
    END IF;
  END LOOP;
  CLOSE table_cursor;
  DROP TABLE IF EXISTS HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.TABLE_COULMNORDERLIST ;
  RETURN 0;
END';
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}