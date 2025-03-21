 
{% macro ALIM_LOG_DATAQUALITY_RUNS() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;
CREATE OR REPLACE PROCEDURE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_LOG_DATAQUALITY_RUNS("DB" VARCHAR(16777216), "SCH" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
 
    var_condition INTEGER DEFAULT 0;
    var_run_id INTEGER DEFAULT 0;    
    var_table_name VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'',''LOG_DATAQUALITY_RUNS'') ;   
    var_result_set_check_runs RESULTSET DEFAULT (select COUNT(*) AS NB from IDENTIFIER(:var_table_name) WHERE END_DATE IS NULL );
    var_cursor_check_runs CURSOR FOR var_result_set_check_runs;


    var_result_set_run_id RESULTSET DEFAULT (select ifnull(MAX(RUN_ID),0)+1 as run_id from IDENTIFIER(:var_table_name)  );
    var_cursor_run_id CURSOR FOR var_result_set_run_id;

BEGIN

  FOR row_variable IN var_cursor_check_runs DO
      var_condition :=  row_variable.NB;
    END FOR;

 IF  ( var_condition > 0) THEN  
 return -1;
 ELSE 
 
  FOR row_variable2 IN var_cursor_run_id DO
        var_run_id := row_variable2.run_id;
    END FOR; 
 INSERT INTO IDENTIFIER(:var_table_name) (RUN_ID, START_DATE, STATUS,MESSAGE, DATE_SK) SELECT :var_run_id, current_timestamp, ''Start'','''',to_varchar (current_timestamp::date, ''YYYYMMDD'') ; 
 
 

 return var_run_id;
 END IF;
END';
        {% endset %}

    {% do run_query(query) %}

{% endmacro %} 