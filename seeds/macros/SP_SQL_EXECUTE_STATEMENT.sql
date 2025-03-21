
{% macro SP_SQL_EXECUTE_STATEMENT() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;
create or replace  PROCEDURE hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.SP_SQL_EXECUTE_STATEMENT(SQL_TEXT VARCHAR(16777216))
RETURNS integer
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN 
 EXECUTE IMMEDIATE :SQL_TEXT;
 return 0  ;  
   EXCEPTION
        WHEN statement_error THEN 
    RETURN 2;
        WHEN OTHER THEN
    RETURN 2;  
END';
        {% endset %}

    {% do run_query(query) %}

{% endmacro %} 

