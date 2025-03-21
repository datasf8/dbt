
{% macro ALIM_DIM_PROFESSIONAL_FIELD() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;
 CREATE OR REPLACE PROCEDURE  ALIM_DIM_PROFESSIONAL_FIELD("DB" VARCHAR(16777216), "SCH" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE 
    var_table_name VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'',''DIM_PROFESSIONAL_FIELD'') ;   
BEGIN

 
CREATE OR REPLACE TABLE IDENTIFIER(:var_table_name) AS 
select
ROW_NUMBER() OVER (ORDER BY PROFESSIONAL_FIELD_CODE ) AS PROFESSIONAL_FIELD_PK
, PROFESSIONAL_FIELD_CODE 
, PROFESSIONAL_FIELD_NAME_EN AS PROFESSIONAL_FIELD_NAME
from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H02_JOB_ARCHITECTURE_EU_{{ env_var('DBT_REGION') }}_PRIVATE.PROFESSIONAL_FIELD
 
union select -1,''ALL'',''ALL''
union select -99, ''not defined'',''not defined''
;
return 1 ;
END';

        {% endset %}

    {% do run_query(query) %}

{% endmacro %} 