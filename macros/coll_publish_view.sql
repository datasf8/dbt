{% macro collate_publish_view () %}

       {% set query %}

    ---alter --
use database HRDP_PUB_{{ env_var('DBT_REGION') }}_DB;
USE WAREHOUSE HRDP_DBT_PREM_COLL_WH;
insert into HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.Collate_execution_publish_view  (qury,table_name)
with vw as
(
select 
        view_definition as qury,
        split_part(view_definition,' ',2) as test,
         table_name
    
from information_schema.views
where table_schema != 'INFORMATION_SCHEMA'
and TABLE_SCHEMA  in ('PMG_LND_SCH','CMP_PUB_SCH','ACK_PUB_SCH','LRN_PUB_SCH','CMN_PUB_SCH','PMG_PUB_SCH')
and test  in('or','OR')
and table_name not in ('FACT_VARIABLE_PAY_BY_COUANT_SNAPSHOT_VW')
)

select qury,table_name from vw;


    {% endset %}
  {% do run_query(query) %}
    {% set region= env_var('DBT_REGION')   %}
    {% set var_drop = run_query('select QURY from HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_publish_view where is_executed=\'false\' order by RN ;') %}
      {% for var_landing1 in var_drop %}
        {% do run_query(var_landing1.QURY) %}
        {% set modquery=var_landing1.QURY.replace("'","\\'")%}
        {% do run_query('update  HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_publish_view set is_executed=\'True\'  where QURY= \''+modquery+ '\'' ) %}
    {% endfor %}  
    {% endmacro %}