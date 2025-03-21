{% macro collate_landing () %}



 {% set region= env_var('DBT_REGION')   %}
    {% set var_drop = run_query('select QURY from HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_landing_drop where is_executed=\'false\' order by RN ;') %}
      {% for var_landing1 in var_drop %}
        {% do run_query(var_landing1.QURY) %}
        {% do run_query('update  HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_landing_drop set is_executed=\'True\'  where QURY= \''+var_landing1.QURY+ '\'' ) %}
    {% endfor %}   
-- landing starts --
    {% set query %}

      

    ---alter --
use database HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
USE WAREHOUSE HRDP_DBT_PREM_COLL_WH;

insert into HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.Collate_execution_landing   (qury,table_name)
with tabs as (

		select
			TABLE_CATALOG as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_TYPE as table_type,
			TABLE_SCHEMA as table_owner,
			null as table_comment
		from INFORMATION_SCHEMA.TABLES where TABLE_TYPE not in ('VIEW')

    ),

    cols as (

        select
            table_catalog as table_database,
            table_schema,
            table_name,
            column_name,
            ordinal_position as column_index,
            data_type as column_type,
			null as column_comment
        from information_schema.columns

    )
,final as
(
    select 
	'ALTER TABLE IF EXISTS '||	tabs.table_database ||'.'|| tabs.table_schema||'."'|| tabs.table_name||  '" rename to ' ||
    tabs.table_database ||'.'|| tabs.table_schema||'."'|| tabs.table_name||'_COLLATE";' as qury,
    tabs.table_name
		
    from tabs
    join cols on tabs.table_database = cols.table_database 
    and tabs.table_schema = cols.table_schema
    and tabs.table_name = cols.table_name
    and tabs.table_name not like '%VW%'
    and tabs.table_schema in ('CMN_LND_SCH','LRN_LND_SCH','PMG_LND_SCH','SDDS_LND_SCH')
    
    --and tabs.table_name not in ilike '%_test'
    order by column_index
  )
  
  select  distinct qury,table_name from final;

---create --
insert into HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.Collate_execution_landing   (qury,table_name)
with tabs as (

		select
			TABLE_CATALOG as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_TYPE as table_type,
			TABLE_SCHEMA as table_owner,
			null as table_comment
		from INFORMATION_SCHEMA.TABLES where TABLE_TYPE not in ('VIEW')

    ),

    cols as (

        select
            table_catalog as table_database,
            table_schema,
            table_name,
            column_name,
            ordinal_position as column_index,
            data_type as column_type,
			null as column_comment
        from information_schema.columns

    )
,final as
(
    select 
	'CREATE OR REPLACE TABLE '||	tabs.table_database ||'.'|| tabs.table_schema||'."'|| replace(tabs.table_name,'_COLLATE','')||  '" like ' ||
    tabs.table_database ||'.'|| tabs.table_schema||'."'|| tabs.table_name ||'_COLLATE";' as qury,
    tabs.table_name
		
    from tabs
    join cols on tabs.table_database = cols.table_database 
    and tabs.table_schema = cols.table_schema
    and tabs.table_name = cols.table_name
    and tabs.table_name not like '%VW%'
    and tabs.table_schema in ('CMN_LND_SCH','LRN_LND_SCH','PMG_LND_SCH','SDDS_LND_SCH')
    
   --and tabs.table_name not in ilike '%_test'
    order by column_index
  )
  
  select  distinct qury,table_name from final;
  
  ---insert --
insert into HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.Collate_execution_landing   (qury,table_name)
with tabs as (

		select
			TABLE_CATALOG as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_TYPE as table_type,
			TABLE_SCHEMA as table_owner,
			null as table_comment
		from INFORMATION_SCHEMA.TABLES where TABLE_TYPE not in ('VIEW')

    ),

    cols as (

        select
            table_catalog as table_database,
            table_schema,
            table_name,
            column_name,
            ordinal_position as column_index,
            data_type as column_type,
			null as column_comment
        from information_schema.columns

    )
,final as
(
    select 
	'INSERT INTO '||	tabs.table_database ||'.'|| tabs.table_schema||'."'|| replace(tabs.table_name,'_COLLATE','')||  '" select * from ' ||
    tabs.table_database ||'.'|| tabs.table_schema||'."'|| tabs.table_name ||'_COLLATE";' as qury,
    tabs.table_name
		
    from tabs
    join cols on tabs.table_database = cols.table_database 
    and tabs.table_schema = cols.table_schema
    and tabs.table_name = cols.table_name
    and tabs.table_name not like '%VW%'
    and tabs.table_schema in ('CMN_LND_SCH','LRN_LND_SCH','PMG_LND_SCH','SDDS_LND_SCH')
    
   -- and tabs.table_name not in ilike '%_test'
    order by column_index
  )
  
  select  distinct qury,table_name from final;

  
  {% endset %}
  {% do run_query(query) %}
    {% set region= env_var('DBT_REGION')   %}
    {% set var_lan = run_query('select QURY from HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_landing  where is_executed=\'false\' order by table_name,QURY ;') %}
     
      {% for var_landing in var_lan %}
        {% do run_query(var_landing.QURY) %}
        {% do run_query('update  HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_landing  set is_executed=\'True\' where QURY= \''+var_landing.QURY+ '\'' ) %}
    {% endfor %}  

    {% set drop_query %}
    {% set region= env_var('DBT_REGION')   %}
    

          insert into HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.Collate_execution_landing_drop  (qury)
  with tabs as (

		select
			TABLE_CATALOG as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_TYPE as table_type,
			TABLE_SCHEMA as table_owner,
			null as table_comment
		from INFORMATION_SCHEMA.TABLES

    ),

    cols as (

        select
            table_catalog as table_database,
            table_schema,
            table_name,
            column_name,
            ordinal_position as column_index,
            data_type as column_type,
			null as column_comment
        from information_schema.columns

    )
,final as
(
    select 
	'DROP TABLE IF EXISTS '||	tabs.table_database ||'.'|| tabs.table_schema||'."'|| tabs.table_name||  '" ; ' as qury
		
    from tabs
    join cols on tabs.table_database = cols.table_database 
    and tabs.table_schema = cols.table_schema
    and tabs.table_name = cols.table_name
    and tabs.table_name ilike ('%_COLLATE%')
    and tabs.table_name not like '%VW%'
    order by column_index
  )
  
  select  distinct qury from final;

    {% endset %}
  {% do run_query(drop_query) %}
  {% set region= env_var('DBT_REGION')   %}
    {% set var_drop = run_query('select QURY from HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_landing_drop where is_executed=\'false\' order by RN ;') %}
      {% for var_landing1 in var_drop %}
        {% do run_query(var_landing1.QURY) %}
        {% do run_query('update  HRDP_PUB_'+region+'_DB.CMN_PUB_SCH.Collate_execution_landing_drop set is_executed=\'True\'  where QURY= \''+var_landing1.QURY+ '\'' ) %}
    {% endfor %}  
    
 -- landing ends --

    {% endmacro %}