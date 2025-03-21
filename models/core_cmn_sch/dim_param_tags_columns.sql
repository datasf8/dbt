{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
         on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
    )
}}
select distinct
    tag_name as dptc_tag_name_dptc,
    tag_value as dptc_tag_value_dptc,
    database_name as dptc_database_dptc,
    ptc.schema_name as dptc_schema_dptc,
    ptc.table_name as dptc_table_dptc,
    masking_column_name as dptc_masking_column_dptc,
    ref_column_name as dptc_ref_column_dptc,
    masking_value as dptc_masking_value_dptc,
    coalesce(
        pub.data_type, core.data_type, stg.data_type, 'Missing'
    ) as dptc_data_type_cols
from {{ source("landing_tables_CMN", "PARAM_TAGS_COLUMNS") }} ptc
left join
    {{ env_var("DBT_PUB_DB") }}.information_schema.columns pub
    on ptc.masking_column_name = pub.column_name
    and ptc.table_name = pub.table_name
    and ptc.schema_name = pub.table_schema
    and ptc.database_name = '{{ env_var("DBT_PUB_DB") }}'
left join
    {{ env_var("DBT_CORE_DB") }}.information_schema.columns core
    on ptc.masking_column_name = core.column_name
    and ptc.table_name = core.table_name
    and ptc.schema_name = core.table_schema
    and ptc.database_name = '{{ env_var("DBT_CORE_DB") }}'
left join
    {{ env_var("DBT_STAGING_DB") }}.information_schema.columns stg
    on ptc.masking_column_name = stg.column_name
    and ptc.table_name = stg.table_name
    and ptc.schema_name = stg.table_schema
    and ptc.database_name = '{{ env_var("DBT_STAGING_DB") }}'
