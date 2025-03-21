{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change='sync_all_columns',
    )
}}
select
    schd_id as schedule_id,
    dbt_valid_from as schedule_resources_data_start_date,
    nvl(dbt_valid_to, '9999-12-31') as schedule_resources_data_end_date,
    start_dte as start_date,
    end_dte as end_date,
    class_hrs as class_hours ,
    lst_upd_usr as last_updated_by
from {{ ref("stg_ps_schd_resources") }}