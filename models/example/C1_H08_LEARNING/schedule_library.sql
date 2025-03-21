{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert"
    )
}}
select catalog_id as library_code, schd_id as schedule_id
from {{ ref("stg_pa_catalog_item_sched") }}
where dbt_valid_to is null
