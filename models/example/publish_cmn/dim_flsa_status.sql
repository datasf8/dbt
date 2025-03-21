{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns"
    )
}}

select
    flsa_status_id as flsa_status_sk,
    flsa_status_code,
    flsa_status_start_date,
    flsa_status_end_date,
    flsa_status_name_en,
    flsa_status_name_fr,
    flsa_status_status
from {{ ref("flsa_status_v1") }}
union all
select -1, null, null, null, null, null, null
