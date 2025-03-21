{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    key_position_type_id as key_position_type_sk,
    key_position_type_code,
    key_position_type_start_date,
    key_position_type_end_date,
    key_position_type_name_en,
    key_position_type_name_fr,
    key_position_type_status
from {{ ref("key_position_type_v1") }}
union all
select -1, null, null, null, null, null, null
