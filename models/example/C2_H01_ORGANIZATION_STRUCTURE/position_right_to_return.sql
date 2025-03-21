{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    userid as user_id,
    startdate as position_right_to_return_start_date,
    enddate as position_right_to_return_end_date,
    position as position_code,
    effectivestatus as position_right_to_return_status
from {{ ref("stg_position_right_to_return_flatten") }}
where dbt_valid_to is null