{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    enrl_seat_id as enrollment_seat_id,
    dbt_valid_from as enrollment_seat_data_start_date,
    nvl(dbt_valid_to,'9999-12-31') as enrollment_seat_data_end_date,
    stud_id as user_id,
    schd_id as schedule_id,
    enrl_dte as enrollment_date,
    enrl_stat_id as enrollment_status_code
from {{ ref("stg_pa_enroll_seat") }}
