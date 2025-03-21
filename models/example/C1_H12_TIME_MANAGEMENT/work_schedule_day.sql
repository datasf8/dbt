{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    workschedule_externalcode as work_schedule_code,
    day as day_number,
    hoursandminutes as planned_hours_and_minutes,
    workinghours as planed_hours,
    mdfsystemrecordstatus as work_schedule_day_status
from {{ ref("stg_workscheduleday_flatten") }}
where dbt_valid_to is null
