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
    daymodel as work_schedule_day_model_code,
    category as work_schedule_day_model_assignment_category,
    mdfsystemrecordstatus as work_schedule_day_model_assignment_status

from {{ ref("stg_workscheduledaymodelassignment_flatten") }}
where dbt_valid_to is null
