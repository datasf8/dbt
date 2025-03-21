{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(onb2process_processid, taskid, startdate) as onboarding_process_tasks_id,
    startdate as task_start_date,
    enddate as task_end_date,
    taskid as task_id,
    onb2process_processid as process_id,
    status as task_status,
    taskduedate as task_due_date,
    type as task_type,
    completedby as task_completed_by_user_id
from {{ ref("stg_onb2_process_task_flatten") }}
where dbt_valid_to is null
