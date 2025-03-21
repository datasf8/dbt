{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    externalcode as work_schedule_day_model_code,
    externalname_defaultvalue as work_schedule_day_model_name_en,
    externalname_fr_fr as work_schedule_day_model_name_fr,
    workinghours as work_schedule_day_model_working_hours,
    mdfsystemrecordstatus as work_schedule_day_model_status

from {{ ref("stg_workscheduledaymodel_flatten") }}
where dbt_valid_to is null
