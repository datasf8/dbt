{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    personidexternal as personal_id,
    userid as user_id,
    startdate as global_assignment_start_date,
    enddate as global_assignment_end_date,
    plannedenddate as global_assignment_planned_end_date,
    assignmenttype as global_assignment_type_id,
    customstring101 as assignment_package_id
from {{ ref("stg_empglobalassignment_flatten") }}
where dbt_valid_to is null
