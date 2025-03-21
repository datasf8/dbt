{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select stud_id as user_id, job_role_id as learning_job_role_id, org_id
from {{ ref("stg_pa_job_role_org") }}
where dbt_valid_to is null
qualify row_number() over (partition by stud_id, job_role_id order by org_id) = 1
