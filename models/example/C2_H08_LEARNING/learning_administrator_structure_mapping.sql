{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    admin_id as user_id,
    role_id as learning_job_role_id,
    approval_role_id as approval_role_id,
    sec_dom as secondary_domain_id,
    status as status,
    learning_administrator_id as learning_administrator_code,
    administrator_category_id as learning_administrator_category_code
from {{ ref("stg_lms_admin_structure_mapping") }}
where dbt_valid_to is null
