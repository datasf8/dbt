{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','USER_ASSIGNMENT_PROFILE');"
    )
}}

select 
STUD_ID	AS	user_id,
DBT_VALID_FROM	AS	user_assignment_profile_start_date,
DBT_VALID_TO	AS	user_assignment_profile_end_date,
AP_ID	AS	assignment_profile_id
from {{ ref('stg_pa_stud_assgn_prfl') }}
WHERE DBT_VALID_TO IS NULL