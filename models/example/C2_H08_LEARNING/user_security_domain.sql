{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','USER_SECURITY_DOMAIN');"
    )
}}

select
    stud_id as user_id,
    dbt_valid_from as user_security_domain_start_date,
    dbt_valid_to as user_security_domain_end_date,
    dmn_id as security_domain_code

from {{ ref("stg_pa_student") }}
where dbt_valid_to is null
