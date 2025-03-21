{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','EMPLOYMENT_TERMINATION_DETAILS');"
    )
}}
select
    personidexternal as personal_id,
    userid as user_id,
    enddate as employment_termination_details_end_date,
    benefitsenddate as benefits_end_date,
    customstring20 as resignation_reason_code,
    customdate6 as date_of_death,
    payrollenddate as payroll_end_date,
    lastdateworked as last_date_worked,
    oktorehire as ok_to_rehire_flag,
    customstring16 as competition_clause_yn_flag_id,
    customstring17 as competition_clause_status_id,
    customstring18 as severance_agreement_yn_flag_id,
    customstring19 as confidential_termination_yn_flag_id
from {{ ref("stg_emp_employement_termination_flatten") }}
where dbt_valid_to is null
