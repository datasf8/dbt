{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','EMPLOYMENT_DETAILS');"
    )
}}
with
    emp_qualify as (
        select *
        from {{ ref("stg_emp_employement_flatten") }}
        qualify
            row_number() over (
                partition by userid, startdate
                order by lastmodifieddatetime desc, dbt_updated_at desc
            )
            = 1
    ),
    emp as (
        select
            *,
            iff(enddate is null, '9999-12-31', enddate) calc_enddate,
            lead(startdate, 1) over (
                partition by userid order by startdate
            ) next_startdate
        from emp_qualify
    )
select
    personidexternal as personal_id,
    userid as user_id,
    startdate as employment_details_start_date,
    iff(
        calc_enddate >= next_startdate, next_startdate - 1, calc_enddate
    ) as employment_details_end_date,
    customdate1 as time_pay_seniority,
    customdate2 as group_seniority,
    customdate3 as int_transfer_ga_date,
    senioritydate as company_seniority,
    benefitseligibilitystartdate as benefits_start_date
from emp
