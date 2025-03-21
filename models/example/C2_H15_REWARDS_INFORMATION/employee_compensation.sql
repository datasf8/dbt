{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    userid as user_id,
    startdate as employee_compensation_information_start_date,
    enddate as employee_compensation_information_end_date,
    seqnumber as sequence_number,
    payrollid as payroll_id,
    customstring4 as compensation_plan_eligibility_id,
    customdouble2 as profit_share_cap,
    paygroup as pay_group_code,
    customdate1 as last_salary_increase_date,
    customdouble1 as last_salary_increase_percentage,
    event as events_code,
    eventreason as event_reasons_code
from {{ ref("stg_emp_compensation_flatten") }}
where dbt_valid_to is null
