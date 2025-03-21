{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_DQ_INACCURATE_HISTORY');"
    )
}}
select
    nvl(ep.employee_profile_sk, -1) as employee_profile_sk,
    nvl(i.item_sk, -1) as item_sk,
    nvl(c.class_sk, -1) as class_sk,
    to_char(la.completion_date, 'YYYYMMDD') as completion_date_sk,
    la.user_id,
    la.total_hours item_duration,
    la.completion_status_code as completion_status,
    la.completion_date,
    nvl(epa.employee_profile_sk, -1) admin_employee_profile_sk,
    la.last_updated_by as admin_user_id,
    nvl(lad.learning_administrator_sk, -1) as learning_administrator_sk,
    case
        when la.total_hours > 48
        then 'DURATION TO CHECK'
        when total_hours = 0 or total_hours is null
        then 'DURATION TO CORRECT'
        else 'Valid'
    end as data_quality_issue
from {{ ref("learning_activity_v1") }} la
join
    {{ ref("completion_status_v1") }} cs
    on la.completion_status_code = cs.completion_status_code
    and cs.completion_status_end_date = '9999-12-31'
    and try_to_number(la.user_id) is not null
    -- and (la.total_hours is null or la.total_hours = 0 or la.total_hours > 40)
    and nvl(cs.item_type_code, '') != 'SYSTEM_PROGRAM_ENTITY'
    and cs.provides_credit = 'Y'
    and (la.completion_date < trunc(current_date(), 'month') or la.completion_date is null) 
left join {{ ref("dim_employee_profile_v1") }} ep on la.user_id = ep.user_id
left join {{ ref("dim_class") }} c on la.schedule_id = c.class_id
left join {{ ref("dim_item") }} i on la.item_code = i.item_code
left join {{ ref("dim_employee_profile_v1") }} epa on la.last_updated_by = epa.user_id
left join
    {{ ref("dim_learning_administrator") }} lad on la.last_updated_by = lad.user_id
order by 4 desc
