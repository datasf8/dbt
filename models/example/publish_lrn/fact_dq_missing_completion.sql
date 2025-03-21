{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_DQ_MISSING_COMPLETION');"
    )
}}
select
    c.class_sk,
    nvl(i.item_sk, -1) as item_sk,
    nvl(ep.employee_profile_sk, -1) as employee_profile_sk,
    to_char(sr.end_date, 'YYYYMMDD') as schedule_end_date_sk,
    es.user_id,
    es.enrollment_status_code as registration_status,
    la.completion_status_code as completion_status,
    la.completion_date,
    nvl(epa.employee_profile_sk, -1) as admin_employee_profile_sk,
    sr.last_updated_by as admin_user_id,
    nvl(lad.learning_administrator_sk, -1) as learning_administrator_sk,
    -- case when 'Missing Completion Status' as data_quality_issue
    case
        when completion_status_code is null or completion_status_code = 'Missing/Blank'
        then 'Missing Completion Status'
        else 'Valid'
    end as data_quality_issue
from {{ ref("schedule_v1") }} s
join
    {{ ref("enrollment_seat_v1") }} es
    on s.schedule_id = es.schedule_id
    and s.schedule_data_end_date = '9999-12-31'
    and es.enrollment_seat_data_end_date = '9999-12-31'
    and es.enrollment_status_code = 'ENROLL'
    and try_to_number(es.user_id) is not null
left join
    {{ ref("learning_activity_v1") }} la
    on s.schedule_id = la.schedule_id
    and s.item_code = la.item_code
    and es.user_id = la.user_id
left join
    {{ ref("schedule_resources_v1") }} sr
    on s.schedule_id = sr.schedule_id
    and sr.schedule_resources_data_end_date = '9999-12-31'
left join
    (
        select *
        from {{ ref("job_role_org_v1") }}
        qualify
            row_number() over (partition by user_id order by learning_job_role_id) = 1
    ) jro
    on es.user_id = jro.user_id
join {{ ref("dim_class") }} c on s.schedule_id = c.class_id
left join {{ ref("dim_item") }} i on s.item_code = i.item_code
left join {{ ref("dim_employee_profile_v1") }} ep on es.user_id = ep.user_id
left join {{ ref("dim_employee_profile_v1") }} epa on sr.last_updated_by = epa.user_id
left join {{ ref("dim_learning_administrator") }} lad on sr.last_updated_by = lad.user_id
where (sr.end_date < trunc(current_date(), 'month') or sr.end_date is null)  -- nvl(la.completion_status_code, '') = ''
order by 4 desc
