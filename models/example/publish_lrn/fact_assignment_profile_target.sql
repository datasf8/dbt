{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_ASSIGNMENT_PROFILE_TARGET');"
    )
}}

with
    target as (
        select *
        from {{ ref("user_assignment_profile_v1") }} uap
        where
            exists (
                select 1
                from {{ ref("assignment_profile_referential_v1") }} apr
                where uap.assignment_profile_id = apr.assignment_profile_id
            )
            and user_id not like ('%api%')
            and user_id not like ('%manager%')
            and user_id not like ('%employee%')
            and user_id not like ('%admin%')
            and user_id not like ('%tech%')
            and user_id not like ('%core%')
            and user_id not like ('%karla%')
            and user_id not like ('%ext%')
    ),
    without_target as (
        select assignment_profile_id || '_TARGET' as ap_id_without, *
        from {{ ref("user_assignment_profile_v1") }} uap
        where
            exists (
                select 1
                from {{ ref("assignment_profile_referential_v1") }} apr
                where
                    uap.assignment_profile_id
                    = replace(apr.assignment_profile_id, '_TARGET', '')
            )
            and user_id not like ('%api%')
            and user_id not like ('%manager%')
            and user_id not like ('%employee%')
            and user_id not like ('%admin%')
            and user_id not like ('%tech%')
            and user_id not like ('%core%')
            and user_id not like ('%karla%')
            and user_id not like ('%ext%')
    ),
    assign_prfl_ref as (select * from {{ ref("dim_assignment_profile_referential") }}),
    jn_target as (
        select
            t.user_id,
            t.assignment_profile_id,
            case
                when wt.user_id is null then 'Completed' else 'Not Completed'
            end as completion_status,
            case when wt.user_id is null then 1 else 0 end as completion_flag
        from target t
        left outer join
            without_target wt
            on t.user_id = wt.user_id
            and t.assignment_profile_id = wt.ap_id_without
    ),
    assign_profile as (select * from {{ ref("assignment_profile_v1") }}),  -- select * from assign_profile;
    empl_prfl as (
        select
            iff(
                upper(business_unit_type_name_en) = upper('Country - PLANT'),
                'Plants only',
                'Without Plants'
            ) as plant,
            iff(
                upper(business_unit_type_name_en) = upper('Country - Central'),
                'DCs only',
                'Without DC'
            ) as dc,
            iff(
                upper(specialization_name_en) like upper('Beauty advisory%'),
                'BAs only',
                'Without BA'
            ) as ba,
            iff(
                upper(specialization_name_en) like upper('Internship%'),
                'Interns only',
                'Without Interns'
            ) as intern,
            iff(
                upper(specialization_name_en) like upper('Apprenticeship%'),
                'Apprenticeships only',
                'Without Apprenticeship'
            ) as apprentice,
            *
        from {{ ref("dim_employee_profile_v1_vw") }}
        where nvl(ec_ep_employee_status,'') <> 'f' and nvl(employee_group_code,'') <> 'EG0000'
    ),
    manag_email as (
        select user_id, emp_email_address, mgr_id, mgr_email_address
        from
            (
                select
                    e.user_id,
                    e.email_address as emp_email_address,
                    m.user_id as mgr_id,
                    m.email_address as mgr_email_address
                from (select * from {{ ref("dim_employee_profile_v1_vw") }}) e
                inner join
                    (select * from {{ ref("dim_employee_profile_v1_vw") }}) m
                    on (e.manager_user_id = m.user_id)
            )
    ),
    dim_org as (select * from {{ ref("dim_organization_bycompany_vw") }}),
    job_info as (
        select user_id, employee_status_code, employee_status_name_en
        from
            (
                select *
                from
                    (
                        select *
                        from {{ ref("job_information_v1") }}

                        qualify
                            row_number() over (
                                partition by user_id
                                order by job_start_date desc, sequence_number desc
                            )
                            = 1
                    )
            ) ji
        left outer join
            {{ ref("employee_status_v1") }} es

            on ji.employee_status_id = es.employee_status_id
    ),
    item_map as (
        select *
        from {{ ref("assignment_profiles_item_mapping_v1") }}
        qualify
            row_number() over (
                partition by item_code, assignment_profile_id
                order by assignment_profiles_item_mapping_data_start_date desc
            )
            = 1
    ),
    learn_act as (
        select user_id, item_code, max(completion_date) as completion_date
        from {{ ref("learning_activity_v1") }}
        where completion_date is not null
        group by all
    )
select
    ep.personal_id,
    jt.user_id,
    assignment_profile_id_sk,
    organization_bycompany_sk,
    employee_profile_sk,
    assignment_profile_type,
    la.completion_date,
    jt.completion_status,
    case when completion_status = 'Completed' then 'Yes' else 'No' end as completed,
    jt.completion_flag,
    ec_ep_employee_status as ec_ep_status,
    employee_status_name_en as ec_status,
    me.mgr_email_address as manager_email,
    plant,
    dc,
    ba,
    intern,
    apprentice
from jn_target jt
inner join assign_profile ap on jt.assignment_profile_id = ap.assignment_profile_id
inner join assign_prfl_ref apr on ap.assignment_profile_id = apr.assignment_profile_id
inner join empl_prfl ep on jt.user_id = ep.user_id
left outer join manag_email me on jt.user_id = me.user_id
left outer join dim_org dor on ep.company_code = dor.company_code
left outer join job_info ji on jt.user_id = ji.user_id
left outer join
    item_map im
    on replace(jt.assignment_profile_id, '_TARGET', '') = im.assignment_profile_id
left outer join learn_act la on im.item_code = la.item_code and jt.user_id = la.user_id
order by 1 desc
