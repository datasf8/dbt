{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    direct_manager as (
        select distinct user_id, manager_user_id
        from {{ ref("employee_profile_directory_v1") }}
    ),
    all_manager as (
        select
            user_id as employee_id,
            manager_user_id as direct_manager,
            1 as level,
            manager_user_id as manager_id,
            manager_user_id as all_manager
        from direct_manager
        union all
        select
            mgr.user_id as employee_id,
            mgr.manager_user_id as direct_manager,
            am.level + 1 level,
            am.manager_id as manager_id,
            mgr.manager_user_id || ',' || am.all_manager all_manager
        from all_manager am
        inner join
            direct_manager mgr
            on am.employee_id = mgr.manager_user_id
            and mgr.user_id <> mgr.manager_user_id  -- where mgr.USER_id = '10138354'
    ),
    emp_details as (
        select distinct user_id, firstname, lastname
        from {{ ref("employee_profile_directory_v1") }}
    )
select
    am.employee_id as user_id,
    collate(de.firstname || ' ' || de.lastname, 'en-ci') as user_full_name,
    am.direct_manager,
    collate(dm.firstname || ' ' || dm.lastname, 'en-ci') as direct_manager_full_name,
    am.manager_id,
    collate(hm.firstname || ' ' || hm.lastname, 'en-ci') as manager_full_name,
    level,
    epv.employee_profile_sk as employee_profile_sk
from all_manager am
join emp_details de on de.user_id = am.employee_id
left join emp_details dm on dm.user_id = am.direct_manager
left join emp_details hm on hm.user_id = am.manager_id
left join {{ ref("dim_employee_profile_v1") }} epv on epv.user_id = am.employee_id
