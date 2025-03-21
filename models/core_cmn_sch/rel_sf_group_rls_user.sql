{{ config(materialized="table", cluster_by=["grp_id"], transient=false) }}
with
    group_user as (
        select dgu_grt.grp_id, employee_id, groupname
        from {{ ref("dim_param_sf_roles") }} dpsr
        join {{ ref("dim_sf_roles_group") }} dsrg on dpsr.sfroleid = dsrg.sfroleid
        join
            {{ ref("stg_group_user_flatten") }} dgu_grt
            on dgu_grt.grp_id = dsrg.grtgroup
    ),
    group_user_upn as (
        select grp_id, employee_id, groupname, upper(u.login_name) as employee_upn,
        from group_user gu
        join
            {{ env_var("DBT_SDDS_DB") }}.
            {{ env_var("DBT_C2_H05_EMPLOYMENT_INFORMATION") }}.employee_profile_directory_v1 epd
            on gu.employee_id = epd.user_id
            and epd.ep_employee_status = 't'
        join
            snowflake.account_usage.users u
            on upper(epd.username) = upper(u.login_name)
            and u.deleted_on is null
            and u.disabled = false

    )

select distinct
    grp_id::number as grp_id,
    employee_id,
    groupname,
    employee_upn
from group_user_upn