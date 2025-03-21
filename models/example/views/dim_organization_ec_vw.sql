{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_organization_v1")) }}, 'EC' as subject_domain
from {{ ref("dim_organization_v1") }} o
where
    (
        exists (
            select 1
            from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_company_by_user i
            left join
                {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_param_security_replication s
                on i.user_upn = s.copy_from_user
            where
                user_upn = nvl(copy_to_user, current_user)
                and o.company_code = i.company_code
                and subject_domain = 'EC'
        )
        or current_role() in (
            select dpsr_snowflake_role_dpsr
            from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_param_snowflake_roles
            where dpsr_exclude_security_dpsr = 'y'
        )
    )
    and is_ec_live_flag = true
