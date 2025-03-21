{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_organization_v1")) }}, subject_domain
from {{ ref("dim_organization_v1") }}
join {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_company_by_user using (company_code)
where
    user_upn = current_user()
    and current_role() not in (
        select dpsr_snowflake_role_dpsr
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_param_snowflake_roles
        where dpsr_exclude_security_dpsr = 'y'
    )
union all
select {{ dbt_utils.star(ref("dim_organization_v1")) }}, subject_domain
from {{ ref("dim_organization_v1") }}
join
    (
        select distinct subject_domain
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_company_by_user
    )
where
    current_role() in (
        select dpsr_snowflake_role_dpsr
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_param_snowflake_roles
        where dpsr_exclude_security_dpsr = 'y'
    )
