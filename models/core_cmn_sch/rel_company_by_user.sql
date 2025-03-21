{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;call cmn_core_sch.security_replication_sp();",
    )
}}
with
    rgu as (
        select *
        from {{ ref("rel_pmg_sf_group_rls_employee") }}
        union all
        select *
        from {{ ref("rel_ec_sf_group_rls_employee") }}
        union all
        select *
        from {{ ref("rel_cmp_sf_group_rls_employee") }}
        union all
        select *
        from {{ ref("rel_hire_sf_group_rls_employee") }}
    ),
    reu as (
        select *
        from {{ ref("rel_employee_user") }}
        where reeu_subject_domain_reeu = 'LRN'
        union all
        select distinct
            reeu_subject_domain_reeu,
            gu.employee_id,
            gu.employee_upn,
            reeu_employee_id_ddep
        from rgu
        join {{ ref("rel_sf_group_rls_user") }} gu on rgu.reeu_group_id_reem = gu.grp_id
    )
select distinct
    reeu_subject_domain_reeu as subject_domain,
    reeu_access_employee_id_reem as user_id,
    reeu_access_employee_upn_ddep as user_upn,
    epd.company_code
from reu
join
    {{ env_var("DBT_SDDS_DB") }}.{{ env_var("DBT_C2_H05_EMPLOYMENT_INFORMATION") }}.employee_profile_directory_v1 epd
    on reu.reeu_employee_id_ddep = epd.user_id
    and reu.reeu_employee_id_ddep != 'all'
union
select distinct
    reeu_subject_domain_reeu as subject_domain,
    reeu_access_employee_id_reem as user_id,
    reeu_access_employee_upn_ddep as user_upn,
    epd.company_code
from reu
join
    (
        select distinct company_code
        from
            {{ env_var("DBT_SDDS_DB") }}.
            {{ env_var("DBT_C2_H05_EMPLOYMENT_INFORMATION") }}.employee_profile_directory_v1
    ) epd
    on reu.reeu_employee_id_ddep = 'all'
