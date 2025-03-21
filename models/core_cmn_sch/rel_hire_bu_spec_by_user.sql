{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        cluster_by=["user_upn", "business_unit_code", "specialization_code"],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;call cmn_core_sch.security_replication_sp();",
    )
}}
with
    rheg as (
        (
            select employee_id, employee_upn, reeu_employee_id_ddep
            from {{ ref("rel_sf_group_rls_user") }} a
            join
                {{ ref("rel_hire_sf_group_rls_employee") }} b
                on a.grp_id = b.reeu_group_id_reem
        )
    ),
    bu_spec as (
        select distinct
            employee_id as user_id,
            employee_upn as user_upn,
            epd.business_unit_code,
            nvl(epd.specialization_code, '') as specialization_code
        from rheg
        join (select 1) on rheg.reeu_employee_id_ddep != 'all'
        join
            {{ env_var("DBT_SDDS_DB") }}.
            {{ env_var("DBT_C2_H05_EMPLOYMENT_INFORMATION") }}.employee_profile_directory_v1 epd
            on rheg.reeu_employee_id_ddep = epd.user_id
    )
select *
from bu_spec
union all
select
    employee_id as user_id,
    employee_upn as user_upn,
    'All' as business_unit_code,
    'All' as specialization_code
from rheg
where rheg.reeu_employee_id_ddep = 'all'
