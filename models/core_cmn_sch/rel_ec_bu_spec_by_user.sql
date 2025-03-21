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
select distinct
    reeu_access_employee_id_reem as user_id,
    reeu_access_employee_upn_ddep as user_upn,
    epd.business_unit_code,
    nvl(epd.specialization_code, '') as specialization_code
from {{ ref("rel_employee_user") }} reu
join
    (select 1) on reu.reeu_employee_id_ddep != 'all' and reeu_subject_domain_reeu = 'EC'
join
    {{ env_var("DBT_SDDS_DB") }}.{{ env_var("DBT_C2_H05_EMPLOYMENT_INFORMATION") }}.employee_profile_directory_v1 epd
    on reu.reeu_employee_id_ddep = epd.user_id
union all
select
    reeu_access_employee_id_reem as user_id,
    reeu_access_employee_upn_ddep as user_upn,
    'All' as business_unit_code,
    'All' as specialization_code
from {{ ref("rel_employee_user") }}
where reeu_employee_id_ddep = 'all' and reeu_subject_domain_reeu = 'EC'
