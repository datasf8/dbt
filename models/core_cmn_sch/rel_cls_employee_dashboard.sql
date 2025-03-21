{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;call cmn_core_sch.security_replication_sp();",
    )
}}
select
    'All' report_name,
    ep.ddep_person_id_ddep employee_id,
    u.name employee_upn,
    'Ethnicity' field_name,
    'True' visibility
from {{ ref("dim_employee_profile") }} ep
join
    snowflake.account_usage.users u
    on upper(ep.ddep_employee_upn_ddep) = upper(u.name)
    and ep.ddep_country_code_ddep = 'USA'
    and deleted_on is null
    and disabled = false
union all
select
    'All' report_name,
    ep.ddep_person_id_ddep employee_id,
    u.name employee_upn,
    'Race' field_name,
    'True' visibility
from {{ ref("dim_employee_profile") }} ep
join
    snowflake.account_usage.users u
    on upper(ep.ddep_employee_upn_ddep) = upper(u.name)
    and ep.ddep_country_code_ddep = 'USA'
    and deleted_on is null
    and disabled = false
union all
select
    'All' report_name,
    ep.ddep_person_id_ddep employee_id,
    u.name employee_upn,
    'FLSA_STATUS' field_name,
    'True' visibility
from {{ ref("dim_employee_profile") }} ep
join
    snowflake.account_usage.users u
    on upper(ep.ddep_employee_upn_ddep) = upper(u.name)
    and ep.ddep_country_code_ddep = 'USA'
    and deleted_on is null
    and disabled = false
Union all
select
    report_name,
    employee_id,
    employee_upn,
    nvl(field_name, ' ') as field_name,
    'True' visibility
from {{ ref("cls_employee_dashboard") }}
where nvl(field_name, ' ') <> ' ' 
order by 2
