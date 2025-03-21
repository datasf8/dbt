{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(
        soif.onboardingstableid, soif.processhirestatus, soif.dbt_valid_from
    ) as employee_onboarding_infos_id,
    soif.dbt_valid_from::date as employee_onboarding_infos_start_date,
    nvl(soif.dbt_valid_to, '9999-12-31')::date as employee_onboarding_infos_end_date,
    soif.user as user_id,
    soif.onboardingstableid as process_id,
    soif.processstatus as process_status,
    soif.processhirestatus as process_hires_status,
    soif.processtype as process_type,
    soif.processsubtype as process_subtype,
    soif.createddatetime::date as process_creation_date,
    soif.targetdate as process_target_date,
    manager as manager_user_id
from {{ ref("stg_onboarding_info_flatten") }} soif
left join
    {{ ref("stg_onb2_process_flatten") }} sopf
    on onboardingstableid = processid
    and process_hires_status = sopf.onboardinghirestatus
    and soif.dbt_valid_from between sopf.startdate and nvl(sopf.enddate, '9999-12-31')
    and sopf.dbt_valid_to is null
qualify
    row_number() over (
        partition by soif.onboardingstableid, soif.processhirestatus, soif.dbt_valid_from::date
        order by soif.lastmodifieddatetime desc
    )
    = 1
