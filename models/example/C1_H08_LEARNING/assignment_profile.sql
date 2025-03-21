{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(ap_id, dbt_valid_from) as assignment_profile_id,
    ap_id as assignment_profile_code,
    dbt_valid_from as assignment_profile_start_date,
    nvl(dbt_valid_to, '9999-12-31') as assignment_profile_end_date,
    ap_desc as assignment_profile_description,
    dmn_id as security_domain_code,
    sd.security_domain_id,
    active as assignment_profile_active_flag

from {{ ref("stg_pa_assgn_prfl") }} ap
left join
    {{ ref("security_domain") }} sd
    on ap.dmn_id = sd.security_domain_code
    and sd.security_domain_start_date <=current_date()
where dbt_valid_to is null 
qualify
    row_number() over (
        partition by ap.ap_id, ap.dbt_valid_from
        order by sd.security_domain_start_date desc
    )
    = 1
