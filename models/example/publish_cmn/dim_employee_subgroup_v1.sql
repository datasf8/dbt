{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    employee_subgroup_id as employee_subgroup_sk,
    employee_subgroup_code,
    employee_subgroup_start_date,
    employee_subgroup_end_date,
    employee_subgroup_name_en,
    employee_subgroup_name_en
    || ' ('
    || employee_subgroup_code
    || ')' as employee_subgroup_label,
    employee_subgroup_status,
    esg.country_code,
    country_name_en,
    country_name_en || ' (' || esg.country_code || ')' as country_label,
    country_status,
    is_ec_live_flag
from {{ ref("employee_subgroup") }} esg
left join {{ ref("country") }} c using (country_id)
union all
select -1, null, null, null, null, null, null, null, null, null, null, null
