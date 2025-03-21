{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select
    f.hiring_month_sk month_sk,
    f.headcount_type_code,
    'WW' org_level,
    'WW' org_value,
    count(user_id) hire_cnt,
    count_if(legal_gender_name_label = 'Male') men_hire_cnt,
    count_if(key_position_type_code in ('LKP', 'GKP', 'SKP')) key_position_cnt
from {{ ref("fact_recruitment") }} f
join (select 1) on f.headcount_present_flag = 1
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
left join {{ ref("dim_position_v1") }} dp using (position_sk)
group by 1, 2, 3, 4
union all
select
    f.hiring_month_sk month_sk,
    f.headcount_type_code,
    'Zone' org_level,
    nvl(do.geographic_zone_code, '') org_value,
    count(user_id) hire_cnt,
    count_if(legal_gender_name_label = 'Male') men_hire_cnt,
    count_if(key_position_type_code in ('LKP', 'GKP', 'SKP')) key_position_cnt
from {{ ref("fact_recruitment") }} f
join (select 1) on f.headcount_present_flag = 1
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
left join {{ ref("dim_position_v1") }} dp using (position_sk)
group by 1, 2, 3, 4
union all
select
    f.hiring_month_sk month_sk,
    f.headcount_type_code,
    'Country' org_level,
    nvl(do.country_code, '') org_value,
    count(user_id) hire_cnt,
    count_if(legal_gender_name_label = 'Male') men_hire_cnt,
    count_if(key_position_type_code in ('LKP', 'GKP', 'SKP')) key_position_cnt
from {{ ref("fact_recruitment") }} f
join (select 1) on f.headcount_present_flag = 1
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
left join {{ ref("dim_position_v1") }} dp using (position_sk)
group by 1, 2, 3, 4
order by 1, 2, 3, 4
