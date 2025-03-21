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
    f.termination_month_sk month_sk,
    f.headcount_type_code,
    'WW' org_level,
    'WW' org_value,
    count(user_id) global_term_cnt,
    count_if(termination_type = 'Employee decision') empl_dec_term_cnt,
    count_if(termination_type = 'Company decision') comp_dec_term_cnt,
    count_if(all_players_status_name_en != 'Player') key_players_term_cnt,
    sum(group_seniority_months) tenure_in_months
from {{ ref("fact_turnover") }} f
join (select 1) on f.headcount_present_flag = 1
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
group by 1, 2, 3, 4
union all
select
    f.termination_month_sk month_sk,
    f.headcount_type_code,
    'Zone' org_level,
    nvl(do.geographic_zone_code, '') org_value,
    count(user_id) global_term_cnt,
    count_if(termination_type = 'Employee decision') empl_dec_term_cnt,
    count_if(termination_type = 'Company decision') comp_dec_term_cnt,
    count_if(all_players_status_name_en != 'Player') key_players_term_cnt,
    sum(group_seniority_months) tenure_in_months
from {{ ref("fact_turnover") }} f
join (select 1) on f.headcount_present_flag = 1
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
group by 1, 2, 3, 4
union all
select
    f.termination_month_sk month_sk,
    f.headcount_type_code,
    'Country' org_level,
    nvl(do.country_code, '') org_value,
    count(user_id) global_term_cnt,
    count_if(termination_type = 'Employee decision') empl_dec_term_cnt,
    count_if(termination_type = 'Company decision') comp_dec_term_cnt,
    count_if(all_players_status_name_en != 'Player') key_players_term_cnt,
    sum(group_seniority_months) tenure_in_months
from {{ ref("fact_turnover") }} f
join (select 1) on f.headcount_present_flag = 1
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
group by 1, 2, 3, 4
order by 1, 2, 3, 4
