{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    f.date_month_code as month_sk,
    f.headcount_type_code,
    f.cost_center_id as organization_sk,
    nvl(f.location_id, -1) as location_sk,
    nvl(f.key_position_type_id, -1) as key_position_type_sk,
    nvl(ja.job_architecture_sk, -1) as job_architecture_sk,
    nvl(aj.agg_jobinfo_sk, -1) as agg_jobinfo_sk,
    nvl(f.employee_subgroup_id, -1) as employee_subgroup_sk,
    nvl(f.local_pay_grade_id, -1) as pay_grade_sk,
    nvl(f.local_contract_type_id, -1) as local_contract_type_sk,
    nvl(ap.agg_personalinfo_sk, -1) as agg_personalinfo_sk,
    nvl(et.ethnicity_sk, -1) as ethnicity_sk,
    nvl(f.job_seniority_range_id, -1) as job_seniority_range_sk,
    nvl(f.group_seniority_range_id, -1) as group_seniority_range_sk,
    nvl(f.age_seniority_range_id, -1) as age_seniority_range_sk,
    sum(headcount_number) user_cnt,
    sum(total_group_seniority_in_months) as total_group_seniority_in_months,
    sum(total_job_seniority_in_months) as total_job_seniority_in_months,
    sum(total_age_seniority_in_years) as total_age_seniority_in_years
from {{ ref("fact_headcount_agg") }} f
join {{ ref("date_month_referential") }} dm using (date_month_code)
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_id = org.organization_sk
    and org.is_ec_live_flag = true
left join {{ ref("dim_job_architecture_v1") }} ja using (job_role_id)
left join {{ ref("functional_area") }} fa using (functional_area_id)
left join {{ ref("organizational_area") }} oa using (organizational_area_id)
left join {{ ref("brand") }} br using (brand_id)
left join {{ ref("employee_group") }} eg using (employee_group_id)
left join
    {{ ref("dim_agg_jobinfo") }} aj
    on nvl(fa.functional_area_code, 'NULL') = nvl(aj.functional_area_code, 'NULL')
    and nvl(oa.organizational_area_code, 'NULL')
    = nvl(aj.organizational_area_code, 'NULL')
    and nvl(br.brand_code, 'NULL') = nvl(aj.brand_code, 'NULL')
    and nvl(eg.employee_group_code, 'NULL') = nvl(aj.employee_group_code, 'NULL')
    and nvl(f.job_level_id, 'NULL') = nvl(aj.job_level_id, 'NULL')
left join {{ ref("legal_gender") }} lg using (legal_gender_id)
left join
    {{ ref("all_players_status") }} aps
    on f.all_player_status_id = aps.all_players_status_id
left join
    {{ ref("dim_agg_personalinfo") }} ap
    on nvl(lg.legal_gender_code, 'NULL') = nvl(ap.legal_gender_code, 'NULL')
    and nvl(aps.all_players_status_code, 'NULL')
    = nvl(ap.all_players_status_code, 'NULL')
left join {{ ref("dim_ethnicity_v1") }} et using (ethnicity_id, race_id)
group by all
