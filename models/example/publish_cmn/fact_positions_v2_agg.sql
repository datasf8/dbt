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
    f.date_month_code,
    f.cost_center_id as organization_sk,
    nvl(f.location_id, -1) as location_sk,
    nvl(f.key_position_type_id, -1) as key_position_type_sk,
    nvl(ja.job_architecture_sk, -1) as job_architecture_sk,
    nvl(aj.agg_jobinfo_sk, -1) as agg_jobinfo_sk,
    nvl(aj_emp.agg_jobinfo_sk, -1) as ag_job_info_emp_sk,
    nvl(f.hr_position_id, -1) as hr_position_sk,
    sum(active_positions_number) as active_positions_number,
    sum(active_positions_headcount_number) as active_positions_headcount_number,
    sum(has_right_to_return_number) as has_right_to_return_number,
    sum(is_vacant_number) as is_vacant_number,
    sum(new_positions_number) as new_positions_number,
    sum(is_vacant_total_days) as is_vacant_total_days,
    sum(in_transit_number) as in_transit_number,
    sum(in_transit_total_days) as in_transit_total_days,
    sum(to_be_recruited_number) as to_be_recruited_number,
    sum(direct_hire_number) as direct_hire_number
from {{ ref("fact_positions_agg_v1") }} f
join {{ ref("date_month_referential_v1") }} dm using (date_month_code)
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_id = org.organization_sk
    and org.is_ec_live_flag = true
left join {{ ref("dim_job_architecture_v1") }} ja using (job_role_id)
left join {{ ref("functional_area_v1") }} fa using (functional_area_id)
left join {{ ref("organizational_area_v1") }} oa using (organizational_area_id)
left join {{ ref("brand_v1") }} br using (brand_id)
left join
    {{ ref("employee_group_v1") }} eg
    on f.position_employee_group_id = eg.employee_group_id
left join
    {{ ref("employee_group_v1") }} eg_emp
    on f.employee_group_id = eg_emp.employee_group_id
left join
    {{ ref("dim_agg_jobinfo") }} aj
    on nvl(fa.functional_area_code, 'NULL') = nvl(aj.functional_area_code, 'NULL')
    and nvl(oa.organizational_area_code, 'NULL')
    = nvl(aj.organizational_area_code, 'NULL')
    and nvl(br.brand_code, 'NULL') = nvl(aj.brand_code, 'NULL')
    and nvl(eg.employee_group_code, 'NULL') = nvl(aj.employee_group_code, 'NULL')
    and 'NULL' = nvl(aj.job_level_id, 'NULL')
left join
    {{ ref("dim_agg_jobinfo") }} aj_emp
    on 'NULL' = nvl(aj_emp.functional_area_code, 'NULL')
    and 'NULL' = nvl(aj_emp.organizational_area_code, 'NULL')
    and 'NULL' = nvl(aj_emp.brand_code, 'NULL')
    and nvl(eg_emp.employee_group_code, 'NULL')
    = nvl(aj_emp.employee_group_code, 'NULL')
    and 'NULL' = nvl(aj_emp.job_level_id, 'NULL')
group by all
