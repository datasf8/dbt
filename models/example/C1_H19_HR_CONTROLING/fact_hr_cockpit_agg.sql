{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    date_id,
    date_yyyy_mm_dd,
    employee_indicators_referential_id,
    hr_division_id,
    company_id,
    business_unit_type_id,
    business_unit_id,
    cost_center_id,
    location_id,
    key_position_type_id,
    job_role_id,
    specialization_id,
    professional_field_id,
    functional_area_id,
    organizational_area_id,
    brand_id,
    country_id,
    geographic_zone_id,
    gsr.range_id as group_seniority_range_id,
    jsr.range_id as job_seniority_range_id,
    asr.range_id as age_seniority_range_id,
    all_players_status_id,
    ethnicity_id,
    race_id,
    legal_gender_id,
    employee_group_id,
    employee_subgroup_id,
    local_pay_grade_id,
    job_level_id,
    flsa_status_id,
    sum(value) fact_hr_cockpit_agg_value
from {{ ref("employee_files") }} ef
join {{ ref("employees_by_indicator") }} ebd using (employee_files_id)
join {{ ref("dates_referential") }} dr on ebd.dates_referential_id = dr.date_id
left join
    {{ ref("ranges_v1") }} gsr
    on gsr.range_type = 'GRPSEN'
    and months_between(used_date, group_seniority)
    between gsr.range_start and gsr.range_end - 0.001
left join
    {{ ref("ranges_v1") }} jsr
    on jsr.range_type = 'JOBSEN'
    and months_between(used_date, job_entry_date)
    between jsr.range_start and jsr.range_end - 0.001
left join
    {{ ref("ranges_v1") }} asr
    on asr.range_type = 'AGESEN'
    and months_between(used_date, date_of_birth) / 12
    between asr.range_start and asr.range_end - 0.001
-- where dates_referential_id > 20241000 and employee_indicators_code = 'HDC_LKP'
group by all
