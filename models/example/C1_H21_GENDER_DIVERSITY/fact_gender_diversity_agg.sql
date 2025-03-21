{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    gender_diversity as (
        select *
        from {{ ref("employee_indicators_referential") }}
        where
            employee_indicators_code
            in ('HDC_HDC', 'TRN_GT', 'HIR_GLH', 'GEN_TCP', 'GEN_EPR')
    )
select
    date_id,
    date_yyyymm,
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
    employee_group_id,
    employee_subgroup_id,
    local_pay_grade_id,
    flsa_status_id,
    manager_user_id,
    hr_manager_position_id,
    sum(value) fact_gender_diversity_agg_value
from {{ ref("employees_by_indicator") }} ebi
join gender_diversity using (employee_indicators_referential_id)
join {{ ref("employee_files") }} ef using (employee_files_id)
join {{ ref("dates_referential") }} dr on ebi.dates_referential_id = dr.date_id
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
group by all
