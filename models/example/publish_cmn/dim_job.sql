{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    job_info as (
        select *
        from {{ ref("job_information_v1") }}
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    yes_no_flag_referential_v1 as (
        select *
        from {{ ref("yes_no_flag_referential_v1") }}
        where yes_no_flag_referential_start_date <= current_date()
        qualify
            row_number() over (
                partition by yes_no_flag_referential_code
                order by yes_no_flag_referential_start_date desc
            )
            = 1
    ),
    functional_area_v1 as (
        select *
        from {{ ref("functional_area_v1") }}
        where functional_area_start_date <= current_date()
        qualify
            row_number() over (
                partition by functional_area_code
                order by functional_area_start_date desc
            )
            = 1
    ),
    organizational_area_v1 as (
        select *
        from {{ ref("organizational_area_v1") }}
        where organizational_area_start_date <= current_date()
        qualify
            row_number() over (
                partition by organizational_area_code
                order by organizational_area_start_date desc
            )
            = 1
    ),
    job_level_v1 as (
        select *
        from {{ ref("job_level_v1") }}
        where job_level_start_date <= current_date()
        qualify
            row_number() over (
                partition by job_level_code order by job_level_start_date desc
            )
            = 1
    ),
    professional_field_v1 as (
        select *
        from {{ ref("professional_field_v1") }}
        where professional_field_start_date <= current_date()
        qualify
            row_number() over (
                partition by professional_field_code
                order by professional_field_start_date desc
            )
            = 1
    ),
    local_contract_type_v1 as (
        select *
        from {{ ref("local_contract_type_v1") }}
        where local_contract_type_start_date <= current_date()
        qualify
            row_number() over (
                partition by local_contract_type_code
                order by local_contract_type_start_date desc
            )
            = 1
    ),
    employee_status_v1 as (
        select *
        from {{ ref("employee_status_v1") }}
        where employee_status_start_date <= current_date()
        qualify
            row_number() over (
                partition by employee_status_code
                order by employee_status_start_date desc
            )
            = 1
    ),
    employee_group_v1 as (
        select *
        from {{ ref("employee_group_v1") }}
        where employee_group_start_date <= current_date()
        qualify
            row_number() over (
                partition by employee_group_code order by employee_group_start_date desc
            )
            = 1
    ),
    employee_subgroup_v1 as (
        select *
        from {{ ref("employee_subgroup_v1") }}
        where employee_subgroup_start_date <= current_date()
        qualify
            row_number() over (
                partition by employee_subgroup_code
                order by employee_subgroup_start_date desc
            )
            = 1
    ),
    home_host_designation_v1 as (
        select *
        from {{ ref("home_host_designation_v1") }}
        where home_host_designation_start_date <= current_date()
        qualify
            row_number() over (
                partition by home_host_designation_code
                order by home_host_designation_start_date desc
            )
            = 1
    ),
    brand_v1 as (
        select *
        from {{ ref("brand_v1") }}
        where brand_start_date <= current_date()
        qualify
            row_number() over (partition by brand_code order by brand_start_date desc)
            = 1
    )
select
    hash(ji.user_id, ji.job_start_date, ji.sequence_number) as job_sk,
    ji.user_id,
    ji.job_start_date,
    ji.job_end_date,
    ji.sequence_number,
    ji.event_reasons_code,
    ji.position_code,
    ji.cost_center_code,
    ynh.yes_no_flag_referential_name_en as include_in_headcount,
    ji.fte,
    ji.job_entry_date,
    ji.company_entry_date,
    (
        select max(hir.mobility_date) as hiring_date
        from {{ ref("job_mobility_v1") }} hir
        where
            hir.mobility_type = 'HIRING'
            and ji.user_id = hir.user_id
            and ji.job_start_date >= hir.mobility_date
    ) as hiring_date,
    ji.functional_area_code,
    fa.functional_area_name_en
    || ' ('
    || ji.functional_area_code
    || ')' as functional_area_name_en,
    ji.organizational_area_code,
    oa.organizational_area_name_en
    || ' ('
    || ji.organizational_area_code
    || ')' as organizational_area_name_en,
    ji.local_pay_grade_code,
    lpg.local_pay_grade_name_en,
    lpg.global_grade_code,
    gg.global_grade_name,
    jl.job_level_code,
    jl.job_level_name_en || ' (' || jl.job_level_code || ')' as job_level_name_en,
    lct.local_contract_type_code,
    lct.local_contract_type_name_en,
    es.employee_status_code,
    es.employee_status_name_en
    || ' ('
    || es.employee_status_code
    || ')' as employee_status_name_en,
    ji.employee_group_code,
    eg.employee_group_name_en
    || ' ('
    || ji.employee_group_code
    || ')' as employee_group_name_en,
    ji.employee_subgroup_code,
    esg.employee_subgroup_name_en
    || ' ('
    || ji.employee_subgroup_code
    || ')' as employee_subgroup_name_en,
    hhd.home_host_designation_code,
    hhd.home_host_designation_name_en,
    ji.standard_weekly_hours,
    ji.job_code as job_role_code,
    jr.job_role_name_en || ' (' || jr.job_role_code || ')' as job_role_name_en,
    jr.specialization_code,
    sp.specialization_name_en
    || ' ('
    || sp.specialization_code
    || ')' as specialization_name_en,
    sp.professional_field_code,
    pf.professional_field_name_en
    || ' ('
    || pf.professional_field_code
    || ')' as professional_field_name_en,
    ji.brand_code,
    br.brand_name_en
from job_info ji
-- All Player Status
left join
    yes_no_flag_referential_v1 ynh
    on ji.included_headcount_yn_flag_id = ynh.yes_no_flag_referential_id
left join functional_area_v1 fa on ji.functional_area_code = fa.functional_area_code
left join
    organizational_area_v1 oa
    on ji.organizational_area_code = oa.organizational_area_code
left join
    {{ ref("local_pay_grade_v1") }} lpg
    on ji.local_pay_grade_code = lpg.local_pay_grade_code
    and ji.job_start_date
    between local_pay_grade_start_date and local_pay_grade_end_date
left join
    {{ ref("global_grade_v1") }} gg
    on lpg.global_grade_code = gg.global_grade_code
    and ji.job_start_date between global_grade_start_date and global_grade_end_date
left join job_level_v1 jl on gg.job_level_id = jl.job_level_id
left join
    local_contract_type_v1 lct on ji.local_contract_type_id = lct.local_contract_type_id
left join employee_status_v1 es on ji.employee_status_id = es.employee_status_id
left join employee_group_v1 eg on ji.employee_group_code = eg.employee_group_code
left join
    employee_subgroup_v1 esg on ji.employee_subgroup_code = esg.employee_subgroup_code
left join
    home_host_designation_v1 hhd
    on ji.home_host_designation_id = hhd.home_host_designation_id
left join
    {{ ref("job_role_v1") }} jr
    on ji.job_code = jr.job_role_code
    and job_start_date between job_role_start_date and job_role_end_date
left join
    {{ ref("specialization_v1") }} sp
    on jr.specialization_code = sp.specialization_code
    and job_start_date between specialization_start_date and specialization_end_date
left join
    professional_field_v1 pf on sp.professional_field_code = pf.professional_field_code
left join brand_v1 br on ji.brand_code = br.brand_code
union all
select
    -1,
    '',
    '1900-01-01',
    '9999-12-31',
    0,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
