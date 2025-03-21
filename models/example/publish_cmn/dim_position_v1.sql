{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        cluster_by=["position_sk"],
    )
}}
with
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
    location_v1 as (
        select *
        from {{ ref("location_v1") }}
        where location_start_date <= current_date()
        qualify
            row_number() over (
                partition by location_code order by location_start_date desc
            )
            = 1
    ),
    country_v1 as (
        select *
        from {{ ref("country_v1") }}
        where country_start_date <= current_date()
        qualify
            row_number() over (
                partition by country_code order by country_start_date desc
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
    ),
    local_pay_grade_v1 as (
        select *
        from {{ ref("local_pay_grade_v1") }}
        where local_pay_grade_start_date <= current_date()
        qualify
            row_number() over (
                partition by local_pay_grade_code
                order by local_pay_grade_start_date desc
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
    key_position_type_v1 as (
        select *
        from {{ ref("key_position_type_v1") }}
        where key_position_type_start_date <= current_date()
        qualify
            row_number() over (
                partition by key_position_type_code
                order by key_position_type_start_date desc
            )
            = 1
    )
select
    position_id as position_sk,
    position_code,
    position_start_date,
    position_end_date,
    position_name_en || ' (' || position_code || ')' as position_name_en,
    position_name_fr,
    pos.job_role_code,
    jr.job_role_name_en,
    jr.specialization_code,
    sp.specialization_name_en
    || ' ('
    || jr.specialization_code
    || ')' as specialization_name_en,
    sp.professional_field_code,
    pf.professional_field_name_en
    || ' ('
    || sp.professional_field_code
    || ')' as professional_field_name_en,
    pos.local_job_code,
    pos.functional_area_code,
    fa.functional_area_name_en
    || ' ('
    || pos.functional_area_code
    || ')' as functional_area_name_en,
    pos.organizational_area_code,
    oa.organizational_area_name_en
    || ' ('
    || pos.organizational_area_code
    || ')' as organizational_area_name_en,
    pos.cost_center_code,
    pos.location_code,
    loc.location_name || ' (' || pos.location_code || ')' as location_name,
    loc.country_code as location_country_code,
    con.country_name_en as location_country_name_en,
    pos.brand_code,
    br.brand_name_en,
    pos.local_pay_grade_code,
    lpg.local_pay_grade_name_en,
    pos.pay_range_code,
    pos.employee_group_code,
    eg.employee_group_name_en,
    pos.key_position_type_code,
    kpt.key_position_type_name_en,
    position_prime_flag,
    position_to_be_recruited_flag,
    position_recruiter_user_id,
    position_change_reason_code,
    position_weekly_hours,
    position_fte,
    higher_position_code,
    position_status,
    collate(
        iff(specialization_name_en like 'Beauty advisory', 'BAs only', 'Without BA'),
        'en-ci'
    ) as ba,
    collate(
        iff(
            specialization_name_en like 'Internship', 'Interns only', 'Without Interns'
        ),
        'en-ci'
    ) as intern,
    collate(
        iff(
            specialization_name_en like 'Apprenticeship',
            'Apprenticeships only',
            'Without Apprenticeship'
        ),
        'en-ci'
    ) as apprentice
from {{ ref("position_v1") }} pos
left join
    {{ ref("job_role_v1") }} jr
    on pos.job_role_code = jr.job_role_code
    and position_start_date between job_role_start_date and job_role_end_date
left join
    {{ ref("specialization_v1") }} sp
    on jr.specialization_code = sp.specialization_code
    and position_start_date
    between specialization_start_date and specialization_end_date
left join
    professional_field_v1 pf on sp.professional_field_code = pf.professional_field_code
left join functional_area_v1 fa on pos.functional_area_code = fa.functional_area_code
left join
    organizational_area_v1 oa
    on pos.organizational_area_code = oa.organizational_area_code
left join location_v1 loc on pos.location_code = loc.location_code
left join country_v1 con on loc.country_code = con.country_code
left join brand_v1 br on pos.brand_code = br.brand_code
left join local_pay_grade_v1 lpg on pos.local_pay_grade_code = lpg.local_pay_grade_code
left join employee_group_v1 eg on pos.employee_group_code = eg.employee_group_code
left join
    key_position_type_v1 kpt on pos.key_position_type_code = kpt.key_position_type_code
union all
select
    -1,
    '',
    '1900-01-01',
    '9999-12-31',
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
    'Without BA',
    'Without Interns',
    'Without Apprenticeship'
