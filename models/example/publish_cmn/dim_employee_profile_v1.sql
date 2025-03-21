{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','DIM_EMPLOYEE_PROFILE_V1');"
    )
}}
with
    company_v1 as (
        select *
        from {{ ref("company_v1") }}
        where company_start_date <= current_date()
        qualify
            row_number() over (
                partition by company_code order by company_start_date desc
            )
            = 1
    ),
    hr_division_v1 as (
        select *
        from {{ ref("hr_division_v1") }}
        where hr_division_start_date <= current_date()
        qualify
            row_number() over (
                partition by hr_division_code order by hr_division_start_date desc
            )
            = 1
    ),
    area_v1 as (
        select *
        from {{ ref("area_v1") }}
        where area_start_date <= current_date()
        qualify
            row_number() over (partition by area_code order by area_start_date desc) = 1
    ),
    business_unit_v1 as (
        select *
        from {{ ref("business_unit_v1") }}
        where business_unit_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_code order by business_unit_start_date desc
            )
            = 1
    ),
    legal_gender_v1 as (
        select *
        from {{ ref("legal_gender_v1") }}
        where legal_gender_start_date <= current_date()
        qualify
            row_number() over (
                partition by legal_gender_code order by legal_gender_start_date desc
            )
            = 1
    ),
    job_role_v1 as (
        select *
        from {{ ref("job_role_v1") }}
        where job_role_start_date <= current_date()
        qualify
            row_number() over (
                partition by job_role_code order by job_role_start_date desc
            )
            = 1
    ),
    specialization_v1 as (
        select *
        from {{ ref("specialization_v1") }}
        where specialization_start_date <= current_date()
        qualify
            row_number() over (
                partition by specialization_code order by specialization_start_date desc
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
    all_players_status_v1 as (
        select *
        from {{ ref("all_players_status_v1") }}
        where all_players_status_start_date <= current_date()
        qualify
            row_number() over (
                partition by all_players_status_code
                order by all_players_status_start_date desc
            )
            = 1
    ),
    business_unit_type_v1 as (
        select *
        from {{ ref("business_unit_type_v1") }}
        where business_unit_type_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_type_code
                order by business_unit_type_start_date desc
            )
            = 1
    ),
    cost_center_v1 as (
        select *
        from {{ ref("cost_center_v1") }}
        where cost_center_start_date <= current_date()
        qualify
            row_number() over (
                partition by cost_center_code order by cost_center_start_date desc
            )
            = 1
    ),
    geographic_zone_v1 as (
        select *
        from {{ ref("geographic_zone_v1") }}
        where geographic_zone_start_date <= current_date()
        qualify
            row_number() over (
                partition by geographic_zone_code
                order by geographic_zone_start_date desc
            )
            = 1
    ),
    security_domain_code_v1 as (
        select *
        from {{ ref("user_security_domain_v1") }}
        where user_security_domain_start_date <= current_date()
        qualify
            row_number() over (
                partition by user_id, security_domain_code
                order by user_security_domain_start_date desc
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
    )
select
    hash(epd.user_id) as employee_profile_sk,
    epd.user_id,
    personal_id,
    firstname,
    lastname,
    email_address,
    birth_date,
    username,
    legal_gender_code,
    legal_gender_name_en,
    company_code,
    company_name_en,
    hr_division_code,
    hr_division_name_en,
    area_code,
    area_name_en,
    business_unit_code,
    business_unit_name_en,
    job_role_code,
    job_role_name_en,
    professional_field_code,
    professional_field_name_en,
    specialization_code,
    specialization_name_en,
    hiring_date,
    position_entry_date,
    job_entry_date,
    group_seniority,
    country_code,
    country_name_en,
    key_position_level,
    all_player_status_code as all_players_status_code,
    all_players_status_name_en,
    business_unit_type_code,
    business_unit_type_name_en,
    cost_center_code,
    cost_center_name_en,
    manager_user_id,
    ec_ep_employee_status,
    ep_employee_status,
    geographic_zone_code,
    geographic_zone_name_en,
    security_domain_code,
    functional_area_code,
    functional_area_name_en,
    organizational_area_code,
    organizational_area_name_en,
    employee_group_code
from {{ ref("employee_profile_directory_v1") }} epd
left join legal_gender_v1 using (legal_gender_code)
left join company_v1 using (company_code)
left join hr_division_v1 using (hr_division_code)
left join area_v1 using (area_code)
left join business_unit_v1 using (business_unit_code)
left join professional_field_v1 using (professional_field_code)
left join specialization_v1 using (specialization_code)
left join job_role_v1 using (job_role_code)
left join country_v1 using (country_code)
left join all_players_status_v1 on all_player_status_code = all_players_status_code
left join business_unit_type_v1 using (business_unit_type_code)
left join cost_center_v1 using (cost_center_code)
left join geographic_zone_v1 using (geographic_zone_code)
left join security_domain_code_v1 sdc on epd.user_id = sdc.user_id
left join functional_area_v1 fa using (functional_area_code)
left join organizational_area_v1 using (organizational_area_code)
union all
select
    -1,
    '-1',
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
