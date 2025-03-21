{{ config(schema="cmn_pub_sch") }}
select
    employee_profile_sk,
    user_id,
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
    business_unit_name_en
    || ' '
    || '('
    || business_unit_code
    || ')' as business_unit_name_en,
    business_unit_name_en as business_unit_name_label,
    job_role_code,
    job_role_name_en || ' ' || '(' || job_role_code || ')' as job_role_name_en,
    professional_field_code,
    professional_field_name_en
    || ' '
    || '('
    || professional_field_code
    || ')' as professional_field_name_en,
    specialization_code,
    specialization_name_en
    || ' '
    || '('
    || specialization_code
    || ')' as specialization_name_en,
    hiring_date,
    position_entry_date,
    job_entry_date,
    group_seniority,
    country_code,
    country_name_en,
    key_position_level,
    all_players_status_code,
    all_players_status_name_en,
    business_unit_type_code,
    business_unit_type_name_en,
    cost_center_code,
    cost_center_name_en
    || ' '
    || '('
    || split_part(cost_center_code, '_', 1)
    || ')' as cost_center_name_en,
    cost_center_name_en as cost_center_name_label,
    manager_user_id,
    ec_ep_employee_status,
    geographic_zone_code,
    geographic_zone_name_en,
    ep_employee_status,
    personal_id,
    security_domain_code,
    functional_area_code,
    functional_area_name_en,
    organizational_area_code,
    organizational_area_name_en,
    employee_group_code
from {{ ref("dim_employee_profile_v1") }}
