{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','EMPLOYEE_PROFILE_DIRECTORY');"
    )
}}
with
    user_flat as (
        select
            userid,
            empid,
            firstname,
            lastname,
            email,
            dateofbirth,
            username,
            gender,
            trim(replace(split_part(custom04, '(', -1), ')', '')) as company_code,
            trim(replace(split_part(custom03, '(', -1), ')', '')) as hr_division_code,
            trim(replace(split_part(division, '(', -1), ')', '')) as area_code,
            trim(
                replace(split_part(department, '(', -1), ')', '')
            ) as business_unit_code,
            trim(replace(split_part(jobcode, '(', -1), ')', '')) as job_role_code,
            trim(
                replace(split_part(custom06, '(', -1), ')', '')
            ) as professional_field_code,
            trim(
                replace(split_part(custom07, '(', -1), ')', '')
            ) as specialization_code,
            substr(
                custom12,
                position('(', custom12) + 1,
                position(')', custom12) - position('(', custom12) - 1
            ) as functional_area_code,
            substr(
                custom13,
                position('(', custom13) + 1,
                position(')', custom13) - position('(', custom13) - 1
            ) as organizational_area_code,
            hiredate,
            servicedate,
            jobseniority,
            customdate2,
            upper(country) as country,
            upper(custom08) as key_posn_type,
            manager_userid,
            custom01 as ec_ep_employee_status,
            status as ep_employee_status,
            trim(replace(split_part(custom10, '(', -1), ')', '')) as employee_group_code
        from {{ ref("stg_user_flatten") }}
        where dbt_valid_to is null
    ),
    legal_gender as (
        select *
        from {{ ref("legal_gender") }}
        where legal_gender_start_date <= current_date()
        qualify
            row_number() over (
                partition by legal_gender_code order by legal_gender_start_date desc
            )
            = 1
    ),
    company as (
        select *
        from {{ ref("company") }}
        where company_start_date <= current_date()
        qualify
            row_number() over (
                partition by company_code order by company_start_date desc
            )
            = 1
    ),
    hr_division as (
        select *
        from {{ ref("hr_division") }}
        where hr_division_start_date <= current_date()
        qualify
            row_number() over (
                partition by hr_division_code order by hr_division_start_date desc
            )
            = 1
    ),
    area as (
        select *
        from {{ ref("area") }}
        where area_start_date <= current_date()
        qualify
            row_number() over (partition by area_code order by area_start_date desc) = 1
    ),
    business_unit as (
        select *
        from {{ ref("business_unit") }}
        where business_unit_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_code order by business_unit_start_date desc
            )
            = 1
    ),
    job_role as (
        select *
        from {{ ref("job_role") }}
        where job_role_start_date <= current_date()
        qualify
            row_number() over (
                partition by job_role_code order by job_role_start_date desc
            )
            = 1
    ),
    professional_field as (
        select *
        from {{ ref("professional_field") }}
        where professional_field_start_date <= current_date()
        qualify
            row_number() over (
                partition by professional_field_code
                order by professional_field_start_date desc
            )
            = 1
    ),
    specialization as (
        select *
        from {{ ref("specialization") }}
        where specialization_start_date <= current_date()
        qualify
            row_number() over (
                partition by specialization_code order by specialization_start_date desc
            )
            = 1
    ),
    functional_area as (
        select *
        from {{ ref("functional_area") }}
        where functional_area_start_date <= current_date()
        qualify
            row_number() over (
                partition by functional_area_code
                order by functional_area_start_date desc
            )
            = 1
    ),
    organizational_area as (
        select *
        from {{ ref("organizational_area") }}
        where organizational_area_start_date <= current_date()
        qualify
            row_number() over (
                partition by organizational_area_code
                order by organizational_area_start_date desc
            )
            = 1
    ),
    country as (
        select *
        from {{ ref("country") }}
        where country_start_date <= current_date()
        qualify
            row_number() over (
                partition by country_code order by country_start_date desc
            )
            = 1
    ),
    key_position_type as (
        select *
        from {{ ref("key_position_type") }}
        where key_position_type_start_date <= current_date()
        qualify
            row_number() over (
                partition by key_position_type_name_en
                order by key_position_type_start_date desc
            )
            = 1
    ),
    pa_student as (
        select stud_id, col_num, user_value
        from {{ ref("stg_pa_stud_user") }}
        where dbt_valid_to is null and col_num in ('200', '210', '100')  -- '240'
        qualify
            row_number() over (partition by stud_id, col_num order by user_value) = 1
    ),
    all_players_status as (
        select *
        from {{ ref("all_players_status") }}
        where all_players_status_start_date <= current_date()
        qualify
            row_number() over (
                partition by all_players_status_name_en
                order by all_players_status_start_date desc
            )
            = 1
    ),
    business_unit_type as (
        select *
        from {{ ref("business_unit_type") }}
        where
            business_unit_type_start_date <= current_date()
            and business_unit_type_status = 'A'
        qualify
            row_number() over (
                partition by business_unit_type_name_en
                order by business_unit_type_start_date desc
            )
            = 1
    ),
    cost_center as (
        select *
        from {{ ref("cost_center") }}
        where cost_center_start_date <= current_date()
        qualify
            row_number() over (
                partition by cost_center_code order by cost_center_start_date desc
            )
            = 1
    ),
    photo as (
        select
            case
                when phototype = 1 and photo is not null then 'Yes' else 'No'
            end as photo_flag,
            phototype,
            photo,
            userid
        from {{ ref("stg_photo_flatten") }}
        where dbt_valid_to is null and phototype = 1
    ),
    upn as (
        select userid, empid, username
        from user_flat
        where split_part(username, '@', -1) ilike '%loreal.com'
        qualify
            row_number() over (
                partition by empid order by ep_employee_status desc, username
            )
            = 1
    ),
    employee_group as (
        select *
        from {{ ref("employee_group") }}
        where employee_group_start_date <= current_date()
        qualify
            row_number() over (
                partition by employee_group_code order by employee_group_start_date desc
            )
            = 1
    )
select
    hash(uf.empid, uf.userid) as employee_profile_directory_id,
    uf.empid as personal_id,
    uf.userid as user_id,
    firstname as firstname,
    lastname as lastname,
    email as email_address,
    dateofbirth as birth_date,
    case
        when split_part(uf.username, '@', -1) ilike '%loreal.com'
        then uf.username
        when upn.username is not null
        then upn.username
        else uf.username
    end as username,
    lg.legal_gender_id,
    gender as legal_gender_code,
    cmpny.company_id,
    uf.company_code,
    hrd.hr_division_id,
    uf.hr_division_code,
    ar.area_id,
    uf.area_code,
    bu.business_unit_id,
    uf.business_unit_code,
    jr.job_role_id,
    uf.job_role_code,
    pf.professional_field_id,
    uf.professional_field_code,
    sp.specialization_id,
    uf.specialization_code,
    fa.functional_area_id,
    uf.functional_area_code,
    oa.organizational_area_id,
    uf.organizational_area_code,
    hiredate as hiring_date,
    servicedate as position_entry_date,
    jobseniority as job_entry_date,
    customdate2 as group_seniority,
    cntry.country_id,
    cntry.country_code,
    kpt.key_position_type_id as key_position_id,
    kpt.key_position_type_code as key_position_level,
    aps.all_players_status_id as all_player_status_id,
    aps.all_players_status_code as all_player_status_code,
    but.business_unit_type_id,
    but.business_unit_type_code,
    cc.cost_center_id,
    u_cc.user_value as cost_center_code,
    manager_userid as manager_user_id,
    uf.ec_ep_employee_status,
    uf.ep_employee_status,
    nvl(photo_flag, 'No') as has_photo_flag,
    eg.employee_group_id,
    uf.employee_group_code
from user_flat uf
left join legal_gender lg on uf.gender = lg.legal_gender_code
left join company cmpny on uf.company_code = cmpny.company_code
left join hr_division hrd on uf.hr_division_code = hrd.hr_division_code
left join area ar on uf.area_code = ar.area_code
left join business_unit bu on uf.business_unit_code = bu.business_unit_code
left join job_role jr on uf.job_role_code = jr.job_role_code
left join
    professional_field pf on uf.professional_field_code = pf.professional_field_code
left join specialization sp on uf.specialization_code = sp.specialization_code
left join functional_area fa on uf.functional_area_code = fa.functional_area_code
left join
    organizational_area oa on uf.organizational_area_code = oa.organizational_area_code
left join country cntry on uf.country = upper(cntry.country_name_en)
left join
    key_position_type kpt on uf.key_posn_type = upper(kpt.key_position_type_name_en)
left join pa_student u_ap on uf.userid = u_ap.stud_id and u_ap.col_num = '200'
left join all_players_status aps on u_ap.user_value = aps.all_players_status_name_en
left join pa_student u_but on uf.userid = u_but.stud_id and u_but.col_num = '210'
left join business_unit_type but on u_but.user_value = but.business_unit_type_name_en
left join pa_student u_cc on uf.userid = u_cc.stud_id and u_cc.col_num = '100'
left join cost_center cc on u_cc.user_value = cc.cost_center_code
left outer join photo on uf.userid = photo.userid
left outer join upn on uf.empid = upn.empid
left join employee_group eg on uf.employee_group_code = eg.employee_group_code
