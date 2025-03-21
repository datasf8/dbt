{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        cluster_by=["ddep_employee_id_ddep"],
    )
}}
with
    employee_profile as (
        select distinct
            ddep_employee_id_ddep,  -- userId
            ddep_person_id_ddep,  -- empId
            case
                when split_part(ddep_upn_ddep, '@', -1) ilike '%loreal.com'
                then ddep_upn_ddep
                when ddep_upn_ddep_calc is not null
                then ddep_upn_ddep_calc
                else ddep_upn_ddep
            end as ddep_employee_upn_ddep,
            ddep_upn_ddep as ddep_employee_src_upn_ddep,
            ddep_first_name_ddep,
            ddep_last_name_ddep,
            ddep_email_ddep,
            ddep_hire_date_ddep,
            ddep_gender_ddep,
            ddep_status_ddep,
            ddep_ec_status_ddep,
            ddep_location_code_ddep,
            ddep_location_ddep,
            ddep_birth_date_ddep,
            ddep_group_seniority_ddep,
            ddep_nationality_ddep,
            ddep_country_code_ddep,
            ddep_country_ddep,
            ddep_job_role_code_ddep,
            ddep_job_role_ddep,
            pf1.code1 as ddep_specialization_code_ddep,
            pf1.label1 as ddep_specialization_ddep,
            pf.code as ddep_professional_field_code_ddep,
            pf.label as ddep_professional_field_ddep,
            ddep_job_seniority_ddep,
            ddep_company_code_ddep,
            ddep_company_ddep,
            ddep_division_code_ddep,
            ddep_division_ddep,
            ddep_bu_type_code_ddep,
            ddep_bu_type_ddep,
            ddep_bu_code_ddep,
            ddep_bu_ddep,
            ddep_position_ddep,
            iff(
                ddep_employee_id_ddep in (
                    select distinct reem_fk_manager_key_ddep
                    from {{ ref("rel_employee_managers") }}
                    where reem_fk_manager_key_ddep is not null
                ),
                'Y',
                'N'
            ) as ddep_ismanager_ddep,
            b.all_player_status as ddep_all_player_status_dlrn,
            ddep_employee_group_ddep,
            iff(
                upper(ddep_bu_type_ddep) = upper('Country - PLANT'),
                'Plants',
                'Without Plants'
            ) as plant,
            iff(
                upper(ddep_bu_type_ddep) = upper('Country - Central'),
                'DCs',
                'Without DC'
            ) as dc,
            iff(
                upper(ddep_specialization_ddep) = upper('Beauty advisory '),
                'BAs',
                'Without BAs'
            ) as ba,
            iff(
                upper(ddep_specialization_ddep) = upper('Internship '),
                'Interns',
                'Without Interns'
            ) as intern,
            iff(
                upper(ddep_specialization_ddep) like upper('Apprenticeship '),
                'Apprentices',
                'Without Apprentices'
            ) as apprentice,
            iff(
                upper(ddep_status_ddep) = 'F', 'Departures', 'Active Employees'
            ) as departed
        from {{ ref("stg_employee_profile_flatten") }} a
        left join
            "{{ env_var('DBT_PUB_DB') }}"."LRN_PUB_SCH"."DIM_LEARNER_VW" b
            on a.ddep_employee_id_ddep = b.learner_id
        left outer join
            (
                select distinct
                    ddep_professional_field_code_ddep code,
                    min(ddep_professional_field_ddep) label
                from {{ ref("stg_employee_profile_flatten") }}
                group by ddep_professional_field_code_ddep
            ) pf
            on code = a.ddep_professional_field_code_ddep
        left outer join
            (
                select distinct
                    ddep_specialization_code_ddep code1,
                    min(ddep_specialization_ddep) label1
                from {{ ref("stg_employee_profile_flatten") }}
                group by ddep_specialization_code_ddep
            ) pf1
            on code1 = a.ddep_specialization_code_ddep
        left outer join
            (
                select ddep_person_id_ddep person_id, ddep_upn_ddep ddep_upn_ddep_calc
                from {{ ref("stg_employee_profile_flatten") }}
                where split_part(ddep_upn_ddep, '@', -1) ilike '%loreal.com'
                qualify
                    row_number() over (
                        partition by ddep_person_id_ddep
                        order by ddep_status_ddep desc, ddep_upn_ddep
                    )
                    = 1
            ) upn
            on a.ddep_person_id_ddep = upn.person_id

    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("DDEP_EMPLOYEE_ID_DDEP") }} as sk_userid
        from employee_profile p
    )

select
    hash(ddep_employee_id_ddep) as ddep_employee_profile_sk_ddep,
    sk_userid as ddep_employee_profile_key_ddep,
    ddep_employee_id_ddep,
    ddep_person_id_ddep,
    ddep_employee_upn_ddep,
    ddep_employee_src_upn_ddep,
    ddep_first_name_ddep,
    ddep_last_name_ddep,
    ddep_email_ddep,
    ddep_hire_date_ddep,
    ddep_gender_ddep,
    ddep_status_ddep,
    ddep_ec_status_ddep,
    ddep_location_code_ddep,
    ddep_location_ddep,
    ddep_birth_date_ddep,
    ddep_group_seniority_ddep,
    ddep_nationality_ddep,
    ddep_country_code_ddep,
    ddep_country_ddep,
    ddep_job_role_code_ddep,
    ddep_job_role_ddep,
    ddep_specialization_code_ddep,
    ddep_specialization_ddep,
    ddep_professional_field_code_ddep,
    ddep_professional_field_ddep,
    ddep_job_seniority_ddep,
    ddep_company_code_ddep,
    ddep_company_ddep,
    ddep_division_code_ddep,
    ddep_division_ddep,
    ddep_bu_type_code_ddep,
    ddep_bu_type_ddep,
    ddep_bu_code_ddep,
    ddep_bu_ddep,
    ddep_position_ddep,
    ddep_ismanager_ddep,
    ddep_all_player_status_dlrn,
    ddep_employee_group_ddep,
    plant,
    dc,
    ba,
    intern,
    apprentice,
    departed
from surrogate_key
