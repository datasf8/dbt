{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        cluster_by=["ddep_employee_id_ddep"],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH; call rls_policy_apply_sp('{{ database }}','{{ schema }}','DIM_EMPLOYEE_PROFILE_FINAL');",
    )
}}
with
    employee_profile_final as (
        select distinct
            ddep_employee_profile_sk_ddep,
            ddep_employee_profile_key_ddep,
            ddep_employee_id_ddep,
            ddep_employee_upn_ddep,
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
            b.zones as zones,
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
            collate(
                case when plant = 'Plants' then 'With Plants' else 'Without Plants' end,
                'en-ci'
            ) as plant,
            collate(
                case when dc = 'DCs' then 'With DC' else 'Without DC' end, 'en-ci'
            ) as dc,
            collate(
                case when ba = 'BAs' then 'BAs Only' else 'Without BAs' end, 'en-ci'
            ) as ba,
            collate(
                case
                    when intern = 'Interns' then 'Only Interns' else 'Without Interns'
                end,
                'en-ci'
            ) as intern,
            collate(
                case
                    when apprentice = 'Apprentices'
                    then 'Apprentices Only'
                    else 'Without Apprentices'
                end,
                'en-ci'
            ) as apprentice,
            collate(
                case
                    when departed = 'Active Employees'
                    then 'Active Only'
                    else 'Terminated Only'
                end,
                'en-ci'
            ) as departed,
            collate(
                case
                    when ddep_ismanager_ddep = 'Y'
                    then 'Managers Only'
                    else 'Without Managers'
                end,
                'en-ci'
            ) as ddep_ismanager_ddep,
            job_range.range_code as ddep_job_seniority_range_ddep,
            age_range.range_code as ddep_age_range_ddep,
            group_range.range_code as ddep_group_seniority_range_ddep,
            case
                when
                    ddep_all_player_status_dlrn
                    in ('Essential Player', 'Rising Player', 'Future Leader')
                then ddep_all_player_status_dlrn
                else 'N/A'
            end as all_player_status,
            ddep_employee_group_ddep,
            concat(ddep_bu_ddep, '( ', ddep_bu_code_ddep, ' )') as business_unit
        from {{ ref("dim_employee_profile") }} a
        join {{ ref("country_zones") }} b on a.ddep_country_ddep = b.country
        left outer join
            {{ ref("dim_range") }} job_range
            on datediff('month', ddep_job_seniority_ddep, current_date())
            between job_range.range_inf and job_range.range_sup
            and job_range.range_type = 'Job'
        left outer join
            {{ ref("dim_range") }} age_range
            on datediff('month', ddep_birth_date_ddep, current_date())
            between age_range.range_inf and age_range.range_sup
            and age_range.range_type = 'Age'
        left outer join
            {{ ref("dim_range") }} group_range
            on datediff('month', ddep_group_seniority_ddep, current_date())
            between group_range.range_inf and group_range.range_sup
            and group_range.range_type = 'Group'
        where
            ifnull(ddep_division_ddep, '') != '' and ifnull(ddep_bu_code_ddep, '') != ''
    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("DDEP_EMPLOYEE_ID_DDEP") }} as sk_userid
        from employee_profile_final p
    )

select
    ddep_employee_profile_sk_ddep,
    ddep_employee_profile_key_ddep,
    ddep_employee_id_ddep,
    ddep_employee_upn_ddep,
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
    zones,
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
    plant,
    dc,
    ba,
    intern,
    apprentice,
    departed,
    ddep_ismanager_ddep,
    ddep_job_seniority_range_ddep,
    ddep_age_range_ddep,
    ddep_group_seniority_range_ddep,
    all_player_status,
    ddep_employee_group_ddep,
    business_unit
from surrogate_key
