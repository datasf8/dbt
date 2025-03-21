{{
    config(
        materialized="table",
        transient=false,
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call masking_policy_apply_sp('{{ env_var('DBT_PUB_DB') }}','CMP_PUB_SCH','FACT_VARIABLE_PAY_BY_GOALS_SNAPSHOT');         USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call rls_policy_apply_sp('{{ env_var('DBT_PUB_DB') }}','CMP_PUB_SCH','FACT_VARIABLE_PAY_BY_GOALS_SNAPSHOT');",
    )
}}
with
    vp_goals as (
        select
            ftvp_user_id_ftvp as user_id,
            ftvp_person_id_external_ftvp as employee_id,
            ftvp_fk_empl_ftvp as employee_key,
            date_employee_key,
            ftvp_fk_variable_pay_ftvp as variable_pay_key,
            ftvp_fk_job_architecture_key_ftvp as job_architecture_key,
            ftvp_fk_organization_ftvp as organization_key,
            ftvp_fk_all_players_key_fcyt as all_players_key,
            ftvp_job_seniority_range_code_ftvp as job_seniority_range_code,
            ftvp_age_range_code_ftvp as age_range_code,
            ftvp_group_seniority_range_code_ftvp as group_seniority_range_code,
            ftvp_payout_range_code_ftvp as range_payout_code,
            fcyt_pk_gender_fcyt as gender_key,
            ftvp_pk_local_pay_grade_lpgr as local_pay_grade_key,
            ftvp_pk_global_pay_grade_gpgr as global_pay_grade_key,
            ftvp_pk_employee_group_emgp as employee_group_key,
            ftvp_pk_employee_subgroup_emsg as employee_subgroup_key,
            ftvp_pk_home_host_hoho as home_host_key,
            ftvp_pk_operational_area_opar as operational_area_key,
            ftvp_pk_scope_type_scot as scope_type_key,
            ftvp_pk_contract_type_coty as contract_type_key,
            'People Goals' goal_type,
            ftvp_people_rating_guideline_dvpg as rating_guideline_key,
            ftvp_pg_people_target_ftvp as target,
            ftvp_pg_rating_ftvp as rating,
            ftvp_pg_payout_percent_ftvp as payout_percent,
            ftvp_pg_individual_result_payout_amount_ftvp as payout_amount,
            case
                when
                    ftvp_pg_payout_percent_ftvp
                    between dvpg_min_guidelines_dvpg and dvpg_max_guidelines_dvpg
                then 'Aligned'
                else 'Not Aligned'
            end as flag_aligned,
            ftvp_empl_status_ftvp as empl_status,
            ftvp_hire_date_ftvp as hire_date,
            ftvp_pk_job_level_jlvl as job_level_key,
            ftvp_pk_ethnicity_ecty as ethnicity_key,
            ftvp_sk_hr_id_empm as sk_hr_id,
            ftvp_pk_kpos as key_position_level_key,
            guideline_code as guidelines_aligned_key,
            ftvp_percent_of_prorated_bonusable_salary
            as percent_of_prorated_bonusable_salary,
            ftvp_fk_organization_v1_sk_ftvp as organization_v1_sk,
            ftvp_compensation_planner_empl as compensation_planner,
            ftvp_comp_plan_owner_ftvp as comp_plan_owner,
            ftvp_comp_plan_sk_dcpv as comp_plan_sk
        from {{ ref("fact_variable_pay_snapshot") }}
        left outer join
            {{ ref("dim_param_variable_pay_guidelines_snapshot") }}
            on dvpg_pk_guidelines_dvpg = ftvp_people_rating_guideline_dvpg
        left outer join
            {{ ref("dim_param_guidelines_key") }}
            on flag_aligned = people_guidelines_aligned

        union

        select
            ftvp_user_id_ftvp as user_id,
            ftvp_person_id_external_ftvp as employee_id,
            ftvp_fk_empl_ftvp as employee_key,
            date_employee_key,
            ftvp_fk_variable_pay_ftvp as variable_pay_key,
            ftvp_fk_job_architecture_key_ftvp as job_architecture_key,
            ftvp_fk_organization_ftvp as organization_key,
            ftvp_fk_all_players_key_fcyt as all_players_key,
            ftvp_job_seniority_range_code_ftvp as job_seniority_range_code,
            ftvp_age_range_code_ftvp as age_range_code,
            ftvp_group_seniority_range_code_ftvp as group_seniority_range_code,
            ftvp_payout_range_code_ftvp as range_payout_code,
            fcyt_pk_gender_fcyt as gender_key,
            ftvp_pk_local_pay_grade_lpgr as local_pay_grade_key,
            ftvp_pk_global_pay_grade_gpgr as global_pay_grade_key,
            ftvp_pk_employee_group_emgp as employee_group_key,
            ftvp_pk_employee_subgroup_emsg as employee_subgroup_key,
            ftvp_pk_home_host_hoho as home_host_key,
            ftvp_pk_operational_area_opar as operational_area_key,
            ftvp_pk_scope_type_scot as scope_type_key,
            ftvp_pk_contract_type_coty as contract_type_key,
            'Business Goals' goal_type,
            ftvp_business_rating_guideline_dvpg as rating_guideline_key,
            ftvp_bg_business_target_ftvp as target,
            ftvp_bg_rating_ftvp as rating,
            ftvp_bg_payout_percent_ftvp as payout_percent,
            ftvp_bg_team_result_payout_amount_ftvp as payout_amount,
            case
                when
                    ftvp_bg_payout_percent_ftvp
                    between dvpg_min_guidelines_dvpg and dvpg_max_guidelines_dvpg
                then 'Aligned'
                else 'Not Aligned'
            end as flag_aligned,
            ftvp_empl_status_ftvp as empl_status,
            ftvp_hire_date_ftvp as hire_date,
            ftvp_pk_job_level_jlvl as job_level_key,
            ftvp_pk_ethnicity_ecty as ethnicity_key,
            ftvp_sk_hr_id_empm as sk_hr_id,
            ftvp_pk_kpos as key_position_level_key,
            guideline_code as guidelines_aligned_key,
            ftvp_percent_of_prorated_bonusable_salary
            as percent_of_prorated_bonusable_salary,
            ftvp_fk_organization_v1_sk_ftvp as organization_v1_sk,
            ftvp_compensation_planner_empl as compensation_planner,
            ftvp_comp_plan_owner_ftvp as comp_plan_owner,
            ftvp_comp_plan_sk_dcpv as comp_plan_sk
        from {{ ref("fact_variable_pay_snapshot") }}
        left outer join
            {{ ref("dim_param_variable_pay_guidelines_snapshot") }}
            on dvpg_pk_guidelines_dvpg = ftvp_business_rating_guideline_dvpg
        left outer join
            {{ ref("dim_param_guidelines_key") }}
            on flag_aligned = team_guidelines_aligned
    )
select
    user_id,
    employee_id,
    employee_key,
    date_employee_key,
    variable_pay_key,
    job_architecture_key,
    organization_key,
    all_players_key,
    job_seniority_range_code,
    age_range_code,
    group_seniority_range_code,
    range_payout_code,
    gender_key,
    local_pay_grade_key,
    global_pay_grade_key,
    employee_group_key,
    employee_subgroup_key,
    home_host_key,
    operational_area_key,
    scope_type_key,
    contract_type_key,
    goal_type,
    rating_guideline_key,
    target,
    rating,
    payout_percent,
    payout_amount,
    flag_aligned,
    empl_status,
    hire_date,
    job_level_key,
    ethnicity_key,
    sk_hr_id,
    key_position_level_key,
    guidelines_aligned_key,
    percent_of_prorated_bonusable_salary,
    organization_v1_sk,
    compensation_planner,
    comp_plan_owner,
    comp_plan_sk
from vp_goals
