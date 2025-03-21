select
    ftvp_user_id_ftvp as user_id,
    ftvp_person_id_external_ftvp as employee_id,
    date_employee_key as date_employee_key,
    ftvp_fk_empl_ftvp as employee_key,
    ftvp_fk_variable_pay_ftvp as variable_pay_key,
    ftvp_fk_job_architecture_key_ftvp as job_architecture_key,
    ftvp_fk_organization_ftvp as organization_key,
    ftvp_hire_date_ftvp as hire_date,
    ftvp_business_rating_guideline_dvpg as business_rating_guideline_key,
    ftvp_people_rating_guideline_dvpg as people_rating_guideline_key,
    ftvp_bonusable_salary_ftvp as bonusable_salary,
    ftvp_target_amount_ftvp as target_amount,
    ftvp_bu_payout_percent_ftvp as bu_payout_percent,
    ftvp_bu_payout_ftvp as bu_payout,
    ftvp_bg_business_target_ftvp as bg_business_target,
    ftvp_bg_rating_ftvp as bg_rating,
    ftvp_bg_business_rating_ftvp as bg_business_rating,
    ftvp_bg_team_result_payout_amount_ftvp as bg_team_result_payout_amount,
    ftvp_pg_people_target_ftvp as pg_people_target,
    ftvp_pg_rating_ftvp as pg_rating,
    ftvp_pg_people_rating_ftvp as pg_people_rating,
    ftvp_pg_individual_result_payout_amount_ftvp as pg_individual_result_payout_amount,
    ftvp_total_final_bonus_payout_ftvp as total_final_bonus_payout,
    ftvp_total_payout_as_percent_of_target_ftvp as total_payout_as_percent_of_target,
    ftvp_total_payout_as_percent_of_bonusable_salary_ftvp
    as total_payout_as_percent_of_bonusable_salary,
    ftvp_bg_payout_percent_ftvp as bg_payout_percent,
    ftvp_pg_payout_percent_ftvp as pg_payout_percent,
    ftvp_fcyt_one_time_bonus_fcyt as one_time_bonus,
    ftvp_fk_all_players_key_fcyt as all_players_key,
    ftvp_job_seniority_range_code_ftvp as job_seniority_range_code,
    ftvp_age_range_code_ftvp as age_range_code,
    ftvp_payout_range_code_ftvp as range_payout_code,
    ftvp_group_seniority_range_code_ftvp as group_seniority_range_code,
    fcyt_pk_gender_fcyt as gender_key,
    ftvp_pk_local_pay_grade_lpgr as local_pay_grade_key,
    ftvp_pk_global_pay_grade_gpgr as global_pay_grade_key,
    ftvp_pk_employee_group_emgp as employee_group_key,
    ftvp_pk_employee_subgroup_emsg as employee_subgroup_key,
    ftvp_pk_home_host_hoho as home_host_key,
    ftvp_pk_operational_area_opar as operational_area_key,
    ftvp_pk_scope_type_scot as scope_type_key,
    ftvp_pk_contract_type_coty as contract_type_key,
    ftvp_empl_status_ftvp as empl_status,
    ftvp_team_rating_guidlines_aligned_ftvp as team_rating_guidlines_aligned,
    ftvp_people_rating_guidlines_aligned_ftvp as people_rating_guidlines_aligned,
    ftvp_pk_job_level_jlvl as job_level_key,
    ftvp_pk_ethnicity_ecty as ethnicity_key,
    ftvp_sk_hr_id_empm as sk_hr_id,
    case
        when ftvp_team_rating_guidlines_aligned_ftvp = 'Aligned'
        then 'Yes'
        when ftvp_team_rating_guidlines_aligned_ftvp = 'Not Aligned'
        then 'No'
    end as team_guidelines_aligned,
    case
        when ftvp_people_rating_guidlines_aligned_ftvp = 'Aligned'
        then 'Yes'
        when ftvp_people_rating_guidlines_aligned_ftvp = 'Not Aligned'
        then 'No'
    end as people_guidelines_aligned,
    ftvp_pk_kpos as key_position_level_key,
    ftvp_people_guidelines_aligned_key as people_guidelines_aligned_key,
    ftvp_business_guidelines_aligned_key as business_guidelines_aligned_key,
    ftvp_percent_of_prorated_bonusable_salary as percent_of_prorated_bonusable_salary,
    ftvp_fk_organization_v1_sk_ftvp as organization_v1_sk,
    ftvp_compensation_planner_empl as compensation_planner,
    ftvp_comp_plan_owner_ftvp as comp_plan_owner,
    ftvp_comp_plan_sk_dcpv as comp_plan_sk
from {{ ref("fact_variable_pay_snapshot") }}
