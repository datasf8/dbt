{{
    config(
        materialized="incremental",
        unique_key="FORM_TEMPLATE_ID",
        transient=true,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    cpsn_report_int as (
        select
            user_id,
            employee_id,
            gender,
            business_unit,
            annualized_salary,
            status,
            ec_employee_status,
            new_annualized_salary,
            base_salary_increase_percent,
            last_salary_change_percent,
            promotion_approved,
            position,
            manager_user_sys_id,
            all_player_status,
            country_region,
            job_code,
            new_annualized_salary - annualized_salary as salary_increase_delta,
            case
                when promotion_approved = 'No'
                then 'N'
                when promotion_approved = 'Yes'
                then 'Y'
                else null
            end as flag_promotion_key,
            case
                when salary_increase_delta > '0'
                then 'Y'
                when salary_increase_delta <= '0'
                then 'N'
                else null
            end as flag_increase_key,
            annualized_salary_increase_merit_guideline_maximum,
            annualized_salary_increase_merit_guideline_low,
            annualized_salary_increase_merit_guideline_high,
            salary_increase_budget_percent,
            budget_proration,
            salary_increase_budget_amount,
            total_proposed_spend,
            annualized_salary_increase_merit,
            case
                when rating_label = 'N/A' then 'unrated' else rating_label
            end as rating_label,
            case when rating = '-999999' then '-1972' else rating end as rating,
            form_template_id,
            form_template_name,
            compa_ratio,
            new_compa_ratio,
            new_range_penetration,
            range_penetration,
            position_in_range,
            new_position_in_range,
            hire_date,
            frequency,
            periodic_base_salary,
            compa_ratio_1 as compa_ratio_total_cash,
            new_compa_ratio_1 as new_compa_ratio_total_cash,
            new_position_in_range_1 as new_position_in_range_total_cash,
            position_in_range_1 as position_in_range_total_cash,
            one_time_bonus,
            round(
                annualized_salary_increase_merit_guideline_low
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_low,
            round(
                annualized_salary_increase_merit_guideline_high
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_high,
            guidance_low || '%-' || guidance_high || '%' as guidance,
            null as position_title,
            null as form_title,
            null as form_holder_name,
            null as currency,
            null as fixed_pay,
            null as total_cash_target,
            null as new_fixed_pay,
            null as new_total_cash_target,
            null as comp_plan_owner,
            null as comp_plan_eligibility
        from {{ ref("stg_ye_cpsn_report") }} sycr
        union
        select
            user_id,
            employee_id,
            gender,
            business_unit,
            annualized_salary,
            status,
            ec_employee_status,
            new_annualized_salary,
            base_salary_increase_percent,
            last_salary_change_percent,
            promotion_approved,
            position,
            manager_user_sys_id,
            all_player_status,
            country_region,
            job_code,
            new_annualized_salary - annualized_salary as salary_increase_delta,
            case
                when promotion_approved = 'No'
                then 'N'
                when promotion_approved = 'Yes'
                then 'Y'
                else null
            end as flag_promotion_key,
            case
                when salary_increase_delta > '0'
                then 'Y'
                when salary_increase_delta <= '0'
                then 'N'
                else null
            end as flag_increase_key,
            annualized_salary_increase_merit_guideline_maximum,
            annualized_salary_increase_merit_guideline_low,
            annualized_salary_increase_merit_guideline_high,
            salary_increase_budget_percent,
            budget_proration,
            salary_increase_budget_amount,
            total_proposed_spend,
            annualized_salary_increase_merit,
            case
                when rating_label = 'N/A' then 'unrated' else rating_label
            end as rating_label,
            case when rating = '-999999' then '-1972' else rating end as rating,
            form_template_id,
            form_template_name,
            compa_ratio,
            new_compa_ratio,
            new_range_penetration,
            range_penetration,
            position_in_range,
            new_position_in_range,
            hire_date,
            frequency,
            periodic_base_salary,
            compa_ratio_1 as compa_ratio_total_cash,
            new_compa_ratio_1 as new_compa_ratio_total_cash,
            new_position_in_range_1 as new_position_in_range_total_cash,
            position_in_range_1 as position_in_range_total_cash,
            one_time_bonus,
            round(
                annualized_salary_increase_merit_guideline_low
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_low,
            round(
                annualized_salary_increase_merit_guideline_high
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_high,
            guidance_low || '%-' || guidance_high || '%' as guidance,
            null as position_title,
            null as form_title,
            null as form_holder_name,
            null as currency,
            null as fixed_pay,
            null as total_cash_target,
            null as new_fixed_pay,
            null as new_total_cash_target,
            null as comp_plan_owner,
            null as comp_plan_eligibility
        from {{ ref("stg_ye_cpsn_report_aye2022") }} sycry2
        union
        select
            user_id,
            employee_id,
            gender,
            business_unit,
            annualized_salary,
            status,
            ec_employee_status,
            new_annualized_salary,
            base_salary_increase_percent,
            last_salary_change_percent,
            promotion_approved,
            position,
            manager_user_sys_id,
            all_player_status,
            country_region,
            job_code,
            new_annualized_salary - annualized_salary as salary_increase_delta,
            case
                when promotion_approved = 'No'
                then 'N'
                when promotion_approved = 'Yes'
                then 'Y'
                else null
            end as flag_promotion_key,
            case
                when salary_increase_delta > '0'
                then 'Y'
                when salary_increase_delta <= '0'
                then 'N'
                else null
            end as flag_increase_key,
            annualized_salary_increase_merit_guideline_maximum,
            annualized_salary_increase_merit_guideline_low,
            annualized_salary_increase_merit_guideline_high,
            salary_increase_budget_percent,
            budget_proration,
            salary_increase_budget_amount,
            total_proposed_spend,
            annualized_salary_increase_merit,
            case
                when rating_label = 'N/A' then 'unrated' else rating_label
            end as rating_label,
            case when rating = '-999999' then '-1972' else rating end as rating,
            form_template_id,
            form_template_name,
            compa_ratio,
            new_compa_ratio,
            new_range_penetration,
            range_penetration,
            position_in_range,
            new_position_in_range,
            hire_date,
            frequency,
            periodic_base_salary,
            compa_ratio_1 as compa_ratio_total_cash,
            new_compa_ratio_1 as new_compa_ratio_total_cash,
            new_position_in_range_1 as new_position_in_range_total_cash,
            position_in_range_1 as position_in_range_total_cash,
            one_time_bonus,
            round(
                annualized_salary_increase_merit_guideline_low
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_low,
            round(
                annualized_salary_increase_merit_guideline_high
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_high,
            guidance_low || '%-' || guidance_high || '%' as guidance,
            null as position_title,
            null as form_title,
            null as form_holder_name,
            null as currency,
            null as fixed_pay,
            null as total_cash_target,
            null as new_fixed_pay,
            null as new_total_cash_target,
            null as comp_plan_owner,
            null as comp_plan_eligibility
        from {{ ref("stg_ye_cpsn_report_ye2023") }} sycr3
        union
        select
            user_id,
            employee_id,
            gender,
            business_unit,
            annualized_salary,
            null as status,
            null as ec_employee_status,
            new_annualized_salary,
            base_salary_increase_percent,
            null as last_salary_change_percent,
            promotion_approved,
            null as position,
            manager_user_sys_id,
            all_player_status,
            null as country_region,
            job_code,
            new_annualized_salary - annualized_salary as salary_increase_delta,
            case
                when promotion_approved = 'No'
                then 'N'
                when promotion_approved = 'Yes'
                then 'Y'
                else null
            end as flag_promotion_key,
            case
                when salary_increase_delta > '0'
                then 'Y'
                when salary_increase_delta <= '0'
                then 'N'
                else null
            end as flag_increase_key,
            annualized_salary_increase_merit_guideline_maximum,
            annualized_salary_increase_merit_guideline_low,
            annualized_salary_increase_merit_guideline_high,
            null as salary_increase_budget_percent,
            null as budget_proration,
            salary_increase_budget_amount,
            total_proposed_spend,
            annualized_salary_increase_merit,
            case
                when rating_label = 'N/A' then 'Unrated' else rating_label
            end as rating_label,
            case
                when UPPER(rating_label) = UPPER('Unrated')
                then '-1972'
                when UPPER(rating_label) = UPPER('Too New To Rate')
                then '-1971'
                when UPPER(rating_label) = UPPER('Does Not Achieve Expectations')
                then '1'
                when UPPER(rating_label) = UPPER('Exceeds Expectations')
                then '4'
                when UPPER(rating_label) = UPPER('Partially Achieves Expectations')
                then '2'
                when UPPER(rating_label) = UPPER('Outstanding')
                then '5'
                when UPPER(rating_label) = UPPER('Achieves Expectations')
                then '3'
                else null
            end as rating,
            form_template_id,
            form_template_name,
            null as compa_ratio,
            -- try_to_number(new_compa_ratio) as new_compa_ratio,
            null as new_compa_ratio,
            null as new_range_penetration,
            null as range_penetration,
            null as position_in_range,
            null as new_position_in_range,
            hire_date,
            null as frequency,
            periodic_base_salary,
            compa_ratio_1 as compa_ratio_total_cash,
            try_to_number(
                replace(new_compa_ratio_1, '%', ''), 38, 2
            ) as new_compa_ratio_total_cash,
            new_position_in_range_1 as new_position_in_range_total_cash,
            position_in_range_1 as position_in_range_total_cash,
            one_time_bonus,
            round(
                annualized_salary_increase_merit_guideline_low
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_low,
            round(
                annualized_salary_increase_merit_guideline_high
                * 100
                / iff(annualized_salary = 0, null, annualized_salary),
                2
            ) as guidance_high,
            guidance_low || '%-' || guidance_high || '%' as guidance,
            position_title,
            form_title,
            upper(current_owner_last_name)
            || ' '
            || current_owner_first_name as form_holder_name,
            currency_code as currency,
            fixed_pay,
            total_cash_target,
            new_fixed_pay,
            new_total_cash_target,
            comp_plan_owner,
            comp_plan_eligibility
        from {{ ref("stg_ye_cpsn_report_ye2024") }} sycr4
    )
select
    user_id,
    employee_id,
    gender,
    business_unit,
    status,
    ec_employee_status,
    position,
    manager_user_sys_id,
    all_player_status,
    country_region,
    annualized_salary,
    new_annualized_salary,
    base_salary_increase_percent,
    last_salary_change_percent,
    promotion_approved,
    job_code,
    salary_increase_delta,
    flag_promotion_key,
    flag_increase_key,
    annualized_salary_increase_merit_guideline_maximum,
    annualized_salary_increase_merit_guideline_low,
    annualized_salary_increase_merit_guideline_high,
    salary_increase_budget_percent,
    budget_proration,
    salary_increase_budget_amount,
    total_proposed_spend,
    annualized_salary_increase_merit,
    case
        when flag_promotion_key = 'Y'
        then 'N/A'
        else
            case
                when base_salary_increase_percent between guidance_low and guidance_high
                then 'Y'
                when base_salary_increase_percent is null
                then null
                else 'N'
            end
    end as flag_conformed_to_guidance_key,
    rating_label,
    rating,
    form_template_id,
    form_template_name,
    compa_ratio,
    new_compa_ratio,
    new_range_penetration,
    range_penetration,
    position_in_range,
    new_position_in_range,
    hire_date,
    frequency,
    periodic_base_salary,
    compa_ratio_total_cash,
    new_compa_ratio_total_cash,
    new_position_in_range_total_cash,
    position_in_range_total_cash,
    one_time_bonus,
    guidance_low,
    guidance_high,
    guidance,
    position_title,
    form_title,
    form_holder_name,
    currency,
    fixed_pay,
    total_cash_target,
    new_fixed_pay,
    new_total_cash_target,
    comp_plan_owner,
    Case when comp_plan_eligibility in ('Use Policy Rules','Override Policy Rules') then 'Yes' else 'No' end as comp_plan_eligibility
    from cpsn_report_int
