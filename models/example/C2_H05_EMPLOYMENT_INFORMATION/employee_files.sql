{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call rls_policy_apply_sp('{{ database }}','{{ schema }}','EMPLOYEE_FILES');",
    )
}}
with
    jobinfo as (
        select
            job_start_date as ji_sd,
            job_end_date as ji_ed,
            ji.*,
            -- personal_id,
            iff(job_end_date != '9999-12-31', job_end_date + 1, null) as mobility_date
        from {{ ref("job_information") }} ji
        -- left join  -- To get Personal ID even if Employment is not there
        -- (
        -- select user_id, max(personal_id) personal_id
        -- from {{ ref("employment_details") }}
        -- group by all
        -- ) using (user_id)
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    jn_ji_ed as (
        select
            greatest_ignore_nulls(ji_sd, employment_details_start_date) jied_sd,
            least_ignore_nulls(ji_ed, employment_details_end_date) jied_ed,
            lead(jied_sd) over (
                partition by ji.user_id, ji_sd order by employment_details_start_date
            ) next_jied_sd,
            lag(jied_ed) over (
                partition by ji.user_id, ji_sd order by employment_details_start_date
            ) prev_jied_ed,
            ji.*,
            nvl(
                position_code,
                lag(position_code) ignore nulls over (
                    partition by ji.user_id order by ji_sd
                )
            ) as calc_position_code,
            lead(event_reasons_id) over (
                partition by ji.user_id order by ji_sd
            ) next_event_reasons_id,
            personal_id,
            employment_details_start_date,
            employment_details_end_date
        from jobinfo ji
        left join
            {{ ref("employment_details") }} ed
            on ji.user_id = ed.user_id
            and ji_sd <= employment_details_end_date
            and ji_ed >= employment_details_start_date
    ),
    i_ji_ed as (
        select  -- Pre
            nvl(prev_jied_ed + 1, ji_sd) as jied_isd,
            jied_sd - 1 as jied_ied,
            d.* replace(
                null as personal_id,
                null as employment_details_start_date,
                null as employment_details_end_date
            )
        from jn_ji_ed d
        where (prev_jied_ed is null and jied_sd != ji_sd) or prev_jied_ed + 1 < jied_sd
        union all
        select jied_sd as jied_isd, jied_ed as jied_ied, d.*
        from jn_ji_ed d
        union all
        select  -- Post
            jied_ed + 1 as jied_isd,
            ji_ed as jied_ied,
            d.* replace(
                null as personal_id,
                null as employment_details_start_date,
                null as employment_details_end_date
            )
        from jn_ji_ed d
        where (next_jied_sd is null and jied_ed != ji_ed)
    ),
    jied as (
        select
            * exclude(
                next_jied_sd,
                prev_jied_ed,
                jied_sd,
                jied_ed
            ) rename(jied_isd as jied_sd, jied_ied as jied_ed)
        from i_ji_ed
    ),
    jn_jied_pi as (
        select
            greatest_ignore_nulls(jied_sd, personal_info_start_date) jiedpi_sd,
            least_ignore_nulls(jied_ed, personal_info_end_date) jiedpi_ed,
            lead(jiedpi_sd) over (
                partition by jied.user_id, jied_sd order by personal_info_start_date
            ) next_jiedpi_sd,
            lag(jiedpi_ed) over (
                partition by jied.user_id, jied_sd order by personal_info_start_date
            ) prev_jiedpi_ed,
            jied.*,
            personal_info_start_date,
            personal_info_end_date
        from jied
        left join
            {{ ref("personal_information") }} pi
            on jied.personal_id = pi.personal_id
            and jied_sd <= personal_info_end_date
            and jied_ed >= personal_info_start_date
    ),
    i_jied_pi as (
        select  -- Pre
            nvl(prev_jiedpi_ed + 1, jied_sd) as jiedpi_isd,
            jiedpi_sd - 1 as jiedpi_ied,
            d.* replace(
                null as personal_info_start_date, null as personal_info_end_date
            )
        from jn_jied_pi d
        where
            (prev_jiedpi_ed is null and jiedpi_sd != jied_sd)
            or prev_jiedpi_ed + 1 < jiedpi_sd
        union all
        select jiedpi_sd as jiedpi_isd, jiedpi_ed as jiedpi_ied, d.*
        from jn_jied_pi d
        union all
        select  -- Post
            jiedpi_ed + 1 as jiedpi_isd,
            jied_ed as jiedpi_ied,
            d.* replace(
                null as personal_info_start_date, null as personal_info_end_date
            )
        from jn_jied_pi d
        where (next_jiedpi_sd is null and jiedpi_ed != jied_ed)
    ),
    jiedpi as (
        select
            * exclude(
                next_jiedpi_sd,
                prev_jiedpi_ed,
                jiedpi_sd,
                jiedpi_ed
            ) rename(
                jiedpi_isd as employee_file_start_date,
                jiedpi_ied as employee_file_end_date
            )
        from i_jied_pi
    ),
    ji_active as (
        select *
        from jobinfo
        where employee_status_id in (32245, 32238, 32244, 32241, 32236, 32243)
    ),
    job_mobility as (
        select distinct user_id, event_reasons_id, mobility_date, mobility_type
        from {{ ref("job_mobility") }} jm
        join
            {{ ref("event_reasons") }} er
            on jm.event_reasons_code = er.event_reasons_code
            and mobility_date
            between event_reasons_start_date and event_reasons_end_date
            and mobility_type in ('TERMINATION', 'HIRING')
    ),
    final as (
        select
            f.* replace (po.position_id as position_id),
            po.location_id,
            ethnicity_id,
            race_1_id as race_id,
            ji_active.user_id as hr_manager_user_id,
            jmh.mobility_date as hire_date,
            jmt.mobility_date - 1 as termination_date,
            jmt.event_reasons_id as termination_reasons_id
        from jiedpi f
        left join
            {{ ref("position") }} po
            on f.calc_position_code = po.position_code
            and f.employee_file_start_date <= position_end_date
            and f.employee_file_end_date >= position_start_date
        left join
            {{ ref("personal_information_local_usa") }} pu
            on f.personal_id = pu.personal_id
            and f.employee_file_start_date <= personal_info_usa_end_date
            and f.employee_file_end_date >= personal_info_usa_start_date
        left join  -- To get HR Manager User ID
            ji_active
            on f.hr_manager_position_code = ji_active.position_code
            and f.employee_file_start_date <= ji_active.job_end_date
            and f.employee_file_end_date >= ji_active.job_start_date
        left join
            job_mobility jmh
            on f.user_id = jmh.user_id
            and f.employee_file_start_date = jmh.mobility_date
            and jmh.mobility_type = 'HIRING'
        left join
            job_mobility jmt
            on f.user_id = jmt.user_id
            and f.employee_file_end_date = jmt.mobility_date - 1
            and jmt.mobility_type = 'TERMINATION'
        qualify
            row_number() over (
                partition by f.user_id, employee_file_start_date
                order by
                    position_start_date desc,
                    personal_info_usa_start_date desc,
                    ji_active.job_start_date desc
            )
            = 1
    )
select
    hash(user_id, employee_file_start_date) as employee_files_id,
    personal_id,
    user_id,
    employee_file_start_date,
    employee_file_end_date,
    /* personal_information */
    personal_info_start_date,
    personal_info_end_date,
    legal_first_name,
    legal_last_name,
    preferred_first_name,
    preferred_last_name,
    second_last_name,
    middle_name,
    alternative_first_name,
    alternative_last_name,
    birth_name,
    formal_name,
    salutation_id,
    suffix_id,
    prefered_language_id,
    legal_gender_id,
    marital_status_id,
    marital_status_since,
    first_country_id as country_of_nationality_id,
    second_country_id,
    third_country_id,
    special_accountability_type_id,
    all_player_status_id::int as all_players_status_id,
    disability_status_id,
    attachment_code,
    /* biographical_information */
    loreal_pass_id,
    date_of_birth,
    country_of_birth_id,
    region_of_birth,
    place_of_birth,
    /* employment_details */
    employment_details_start_date,
    employment_details_end_date,
    time_pay_seniority,
    group_seniority,
    int_transfer_ga_date,
    company_seniority,
    benefits_start_date,
    /* job_information */
    jobinfo_id,
    job_start_date,
    job_end_date,
    sequence_number,
    job_role_id,
    job_title,
    cost_center_id,
    brand_id,
    functional_area_id,
    organizational_area_id,
    local_pay_grade_id,
    employee_group_id,
    employee_subgroup_id,
    position_id,
    prime_yn_flag_id,
    employee_status_id,
    fulltime_employee_flag,
    fte,
    standard_weekly_hours,
    local_contract_type_id,
    contract_end_date,
    salary_review_calendar_id,
    bonus_weighting_id,
    bonus_earning_eligibility_id,
    on_secondment_yn_flag_id,
    employee_class_yn_flag_id,
    included_headcount_yn_flag_id,
    competition_clause_flag,
    probation_status_id,
    probationary_period_end_date,
    job_entry_date,
    company_entry_date,
    secondment_expected_end_date,
    home_host_designation_id,
    employee_notice_period_number_id,
    employee_notice_period_unit_id,
    management_training_program_yn_flag_id,
    event_reasons_id,
    ec_employee_status,
    local_contract_duration,
    is_beauty_advisor_flag,
    work_schedule_id,
    holiday_calendar_id,
    time_type_profile_id,
    key_position_type_id::int as key_position_type_id,
    manager_user_id,
    hr_manager_position_id,
    hr_manager_user_id,
    flsa_status_id::int as flsa_status_id,
    /* Calculated/Extended Columns */
    global_grade_id,
    job_level_id,
    specialization_id,
    professional_field_id,
    location_id,
    business_unit_id,
    business_unit_type_id,
    hr_division_id,
    company_id,
    geographic_zone_id,
    co.country_id,
    ethnicity_id,
    race_id,
    hire_date,
    termination_date,
    termination_reasons_id
from final f
left join
    {{ ref("employment_details") }} ed using (
        personal_id, user_id, employment_details_start_date
    )
left join
    {{ ref("personal_information") }} pi using (personal_id, personal_info_start_date)
left join {{ ref("biographical_information") }} bi using (personal_id)
left join {{ ref("local_pay_grade") }} lpg using (local_pay_grade_id)
left join {{ ref("global_grade") }} gg using (global_grade_id)
left join {{ ref("job_role") }} jl using (job_role_id)
left join {{ ref("specialization") }} sp using (specialization_id)
left join {{ ref("cost_center") }} cc using (cost_center_id)
left join {{ ref("business_unit") }} bu using (business_unit_id)
left join {{ ref("area") }} ar using (area_id)
left join {{ ref("company") }} co using (company_id)
