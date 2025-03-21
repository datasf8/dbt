{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        on_schema_change="sync_all_columns",
    )
}}
with
    dates as (
        select *
        from {{ ref("dates_referential") }}
        where date_yyyy_mm_dd = date_month_end_date and date_id > 20220100
    ),
    evnt_rsns as (
        select event_reasons_id as termination_reasons_id, event_reasons_id, erc.*
        from {{ ref("event_reasons") }} er
        join {{ ref("event_reasons_categories") }} erc using (event_reasons_code)
        where is_group_flag = true
    ),
    country_zones as (
        select country_code, geographic_zone_code
        from {{ ref("company") }} co
        qualify
            row_number() over (
                partition by country_code order by company_start_date desc
            )
            = 1
    ),
    country as (
        select * from {{ ref("country") }} c join country_zones cz using (country_code)
    ),

    emp_files_headcount as (
        select
            ef.*,
            legal_gender_code,
            key_position_type_code,
            all_players_status_code,
            employee_group_code,
            decode(
                true,
                c.country_code = cn.country_code,
                'Nationals',
                c.country_code != cn.country_code
                and c.geographic_zone_code = cn.geographic_zone_code,
                'Regionals',
                'Internationals'
            ) as user_type,
            headcount_type_code
        from {{ ref("employee_files") }} ef
        left join {{ ref("legal_gender") }} lg using (legal_gender_id)
        left join {{ ref("key_position_type") }} kp using (key_position_type_id)
        left join {{ ref("all_players_status") }} ap using (all_players_status_id)
        left join {{ ref("employee_group") }} eg using (employee_group_id)
        left join country c using (country_id)
        left join country cn on ef.country_of_nationality_id = cn.country_id
        join {{ ref("headcount") }} hc using (user_id, job_start_date)
        where headcount_present_flag = 1
    ),

    headcount as (
        select
            date_id as dates_referential_id,
            date_month_end_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as hdc_hdc,
            iff(legal_gender_code = 'M', 1::decimal(38, 9), null) hdc_pmhdc,
            iff(legal_gender_code = 'F', 1::decimal(38, 9), null) hdc_pfhdc,
            iff(
                all_players_status_code in ('1', '2', '3'), 1::decimal(38, 9), null
            ) as hdc_kp,
            iff(key_position_type_code = 'LKP', 1::decimal(38, 9), null) hdc_lkp,
            iff(
                key_position_type_code in ('SKP', 'GKP'), 1::decimal(38, 9), null
            ) hdc_sgkp,
            iff(hdc_pfhdc = 1 and hdc_lkp = 1, 1::decimal(38, 9), null) as hdc_pflkp,
            iff(hdc_pmhdc = 1 and hdc_lkp = 1, 1::decimal(38, 9), null) as hdc_mlkp,
            iff(hdc_kp = 1 and hdc_lkp = 1, 1::decimal(38, 9), null) as hdc_kplkp,
            iff(hdc_pmhdc = 1 and hdc_sgkp = 1, 1::decimal(38, 9), null) as hdc_msgkp,
            iff(
                hdc_lkp = 1 and user_type = 'Nationals', 1::decimal(38, 9), null
            ) as hdc_nlkp,
            iff(
                hdc_lkp = 1 and user_type = 'Regionals', 1::decimal(38, 9), null
            ) as hdc_rlkp,
            iff(
                hdc_lkp = 1 and user_type = 'Internationals', 1::decimal(38, 9), null
            ) as hdc_ilkp,
            iff(employee_group_code = 'EG0008', 1::decimal(38, 9), null) as hdc_im,
            iff(
                (months_between(date_month_end_date, global_assignment_start_date)) > 60  -- 5 Years
                and hdc_im = 1,
                1::decimal(38, 9),
                null
            ) as hdc_im5y,
            round(months_between(date_month_end_date, date_of_birth) / 12, 3)::decimal(
                38, 9
            ) as hdc_aa,
            round(months_between(date_month_end_date, group_seniority), 3)::decimal(
                38, 9
            ) as hdc_ags,
            round(months_between(date_month_end_date, job_entry_date), 3)::decimal(
                38, 9
            ) as hdc_ajs
        from emp_files_headcount ef
        join
            dates
            on date_month_end_date
            between employee_file_start_date and employee_file_end_date
        left join
            {{ ref("employee_global_assignment") }} ega using (personal_id, user_id)
    ),
    avgheadcount as (
        select
            date_id as dates_referential_id,
            date_month_end_date as used_date,
            headcount_type_code,
            employee_files_id,
            'HDC_AHDC' as employee_indicators_code,
            1::decimal(38, 9) as value
        from headcount hc
        join dates on hc.used_date between date_year_start_date and date_month_end_date
    ),

    employee_time as (
        select date_id, user_id, count(*) as actions
        from {{ ref("employee_time") }} et
        join
            dates
            on approval_status = 'APPROVED'
            and time_start_date between date_month_start_date and date_month_end_date
        group by all
    ),

    employee_learning as (
        select date_id, user_id, count(*) as actions
        from {{ ref("learning_activity") }} la
        join
            dates dr
            on la.completion_date
            between dr.date_month_start_date and dr.date_month_end_date
        group by all
    ),

    employee_skills as (
        select date_id, es.personal_id, count(*) as actions
        from
            {{ env_var("DBT_SDDS_DB") }}.btdp_ds_c2_h17_growth_portfolio_eu_{{ env_var("DBT_REGION") }}_private.employee_skills es
        join
            dates dr
            on es.employee_skill_start_date
            between dr.date_month_start_date and dr.date_month_end_date
        group by all
    ),

    employee_skills_changes_rated_liked as (
        select
            date_id,
            es.personal_id,
            skill_code,
            is_preference_value_flag,
            lag(is_preference_value_flag) over (
                partition by personal_id, skill_code order by employee_skill_start_date
            ) as prev_is_preference_value_flag,
            coalesce(
                employee_skill_proficiency_level, 0
            ) as employee_skill_proficiency_level,
            lag(coalesce(employee_skill_proficiency_level, 0)) over (
                partition by personal_id, skill_code order by employee_skill_start_date
            ) as prev_employee_skill_proficiency_level
        from
            {{ env_var("DBT_SDDS_DB") }}.btdp_ds_c2_h17_growth_portfolio_eu_{{ env_var("DBT_REGION") }}_private.employee_skills es
        join
            dates dr
            on es.employee_skill_start_date
            between dr.date_month_start_date and dr.date_month_end_date
            and employee_skill_status = 'ACTIVE'
    ),

    employee_skills_actions_rated_liked as (
        select
            date_id,
            personal_id,
            sum(
                case
                    when
                        prev_is_preference_value_flag is null
                        and is_preference_value_flag = 1
                    then 1
                    when is_preference_value_flag != prev_is_preference_value_flag
                    then 1
                    else 0
                end
            ) as actions_liked,
            sum(
                case
                    when
                        prev_employee_skill_proficiency_level is null
                        and employee_skill_proficiency_level > 0
                    then 1
                    when
                        employee_skill_proficiency_level
                        != prev_employee_skill_proficiency_level
                    then 1
                    else 0
                end
            ) as actions_rated,
        from employee_skills_changes_rated_liked escrl
        group by all
    ),

    adoption_time_off as (
        select
            date_id as dates_referential_id,
            date_month_end_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as ado_muuetim,
            iff(et.user_id is not null, 1::decimal(38, 9), null) ado_muutim,
            iff(ado_muutim = 1, actions::decimal(38, 9), null) ado_matim
        from emp_files_headcount ef
        join
            dates
            on date_month_end_date
            between employee_file_start_date and employee_file_end_date
        left join employee_time et using (date_id, user_id)
        left join {{ ref("time_type_profile") }} ttp using (time_type_profile_id)
        where is_using_timeoff_flag = 'Yes' and headcount_type_code = 'STA'

    ),

    adoption_skills as (
        select
            date_id as dates_referential_id,
            date_month_end_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as ado_muuegp,
            1::decimal(38, 9) as ado_muuelgp,
            1::decimal(38, 9) as ado_muuergp,
            iff(es.personal_id is not null, 1::decimal(38, 9), null) as ado_muugp,
            iff(ado_muugp = 1, es.actions::decimal(38, 9), null) as ado_magp,
            iff(
                esarl.personal_id is not null and esarl.actions_liked > 0,
                1::decimal(38, 9),
                null
            ) as ado_muulgp,
            iff(ado_muulgp = 1, esarl.actions_liked::decimal(38, 9), null) as ado_malgp,
            iff(
                esarl.personal_id is not null and esarl.actions_rated > 0,
                1::decimal(38, 9),
                null
            ) as ado_muurgp,
            iff(ado_muurgp = 1, esarl.actions_rated::decimal(38, 9), null) as ado_margp
        from emp_files_headcount ef
        join
            dates
            on date_month_end_date
            between employee_file_start_date and employee_file_end_date
        left join employee_skills es using (date_id, personal_id)
        left join employee_skills_actions_rated_liked esarl using (date_id, personal_id)
        where headcount_type_code = 'STA'

    ),

    adoption_learning as (
        select
            date_id as dates_referential_id,
            date_month_end_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as ado_muuelms,
            iff(el.user_id is not null, 1::decimal(38, 9), null) as ado_muulms,
            iff(ado_muulms = 1, el.actions::decimal(38, 9), null) as ado_malms
        from emp_files_headcount ef
        join
            dates
            on date_month_end_date
            between employee_file_start_date and employee_file_end_date
        left join employee_learning el using (date_id, user_id)
        where headcount_type_code = 'STA'
    ),

    pay_components_recurring as (
        select *
        from {{ ref("pay_components_recurring") }}
        qualify
            row_number() over (
                partition by
                    user_id, pay_component_code, pay_component_recurring_start_date
                order by pay_component_recurring_sequence_number desc
            )
            = 1
    ),
    compensation as (
        select
            date_id,
            user_id,
            sum(
                iff(
                    pay_component_group_code = 'TOTALCOMPENSATION',
                    pay_component_recurring_calculated_amount,
                    0
                )
            ) as gen_tcp  -- pay_component_recurring_frequency
        from pay_components_recurring pcr
        join
            dates
            on date_month_end_date
            between pay_component_recurring_start_date
            and pay_component_recurring_end_date
        left join {{ ref("pay_component_mapping") }} pcm using (pay_component_id)
        group by all
    ),
    kpi_compensation as (
        select
            date_id as dates_referential_id,
            date_month_end_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as gen_epr,
            gen_tcp::decimal(38, 9) as gen_tcp
        from emp_files_headcount ef
        join
            dates
            on date_month_end_date
            between employee_file_start_date and employee_file_end_date
        join compensation using (date_id, user_id)
    ),

    hire as (
        select
            date_id as dates_referential_id,
            hire_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as hir_glh,
            iff(legal_gender_code = 'M', 1::decimal(38, 9), null) hdc_pmh
        from emp_files_headcount ef
        join dates on hire_date between date_year_start_date and date_month_end_date
        left join evnt_rsns er using (event_reasons_id)
    ),
    turnover as (
        select
            date_id as dates_referential_id,
            termination_date as used_date,
            headcount_type_code,
            employee_files_id,
            1::decimal(38, 9) as trn_gt,
            iff(
                event_reasons_category = 'Employee Decision', 1::decimal(38, 9), null
            ) as trn_edt,
            iff(
                event_reasons_category = 'Company Decision', 1::decimal(38, 9), null
            ) as trn_cdt,
            iff(
                all_players_status_code in ('1', '2', '3'), 1::decimal(38, 9), null
            ) as trn_kpt,
            round(months_between(termination_date, group_seniority) / 12, 3)::decimal(
                38, 9
            ) as trn_ayt
        from emp_files_headcount ef
        join
            dates
            on termination_date between date_year_start_date and date_month_end_date
        left join evnt_rsns er using (termination_reasons_id)
    ),  -- 117

    kpidata as (
        select * exclude(hdc_pfhdc)  -- dates_referential_id,employee_indicators_referential_id,sum(value) matrix_value
        from
            headcount unpivot (
                value for employee_indicators_code in (
                    hdc_hdc,
                    hdc_pmhdc,
                    hdc_kp,
                    hdc_lkp,
                    hdc_sgkp,
                    hdc_pflkp,
                    hdc_kplkp,
                    hdc_mlkp,
                    hdc_msgkp,
                    hdc_nlkp,
                    hdc_rlkp,
                    hdc_ilkp,
                    hdc_im,
                    hdc_im5y,
                    hdc_aa,
                    hdc_ags,
                    hdc_ajs
                )
            )
        where value is not null  -- and dates_referential_id > 20241000 --group by all
        union all
        select * replace(value / (floor(dates_referential_id / 100) % 100) as value)  -- dates_referential_id,employee_indicators_referential_id,sum(value) matrix_value
        from avgheadcount
        union all
        select *  -- dates_referential_id,employee_indicators_referential_id,sum(value) matrix_value
        from hire unpivot (value for employee_indicators_code in (hir_glh, hdc_pmh))
        where value is not null  -- and dates_referential_id > 20241000 --group by all
        union all
        select *  -- dates_referential_id,employee_indicators_referential_id,sum(value) matrix_value
        from
            turnover unpivot (
                value for employee_indicators_code
                in (trn_gt, trn_edt, trn_cdt, trn_kpt, trn_ayt)
            )
        where value is not null  -- and dates_referential_id > 20241000 --group by all
        union all
        select *
        from
            adoption_time_off unpivot (
                value for employee_indicators_code
                in (ado_muuetim, ado_muutim, ado_matim)
            )
        where value is not null
        union all
        select *
        from
            adoption_skills unpivot (
                value for employee_indicators_code in (
                    ado_muuegp,
                    ado_muugp,
                    ado_magp,
                    ado_muuelgp,
                    ado_muuergp,
                    ado_muulgp,
                    ado_malgp,
                    ado_muurgp,
                    ado_margp
                )
            )
        where value is not null
        union all
        select *
        from
            adoption_learning unpivot (
                value for employee_indicators_code
                in (ado_muuelms, ado_muulms, ado_malms)
            )
        where value is not null
        union all
        select *
        from
            kpi_compensation
            unpivot (value for employee_indicators_code in (gen_epr, gen_tcp))
        where value is not null
    )
select
    hash(
        dates_referential_id, employee_indicators_referential_id, employee_files_id
    ) as employees_by_indicator_id,
    dates_referential_id,
    used_date,
    employee_indicators_referential_id,
    employee_files_id,
    value
from kpidata kd
join
    {{ ref("employee_indicators_referential") }} using (
        headcount_type_code, employee_indicators_code
    )
