{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
         on_schema_change='sync_all_columns',
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','JOB_INFORMATION');"
    )
}}
with
    jobinfo0 as (
        select
            * replace (
                seqnumber::number(38, 0) as seqnumber
            ) rename(
                userid as user_id,
                seqnumber as sequence_number,
                startdate as job_start_date,
                enddate as job_end_date
            ),
            row_number() over (
                partition by userid, startdate order by seqnumber desc
            ) as active_seq
        from {{ ref("stg_emp_job_flatten") }}
        where dbt_valid_to is null  -- and userid = '00513894'
    ),
    jobinfo1 as (
        select
            user_id,
            job_start_date,
            sequence_number,
            least_ignore_nulls(
                job_end_date,
                lead(job_start_date, 1) over (
                    partition by user_id order by job_start_date
                )
                - 1
            ) as job_end_date
        from jobinfo0
        where active_seq = 1
    )
select
    hash(user_id, job_start_date, sequence_number) as jobinfo_id,
    user_id,
    sequence_number,
    job_start_date,
    nvl(e.job_end_date, ji.job_end_date) as job_end_date,
    jobcode as job_role_code,
    jr.job_role_id,
    jobtitle as job_title,
    costcenter as cost_center_code,
    cc.cost_center_id,
    customstring5 as brand_code,
    b.brand_id,
    customstring18 as functional_area_code,
    fa.functional_area_id,
    customstring17 as organizational_area_code,
    oa.organizational_area_id,
    customstring32 as local_pay_grade_code,
    lpg.local_pay_grade_id,
    customstring4 as employee_group_code,
    eg.employee_group_id,
    customstring31 as employee_subgroup_code,
    esg.employee_subgroup_id,
    position as position_code,
    pos.position_id,
    customstring9 as prime_yn_flag_id,
    emplstatus as employee_status_id,
    isfulltimeemployee as fulltime_employee_flag,
    fte::decimal(38, 6) as fte,
    standardhours::decimal(38, 2) as standard_weekly_hours,
    contracttype as local_contract_type_id,
    contractenddate as contract_end_date,
    customstring14 as salary_review_calendar_id,
    customstring13 as bonus_weighting_id,
    customstring25 as bonus_earning_eligibility_id,
    customstring10 as on_secondment_yn_flag_id,
    employeeclass as employee_class_yn_flag_id,
    customstring41 as included_headcount_yn_flag_id,
    iscompetitionclauseactive as competition_clause_flag,
    customstring16 as probation_status_id,
    probationperiodenddate as probationary_period_end_date,
    jobentrydate as job_entry_date,
    companyentrydate as company_entry_date,
    customdate1 as secondment_expected_end_date,
    customstring12 as home_host_designation_id,
    customstring20 as employee_notice_period_number_id,
    customstring21 as employee_notice_period_unit_id,
    customstring22 as management_training_program_yn_flag_id,
    eventreason as event_reasons_code,
    er.event_reasons_id,
    customstring44 as ec_employee_status,
    case
        when eg.employee_group_name_en = 'Fixed Term Employee'
        then
            datediff(
                'dd',
                company_entry_date,
                nvl(least(contract_end_date, current_date), current_date)
            )
        else null
    end as local_contract_duration,
    case
        when sp.specialization_name_en in ('E-Beauty Advisory', 'Beauty Advisory')
        then 'True'
        else 'False'
    end as is_beauty_advisor_flag,
    workschedulecode as work_schedule_code,
    ws.work_schedule_id,
    holidaycalendarcode as holiday_calendar_code,
    hc.holiday_calendar_id,
    timetypeprofilecode as time_type_profile_code,
    ttp.time_type_profile_id,
    customstring15 as key_position_type_id,
    case
        when length(managerid) < 8 then lpad(managerid, 8, '0') else managerid
    end as manager_user_id,
    customstring30 as hr_manager_position_code,
    hrpos.position_id as hr_manager_position_id,
    flsastatus as flsa_status_id,
    customstring7 as dq_specialization_code,
    spz.specialization_id as dq_specialization_id,
    customstring6 as dq_professional_field_code,
    pf.professional_field_id as dq_professional_field_id,
    department as dq_department,
    bu.business_unit_id as dq_business_unit_id,
    businessunit as dq_businessunit_code,
    hd.hr_division_id as dq_hr_division_id,
    division as dq_division_code,
    ar.area_id as dq_area_id,
    company as dq_company_code,
    cp.company_id as dq_company_id,
    countryofcompany as dq_country_code,
    c.country_id as dq_country_id
from jobinfo0 ji
left join jobinfo1 e using (user_id, job_start_date, sequence_number)
left join
    {{ ref("job_role") }} jr
    on ji.jobcode = jr.job_role_code
    and job_start_date <= job_role_end_date
    and job_end_date >= job_role_start_date
left join
    {{ ref("cost_center") }} cc
    on ji.costcenter = cc.cost_center_code
    and job_start_date <= cost_center_end_date
    and job_end_date >= cost_center_start_date
left join
    {{ ref("brand") }} b
    on ji.customstring5 = b.brand_code
    and job_start_date <= brand_end_date
    and job_end_date >= brand_start_date
left join
    {{ ref("functional_area") }} fa
    on ji.customstring18 = fa.functional_area_code
    and job_start_date <= functional_area_end_date
    and job_end_date >= functional_area_start_date
left join
    {{ ref("organizational_area") }} oa
    on ji.customstring17 = oa.organizational_area_code
    and job_start_date <= organizational_area_end_date
    and job_end_date >= organizational_area_start_date
left join
    {{ ref("local_pay_grade") }} lpg
    on ji.customstring32 = lpg.local_pay_grade_code
    and job_start_date <= local_pay_grade_end_date
    and job_end_date >= local_pay_grade_start_date
left join
    {{ ref("employee_group") }} eg
    on ji.customstring4 = eg.employee_group_code
    and job_start_date <= employee_group_end_date
    and job_end_date >= employee_group_start_date
left join
    {{ ref("employee_subgroup") }} esg
    on ji.customstring31 = esg.employee_subgroup_code
    and job_start_date <= employee_subgroup_end_date
    and job_end_date >= employee_subgroup_start_date
left join
    {{ ref("position") }} pos
    on ji.position = pos.position_code
    and job_start_date <= pos.position_end_date
    and job_end_date >= pos.position_start_date
left join
    {{ ref("event_reasons") }} er
    on ji.eventreason = er.event_reasons_code
    and job_start_date <= event_reasons_end_date
    and job_end_date >= event_reasons_start_date
left join {{ ref("work_schedule") }} ws on ji.workschedulecode = ws.work_schedule_code
left join
    {{ ref("holiday_calendar") }} hc
    on ji.holidaycalendarcode = hc.holiday_calendar_code
left join {{ ref("specialization") }} sp using (specialization_code)
left join
    {{ ref("pre_time_type_profile") }} ttp
    on ji.timetypeprofilecode = ttp.time_type_profile_code
    and job_start_date <= time_type_profile_end_date
    and job_end_date >= time_type_profile_start_date
left join
    {{ ref("position") }} hrpos
    on ji.customstring30 = hrpos.position_code
    and job_start_date <= hrpos.position_end_date
    and job_end_date >= hrpos.position_start_date
left join
    {{ ref("specialization") }} spz
    on ji.customstring7 = spz.specialization_code
    and job_start_date <= spz.specialization_end_date
    and job_end_date >= spz.specialization_start_date
left join
    {{ ref("professional_field") }} pf
    on ji.customstring6 = pf.professional_field_code
    and job_start_date <= pf.professional_field_end_date
    and job_end_date >= pf.professional_field_start_date
left join
    {{ ref("business_unit") }} bu
    on ji.department = bu.business_unit_code
    and job_start_date <= bu.business_unit_end_date
    and job_end_date >= bu.business_unit_start_date
left join
    {{ ref("hr_division") }} hd
    on ji.businessunit = hd.hr_division_code
    and job_start_date <= hd.hr_division_end_date
    and job_end_date >= hd.hr_division_start_date
left join
    {{ ref("area") }} ar
    on ji.division = ar.area_code
    and job_start_date <= ar.area_end_date
    and job_end_date >= ar.area_start_date
left join
    {{ ref("company") }} cp
    on ji.company = cp.company_code
    and job_start_date <= cp.company_end_date
    and job_end_date >= cp.company_start_date
left join
    {{ ref("country") }} c
    on ji.countryofcompany = c.country_code
    and job_start_date <= c.country_end_date
    and job_end_date >= c.country_start_date
qualify
    row_number() over (
        partition by user_id, sequence_number, job_start_date
        order by
            job_role_start_date desc,
            cost_center_start_date desc,
            brand_start_date desc,
            functional_area_start_date desc,
            organizational_area_start_date desc,
            local_pay_grade_start_date desc,
            employee_group_start_date desc,
            employee_subgroup_start_date desc,
            pos.position_start_date desc,
            event_reasons_start_date desc,
            time_type_profile_start_date desc,
            hrpos.position_start_date desc,
            spz.specialization_start_date desc,
            pf.professional_field_start_date desc,
            bu.business_unit_start_date desc,
            hd.hr_division_start_date desc,
            ar.area_start_date desc,
            cp.company_start_date desc,
            c.country_start_date desc
    )
    = 1
