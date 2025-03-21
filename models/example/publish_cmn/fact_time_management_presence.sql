{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        cluster_by=["user_id", "headcount_type_code"],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_TIME_MANAGEMENT_PRESENCE');"
    )
}}
with
    wsd as (
        select
            work_schedule_code,
            to_number(day_number) day_number,
            iff(planed_hours = 0, 0, 1) working_day
        from {{ ref("work_schedule_day_v1") }}  -- where work_schedule_code='USA_MTWTF**40Hrs'
        union all
        select
            work_schedule_code,
            to_number(day_number) day_number,
            iff(work_schedule_day_model_code is null, 0, 1) working_day
        from {{ ref("work_schedule_day_model_assignment_v1") }}
    ),
    ws as (
        select *
        from {{ ref("work_schedule_v1") }} ws  -- where work_schedule_code='GBR_BA_DC Warehouse'
        left join
            (
                select work_schedule_code, max(day_number) max_day_number
                from wsd
                group by 1
            ) using (work_schedule_code)
    ),
    wsdf as (
        select
            to_date('2020-01-01') start_date,
            current_date() end_date,
            *,
            (start_date + value::int) time_date,
            dayname(time_date) day_name
        from ws
        join
            wsd using (work_schedule_code),
            table(
                flatten(
                    array_generate_range(0, datediff('day', start_date, end_date) + 1)
                )
            )
        where
            mod((time_date - work_schedule_start_date), max_day_number) + 1
            = wsd.day_number
            and working_day = 1
    ),
    employee_details as (
        select
            *,
            nvl(
                group_seniority,
                lag(group_seniority) ignore nulls over (
                    partition by user_id order by employment_details_start_date
                )
            ) as last_group_seniority
        from {{ ref("employment_details_v1") }}
    ),
    job_info as (
        select *
        from {{ ref("job_information_v1") }}  -- where user_id='00833003'
        left join
            (select distinct user_id, personal_id from employee_details) using (user_id)
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),  -- select * from ji;
    job_info_calc as (
        select
            * exclude position_code,
            nvl(
                position_code,
                lag(position_code) ignore nulls over (
                    partition by user_id order by job_start_date
                )
            ) position_code,
            lag(iff(employee_group_code = 'EG0005', user_id, null)) ignore nulls over (  -- Expatriate (home)
                partition by personal_id order by job_start_date, employee_group_code
            ) home_user_id
        from job_info
    ),
    ji_pos as (
        select *
        from job_info
        qualify
            row_number() over (
                partition by position_code, job_start_date order by job_end_date desc
            )
            = 1
    ),
    hr as (
        select
            ji.user_id,
            date_month_end_date as headcount_date,
            ji.position_code,
            pl.related_position,
            hr.user_id user_id_hr
        from job_info ji
        join
            wsdf w
            on ji.work_schedule_code = w.work_schedule_code
            and w.time_date between ji.job_start_date and ji.job_end_date
        join
            {{ ref("date_month_referential") }} dm
            on w.time_date between date_month_start_date and date_month_end_date
        left join
            {{ ref("position_relationship_v1") }} pl
            on ji.position_code = pl.position_code
            and w.time_date
            between position_relationship_start_date and position_relationship_end_date
        left join
            ji_pos hr
            on pl.related_position = hr.position_code
            and w.time_date between hr.job_start_date and hr.job_end_date
        qualify
            row_number() over (
                partition by ji.user_id, date_month_end_date
                order by w.time_date desc, hr.job_start_date desc
            )
            = 1
    ),
    presence as (
        select
            ji.*,
            w.time_date,
            case
                when
                    bu.business_unit_type_code in ('3', '03', '15')
                    or sp.professional_field_code = 'PF000024'
                then 'No'
                else ttp.is_using_remote_flag
            end is_using_remote_flag
        from job_info_calc ji
        join
            wsdf w
            on ji.work_schedule_code = w.work_schedule_code
            and w.time_date between ji.job_start_date and ji.job_end_date
        join
            {{ ref("time_type_profile_v1") }} ttp
            on ji.time_type_profile_code = ttp.time_type_profile_code
            and w.time_date
            between ttp.time_type_profile_start_date and ttp.time_type_profile_end_date
            and ttp.is_using_timeoff_flag = 'Yes'
        left join
            {{ ref("cost_center_v1") }} cc
            on ji.cost_center_code = cc.cost_center_code
            and w.time_date between cost_center_start_date and cost_center_end_date
        left join
            {{ ref("business_unit_v1") }} bu
            on cc.business_unit_code = bu.business_unit_code
            and w.time_date between business_unit_start_date and business_unit_end_date
        left join
            {{ ref("job_role_v1") }} jr
            on ji.job_code = jr.job_role_code
            and w.time_date between job_role_start_date and job_role_end_date
        left join
            {{ ref("specialization_v1") }} sp
            on jr.specialization_code = sp.specialization_code
            and w.time_date
            between specialization_start_date and specialization_end_date
        where
            not exists (
                select 1
                from {{ ref("holiday_assignment_v1") }} ha
                where
                    ji.holiday_calendar_code = ha.holiday_calendar_code
                    and w.time_date = ha.date_of_holiday
            )
    ),
    final as (
        select
            p.*,
            hc.headcount_type_code,
            dm.date_month_end_date as headcount_date,
            to_char(headcount_date, 'YYYYMMDD')::int as headcount_date_sk,
            dm.date_month_code as headcount_month_sk,
            iff(
                employee_group_code = 'EG0008',  -- Expatriate (host)
                nvl(emp.last_group_seniority, emph.last_group_seniority),
                emp.last_group_seniority
            ) group_seniority_date,
            round(
                months_between(dm.date_month_end_date, p.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(dm.date_month_end_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(dm.date_month_end_date, bi.date_of_birth), 3
            ) age_seniority_months,
            null user_id_hr
        from presence p
        join
            {{ ref("headcount_v1") }} hc
            on p.user_id = hc.user_id
            and p.job_start_date = hc.job_start_date
            and headcount_present_flag = 1
        join
            {{ ref("date_month_referential") }} dm
            on p.time_date between date_month_start_date and date_month_end_date
        left join
            employee_details emp
            on p.user_id = emp.user_id
            and p.time_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            {{ ref("biographical_information_v1") }} bi
            on emp.personal_id = bi.personal_id
        left join
            employee_details emph
            on p.home_user_id = emph.user_id
            and p.time_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
    -- left join
    -- hr on p.user_id = hr.user_id and dm.date_month_end_date = hr.headcount_date
    )
select
    f.user_id,
    f.personal_id,
    headcount_date,
    headcount_date_sk,
    headcount_month_sk,
    dayofweek(f.time_date) as weekday_sk,
    nvl(emp.employee_sk, -1) as employee_sk,
    nvl(org.organization_sk, -1) as organization_sk,
    nvl(job.job_sk, -1) as job_sk,
    nvl(pos.position_sk, -1) as position_sk,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    headcount_type_code,
    is_using_remote_flag,
    nvl(jsr.range_sk, -1) as job_seniority_range_sk,
    nvl(gsr.range_sk, -1) as group_seniority_range_sk,
    nvl(asr.range_sk, -1) as age_seniority_range_sk,
    sum(f.fte) as working_days
from final f
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_code = org.cost_center_code
    and f.time_date between organization_start_date and organization_end_date
    and org.is_ec_live_flag = true
left join
    {{ ref("dim_employee_v1") }} emp
    on f.user_id = emp.user_id
    and f.time_date between emp.employee_start_date and emp.employee_end_date
left join
    {{ ref("dim_job") }} job
    on f.user_id = job.user_id
    and f.job_start_date = job.job_start_date
left join
    {{ ref("dim_position_v1") }} pos
    on f.position_code = pos.position_code
    and f.time_date between position_start_date and position_end_date
left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on f.user_id = emgr.user_id
    and headcount_month_sk = emgr.month_sk
    and manager_level = 1
left join
    {{ ref("dim_employee_v1") }} hr
    on f.user_id_hr = hr.user_id
    and f.time_date between hr.employee_start_date and hr.employee_end_date
left join
    {{ ref("dim_range_v1") }} jsr
    on jsr.range_type = 'JOBSEN'
    and job_seniority_months >= range_start
    and job_seniority_months < range_end
left join
    {{ ref("dim_range_v1") }} gsr
    on gsr.range_type = 'GRPSEN'
    and group_seniority_months >= gsr.range_start
    and group_seniority_months < gsr.range_end
left join
    {{ ref("dim_range_v1") }} asr
    on asr.range_type = 'AGESEN'
    and age_seniority_months / 12 >= asr.range_start
    and age_seniority_months / 12 < asr.range_end
group by all
