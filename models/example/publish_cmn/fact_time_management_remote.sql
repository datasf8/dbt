{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        cluster_by=["user_id", "time_type_code", "headcount_type_code"],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_TIME_MANAGEMENT_REMOTE');"
    )
}}
with
    employee_time_daily as (
        select
            et.*,
            tt.* exclude (time_type_code),
            nvl(lc.category, 'Not Categorized') category
        from {{ ref("employee_time_daily_v1") }} et
        left join {{ ref("time_type_v1") }} tt using (time_type_code)
        left join
            {{ ref("dim_leave_category_vw") }} lc on tt.time_type_name = lc.time_type
        where
            time_date <= current_date
            and nvl(et.time_type_code,'') != 'USA_Alternate_Work_Loc'
            and (
                contains(et.time_type_code, 'REMOTE')
                or contains(et.time_type_code, 'HOME')
                or contains(et.time_type_code, 'FLEX_WORK')
                or contains(et.time_type_code, 'SMART')
            )
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
        from {{ ref("job_information_v1") }}
        left join
            (select distinct user_id, personal_id from employee_details) using (user_id)
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    job_info_calc as (
        select
            *,
            lag(position_code) ignore nulls over (
                partition by user_id order by job_start_date
            ) last_position_code,
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
            etd.user_id,
            etd.time_date,
            j1.user_id ji_user_id,
            j1.position_code,
            pl.related_position,
            j2.user_id user_id_hr
        from employee_time_daily etd
        join
            job_info_calc j1
            on etd.user_id = j1.user_id
            and etd.time_date between j1.job_start_date and j1.job_end_date
        left join
            {{ ref("position_relationship_v1") }} pl
            on j1.position_code = pl.position_code
            and etd.time_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            ji_pos j2
            on pl.related_position = j2.position_code
            and etd.time_date between j2.job_start_date and j2.job_end_date
        qualify
            row_number() over (
                partition by etd.user_id, etd.time_date order by j2.job_start_date desc
            )
            = 1
    ),
    final as (
        select
            etd.user_id,
            etd.time_type_code,
            etd.time_date,
            etd.time_weekday_number,
            etd.time_weekday_name,
            etd.time_quantity_in_day,
            etd.category,
            etd.time_type_name,
            etd.custom_group_type,
            etd.custom_global_type,
            hr.user_id_hr,
            hc.headcount_type_code,
            nvl(jic.last_position_code, jic.position_code) as position_code,
            jic.cost_center_code,
            jic.job_start_date,
            jic.job_entry_date,
            case
                when
                    bu.business_unit_type_code in ('3', '03', '15')
                    or sp.professional_field_code = 'PF000024'
                then 'No'
                else ttp.is_using_remote_flag
            end is_using_remote_flag,
            iff(
                jic.employee_group_code = 'EG0008',  -- Expatriate (host)
                nvl(emp.last_group_seniority, emph.last_group_seniority),
                emp.last_group_seniority
            ) group_seniority_date,
            bi.date_of_birth,
            round(
                months_between(etd.time_date, jic.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(etd.time_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(etd.time_date, bi.date_of_birth), 3
            ) age_seniority_months
        from employee_time_daily etd
        join
            {{ ref("headcount_v1") }} hc
            on etd.user_id = hc.user_id
            and etd.time_date between hc.job_start_date and hc.job_end_date
            and hc.headcount_present_flag = 1
        join
            job_info_calc jic
            on etd.user_id = jic.user_id
            and etd.time_date between jic.job_start_date and jic.job_end_date
        join
            {{ ref("time_type_profile_v1") }} ttp
            on jic.time_type_profile_code = ttp.time_type_profile_code
            and etd.time_date
            between ttp.time_type_profile_start_date and ttp.time_type_profile_end_date
            and ttp.is_using_timeoff_flag = 'Yes'
        left join
            {{ ref("biographical_information_v1") }} bi
            on jic.personal_id = bi.personal_id
        left join
            employee_details emp
            on etd.user_id = emp.user_id
            and etd.time_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            employee_details emph
            on jic.home_user_id = emph.user_id
            and etd.time_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
        left join hr on etd.user_id = hr.user_id and etd.time_date = hr.time_date
        left join
            {{ ref("cost_center_v1") }} cc
            on jic.cost_center_code = cc.cost_center_code
            and etd.time_date between cost_center_start_date and cost_center_end_date
        left join
            {{ ref("business_unit_v1") }} bu
            on cc.business_unit_code = bu.business_unit_code
            and etd.time_date
            between business_unit_start_date and business_unit_end_date
        left join
            {{ ref("job_role_v1") }} jr
            on jic.job_code = jr.job_role_code
            and etd.time_date between job_role_start_date and job_role_end_date
        left join
            {{ ref("specialization_v1") }} sp
            on jr.specialization_code = sp.specialization_code
            and etd.time_date
            between specialization_start_date and specialization_end_date
    )
select
    f.user_id,
    f.time_type_code,
    f.time_date,
    to_char(f.time_date, 'YYYYMMDD')::integer as time_date_sk,
    to_char(f.time_date, 'YYYYMM')::integer as time_month_sk,
    f.time_weekday_number as weekday_sk,
    nvl(emp.employee_sk, -1) as employee_sk,
    o.organization_sk,
    nvl(j.job_sk, -1) as job_sk,
    nvl(p.position_sk, -1) as position_sk,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    nvl(hr.employee_sk, -1) as employee_hr_sk,
    f.user_id_hr,
    f.time_weekday_name,
    f.time_quantity_in_day,
    f.category,
    f.headcount_type_code,
    f.is_using_remote_flag,
    f.job_start_date,
    f.job_entry_date,
    f.group_seniority_date,
    f.date_of_birth,
    f.job_seniority_months,
    f.group_seniority_months,
    f.age_seniority_months,
    f.time_type_name as time_type,
    f.custom_group_type as time_group_type,
    f.custom_global_type as time_global_type,
    nvl(jsr.range_sk, -1) as job_seniority_range_sk,
    nvl(gsr.range_sk, -1) as group_seniority_range_sk,
    nvl(asr.range_sk, -1) as age_seniority_range_sk,
    jsr.range_name as job_seniority_range_name,
    gsr.range_name as group_seniority_range_name,
    asr.range_name as age_seniority_range_name,
    iff(
        group_seniority_range_name like '%< 6 Months%',
        'Newcomers only',
        'Without Newcomer'
    ) as newcomers
from final f
left join
    {{ ref("dim_employee_v1") }} emp
    on f.user_id = emp.user_id
    and f.time_date between emp.employee_start_date and emp.employee_end_date
join
    {{ ref("dim_organization_v1") }} o
    on f.cost_center_code = o.cost_center_code
    and f.time_date between organization_start_date and organization_end_date
    and is_ec_live_flag = true
left outer join
    {{ ref("dim_job") }} j
    on f.user_id = j.user_id
    and f.time_date between j.job_start_date and j.job_end_date
left outer join
    {{ ref("dim_position_v1") }} p
    on f.position_code = p.position_code
    and f.time_date between p.position_start_date and p.position_end_date
left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on f.user_id = emgr.user_id
    and time_month_sk = emgr.month_sk
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
where is_using_remote_flag = 'Yes'
