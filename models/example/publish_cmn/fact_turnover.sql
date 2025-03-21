{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change='sync_all_columns',
        cluster_by=['user_id','headcount_type_code'],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_TURNOVER');"
    )
}}
with
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
    mobility as (
        select
            mob.*,
            (mobility_date - 1) as termination_date,
            erc.event_reasons_category as termination_type
        from {{ ref("job_mobility_v1") }} mob
        join
            {{ ref("event_reasons_categories_v1") }} erc
            on mob.event_reasons_code = erc.event_reasons_code
            and erc.is_group_flag = true
            and mob.mobility_type = 'TERMINATION'
            and mob.mobility_date <= current_date()
    ),  -- select * from mobility
    ji_pos as (
        select *
        from job_info
        qualify
            row_number() over (
                partition by position_code, job_start_date order by job_end_date desc
            )
            = 1
    ),
    mgr as (
        select
            mob.user_id,
            j1.user_id ji_user_id,
            j1.position_code,
            p1.higher_position_code,
            j2.user_id user_id_manager
        from mobility mob
        join
            job_info j1
            on mob.user_id = j1.user_id
            and mob.termination_date between j1.job_start_date and j1.job_end_date
        left join
            {{ ref("position_v1") }} p1
            on j1.position_code = p1.position_code
            and mob.termination_date
            between p1.position_start_date and p1.position_end_date
        left join
            ji_pos j2
            on p1.higher_position_code = j2.position_code
            and mob.termination_date between j2.job_start_date and j2.job_end_date
        qualify
            row_number() over (partition by mob.user_id order by j2.job_start_date desc)
            = 1
    ),
    hr as (
        select
            mob.user_id,
            j1.user_id ji_user_id,
            j1.position_code,
            pl.related_position,
            j2.user_id user_id_hr
        from mobility mob
        join
            job_info j1
            on mob.user_id = j1.user_id
            and mob.termination_date between j1.job_start_date and j1.job_end_date
        left join
            {{ ref("position_relationship_v1") }} pl
            on j1.position_code = pl.position_code
            and mob.termination_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            ji_pos j2
            on pl.related_position = j2.position_code
            and mob.termination_date between j2.job_start_date and j2.job_end_date
        qualify
            row_number() over (partition by mob.user_id order by j2.job_start_date desc)
            = 1
    ),
    final as (
        select
            mob.user_id,
            mob.termination_date,
            emp.personal_id,
            mgr.user_id_manager,
            hr.user_id_hr,
            mob.mobility_type,
            mob.event_reasons_code,
            mob.headcount_line_id,
            mob.previous_headcount_line_id,
            hc.headcount_type_code,
            hc.headcount_present_flag,
            mob.termination_type,
            er.event_reasons_name as termination_reason,
            et.resignation_reason_code,
            iff(
                er.event_reasons_code = 'TERRESIGN',
                nvl(rr.resignation_reason_name, 'Not Mapped')
                || ' ('
                || et.resignation_reason_code
                || ')',
                'N/A'
            ) as termination_sub_reason,
            et.ok_to_rehire_flag,
            ji.job_start_date,
            nvl(ji.position_code, ji.last_position_code) position_code,
            ji.cost_center_code,
            ji.employee_subgroup_code,
            ji.job_entry_date,
            ji.flsa_status_id,
            iff(
                employee_group_code = 'EG0008',  -- Expatriate (host)
                nvl(emp.last_group_seniority, emph.last_group_seniority),
                emp.last_group_seniority
            ) group_seniority_date,
            bi.date_of_birth,
            round(
                months_between(mob.termination_date, ji.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(mob.termination_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(mob.termination_date, bi.date_of_birth), 3
            ) age_seniority_months,
            round(
                months_between(mob.termination_date, group_seniority_date), 3
            ) tenure_months
        from mobility mob
        left join
            {{ ref("headcount_v1") }} hc
            on mob.previous_headcount_line_id = hc.headcount_line_id
            and hc.headcount_present_flag = 1
        left join
            job_info_calc ji
            on mob.user_id = ji.user_id
            and hc.job_start_date = ji.job_start_date
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            employee_details emp
            on mob.user_id = emp.user_id
            and mob.termination_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            employee_details emph
            on ji.home_user_id = emph.user_id
            and mob.termination_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
        left join
            {{ ref("event_reasons_v1") }} er
            on mob.event_reasons_code = er.event_reasons_code
            and mob.termination_date
            between event_reasons_start_date and event_reasons_end_date
        left join
            {{ ref("employment_termination_details_v1") }} et
            on mob.user_id = et.user_id
            and emp.personal_id = et.personal_id
            and mob.termination_date = et.employment_termination_details_end_date
        left join
            {{ ref("resignation_reasons_v1") }} rr
            on et.resignation_reason_code = rr.resignation_reason_code
            and mob.termination_date
            between resignation_reason_start_date and resignation_reason_end_date
        left join mgr on mob.user_id = mgr.user_id
        left join hr on mob.user_id = hr.user_id
    )  -- select * from final
select
    nvl(emp.employee_sk, -1) as employee_sk,
    f.user_id,
    f.personal_id,
    to_char(f.termination_date, 'YYYYMMDD')::integer as termination_date_sk,
    to_char(f.termination_date, 'YYYYMM')::integer as termination_month_sk,
    nvl(job.job_sk, -1) as job_sk,
    nvl(esg.employee_subgroup_sk, -1) as employee_subgroup_sk,
    nvl(org.organization_sk, -1) as organization_sk,
    nvl(pos.position_sk, -1) as position_sk,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    nvl(mgr.employee_sk, -1) as employee_manager_sk,
    f.user_id_manager,
    nvl(hr.employee_sk, -1) as employee_hr_sk,
    nvl(dfs.flsa_status_sk, -1) as flsa_status_sk,
    f.user_id_hr,
    f.termination_date,
    f.mobility_type,
    f.event_reasons_code,
    f.headcount_line_id,
    f.previous_headcount_line_id,
    f.headcount_type_code,
    f.headcount_present_flag,
    f.termination_type,
    f.termination_reason || ' (' || f.event_reasons_code || ')' as termination_reason,
    nvl(f.termination_sub_reason, 'Not defined') as termination_sub_reason,
    f.ok_to_rehire_flag,
    f.job_entry_date,
    f.group_seniority_date,
    f.date_of_birth,
    f.job_seniority_months,
    f.group_seniority_months,
    f.age_seniority_months,
    f.tenure_months,
    jsr.range_sk as job_seniority_range_sk,
    gsr.range_sk as group_seniority_range_sk,
    asr.range_sk as age_seniority_range_sk,
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
    and f.termination_date between emp.employee_start_date and emp.employee_end_date
left join
    {{ ref("dim_job") }} job
    on f.user_id = job.user_id
    and f.job_start_date = job.job_start_date
left join
    {{ ref("dim_employee_subgroup_v1") }} esg
    on f.employee_subgroup_code = esg.employee_subgroup_code
    and f.termination_date
    between esg.employee_subgroup_start_date and esg.employee_subgroup_end_date
left join
    {{ ref("dim_position_v1") }} pos
    on f.position_code = pos.position_code
    and f.termination_date between position_start_date and position_end_date
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_code = org.cost_center_code
    and f.termination_date between organization_start_date and organization_end_date
    and org.is_ec_live_flag = true
left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on f.user_id = emgr.user_id
    and termination_month_sk = emgr.month_sk
    and manager_level = 1
left join
    {{ ref("dim_employee_v1") }} mgr
    on f.user_id_manager = mgr.user_id
    and f.termination_date between mgr.employee_start_date and mgr.employee_end_date
left join
    {{ ref("dim_employee_v1") }} hr
    on f.user_id_hr = hr.user_id
    and f.termination_date between hr.employee_start_date and hr.employee_end_date
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
left join {{ ref("dim_flsa_status") }} dfs on f.flsa_status_id = dfs.flsa_status_sk
