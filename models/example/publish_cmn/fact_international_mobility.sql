{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        cluster_by=['user_id'],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_INTERNATIONAL_MOBILITY');"
    )
}}
with
    mnth_end as (
        select
            dateadd(month, seq4(), '2020-01-31') as month_end_date,
            month(month_end_date) month,
            year(month_end_date) year,
            iff(
                month_end_date < current_date(), month_end_date, current_date()
            ) as mobility_snapshot_date
        from table(generator(rowcount => 10000))
        where month_end_date <= last_day(current_date())
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
        from {{ ref("job_information_v1") }} ji
        left join
            (select distinct user_id, personal_id from employee_details) using (user_id)
        qualify
            row_number() over (
                partition by ji.user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    international_mobility as (
        select
            ji_host.user_id user_id_host,  -- ji.host.personal_id,
            ji_host.job_start_date job_start_date_host,
            ji_host.position_code position_code_host,
            me.mobility_snapshot_date,
            ji_home.user_id user_id_home,
            ji_home.job_start_date job_start_date_home,
            ji_home.position_code position_code_home,
            ji_home.cost_center_code cost_center_code_home
        from job_info ji_host
        join (select 1) on employee_group_code in ('EG0008')
        join
            mnth_end me
            on mobility_snapshot_date
            between ji_host.job_start_date and ji_host.job_end_date
        left join
            job_info ji_home
            on ji_host.personal_id = ji_home.personal_id
            and ji_home.employee_group_code in ('EG0005')
            and ji_home.ec_employee_status = 'Dormant'
            and mobility_snapshot_date
            between ji_home.job_start_date and ji_home.job_end_date
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
            im.user_id_host,
            im.mobility_snapshot_date,  -- ,im.job_start_date_host,im.position_code_host,pl.related_position,
            j2.user_id user_id_hr
        from international_mobility im
        left join
            {{ ref("position_relationship_v1") }} pl
            on im.position_code_host = pl.position_code
            and im.mobility_snapshot_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            ji_pos j2
            on pl.related_position = j2.position_code
            and im.mobility_snapshot_date between j2.job_start_date and j2.job_end_date
        qualify
            row_number() over (
                partition by im.user_id_host, im.mobility_snapshot_date
                order by j2.job_start_date desc
            )
            = 1
    ),
    final as (
        select
            im.*,
            host.*,
            hr.user_id_hr,
            nvl(
                emp.last_group_seniority, emph.last_group_seniority
            ) group_seniority_date,
            bi.date_of_birth,
            round(
                months_between(im.mobility_snapshot_date, host.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(im.mobility_snapshot_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(im.mobility_snapshot_date, bi.date_of_birth), 3
            ) age_seniority_months,
            global_assignment_start_date,
            global_assignment_end_date,
            global_assignment_type_name_en,
            round(
                months_between(im.mobility_snapshot_date, global_assignment_start_date),
                3
            ) global_assignment_seniority_months,
            assignment_package_name_en,
        from international_mobility im
        join
            {{ ref("headcount_v1") }} hc
            on im.user_id_host = hc.user_id
            and im.job_start_date_host = hc.job_start_date
            and hc.headcount_type_code = 'STA'
            and hc.headcount_present_flag = 1
        join
            job_info host
            on im.user_id_host = host.user_id
            and im.job_start_date_host = host.job_start_date
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            job_info home
            on im.user_id_home = home.user_id
            and im.job_start_date_home = home.job_start_date
        left join
            employee_details emp
            on host.user_id = emp.user_id
            and im.mobility_snapshot_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            employee_details emph
            on home.user_id = emph.user_id
            and im.mobility_snapshot_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
        left join hr using (user_id_host, mobility_snapshot_date)
        left join
            {{ ref("employee_global_assignment_v1") }} ega
            on host.personal_id = ega.personal_id
            and host.user_id = ega.user_id
        left join
            {{ ref("global_assignment_type_v1") }} using (global_assignment_type_id)
        left join {{ ref("assignment_package_v1") }} using (assignment_package_id)
    )  -- select * from final
select
    f.personal_id,
    f.user_id,
    f.job_start_date as mobility_start_date,
    f.job_end_date as mobility_end_date,
    to_char(f.mobility_snapshot_date, 'YYYYMM')::integer as mobility_snapshot_month_sk,
    nvl(emp.employee_sk, -1) as employee_sk,
    nvl(job.job_sk, -1) as job_sk,
    nvl(org.organization_sk, -1) as organization_sk,
    nvl(pos.position_sk, -1) as position_sk,
    f.user_id_home,
    nvl(emp_home.employee_sk, -1) as home_employee_sk,
    nvl(job_home.job_sk, -1) as home_job_sk,
    nvl(org_home.organization_sk, -1) as home_organization_sk,
    nvl(pos_home.position_sk, -1) as home_position_sk,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    nvl(hr.employee_sk, -1) as employee_hr_sk,
    f.user_id_hr,
    f.mobility_snapshot_date,
    f.global_assignment_start_date,
    f.global_assignment_end_date,
    f.global_assignment_type_name_en,
    f.assignment_package_name_en,
    f.job_entry_date,
    f.contract_end_date,
    f.group_seniority_date,
    f.date_of_birth,
    f.job_seniority_months,
    f.group_seniority_months,
    f.age_seniority_months,
    nvl(jsr.range_sk, -1) as job_seniority_range_sk,
    nvl(gsr.range_sk, -1) as group_seniority_range_sk,
    nvl(asr.range_sk, -1) as age_seniority_range_sk,
    nvl(gasr.range_sk, -1) as global_assignment_seniority_range_sk,
    jsr.range_name as job_seniority_range_name,
    gsr.range_name as group_seniority_range_name,
    asr.range_name as age_seniority_range_name,
    gasr.range_name as global_assignment_seniority_range_name,
    iff(
        group_seniority_range_name ilike '< 6 Months',
        'Newcomers only',
        'Without Newcomer'
    ) as newcomers
from final f
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_code = org.cost_center_code
    and f.mobility_snapshot_date
    between organization_start_date and organization_end_date
    and org.is_ec_live_flag = true
left join
    {{ ref("dim_employee_v1") }} emp
    on f.user_id = emp.user_id
    and f.mobility_snapshot_date
    between emp.employee_start_date and emp.employee_end_date
left join
    {{ ref("dim_job") }} job
    on f.user_id = job.user_id
    and f.job_start_date = job.job_start_date
left join
    {{ ref("dim_position_v1") }} pos
    on f.position_code = pos.position_code
    and f.mobility_snapshot_date between position_start_date and position_end_date
left join
    {{ ref("dim_organization_v1") }} org_home
    on f.cost_center_code_home = org_home.cost_center_code
    and f.mobility_snapshot_date
    between org_home.organization_start_date and org_home.organization_end_date
left join
    {{ ref("dim_employee_v1") }} emp_home
    on f.user_id_home = emp_home.user_id
    and f.mobility_snapshot_date
    between emp_home.employee_start_date and emp_home.employee_end_date
left join
    {{ ref("dim_job") }} job_home
    on f.user_id_home = job_home.user_id
    and f.job_start_date_home = job_home.job_start_date
left join
    {{ ref("dim_position_v1") }} pos_home
    on f.position_code_home = pos_home.position_code
    and f.mobility_snapshot_date
    between pos_home.position_start_date and pos_home.position_end_date
left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on f.user_id = emgr.user_id
    and mobility_snapshot_month_sk = emgr.month_sk
    and manager_level = 1
left join
    {{ ref("dim_employee_v1") }} hr
    on f.user_id_hr = hr.user_id
    and f.mobility_snapshot_date between hr.employee_start_date and hr.employee_end_date
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
left join
    {{ ref("dim_range_v1") }} gasr
    on gasr.range_type = 'GASSEN'
    and global_assignment_seniority_months / 12 >= gasr.range_start
    and global_assignment_seniority_months / 12 < gasr.range_end
