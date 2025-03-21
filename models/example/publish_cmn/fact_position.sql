{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change='sync_all_columns',
        cluster_by=['position_code'],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_POSITION');"
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
        from {{ ref("job_information_v1") }} ji
        left join
            (select distinct user_id, personal_id from employee_details) using (user_id)
        qualify
            row_number() over (
                partition by ji.user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    job_info_calc as (
        select
            *,
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
    pos1 as (
        select
            decode(
                true,
                position_to_be_recruited_flag = true
                and position_change_reason_code in ('requestrecruit', 'directhire'),
                'Open',
                position_to_be_recruited_flag = false
                and position_change_reason_code in ('positionfilled', 'updatePosition'),
                'Filled',
                null
            ) state,
            lag(state, 1, 'Filled') ignore nulls over (
                partition by position_code order by position_start_date
            ) p_state,
            nvl(state, p_state) c_state,
            iff(
                c_state = 'Open' and p_state = 'Filled', position_start_date, null
            ) open_date,
            iff(
                c_state = 'Filled' and p_state = 'Open', position_start_date, null
            ) filled_date,
            p.*
        from {{ ref("position_v1") }} p
        where position_status = 'A' and position_start_date <= current_date()  -- and position_code in ('0000002989a', '0000001402')  -- 0000019662
    ),
    pos2 as (
        select
            decode(
                true,
                p_state = 'Open',
                lag(open_date, 1, open_date) ignore nulls over (
                    partition by position_code order by position_start_date
                ),
                c_state = 'Open',
                open_date,
                null
            ) opened_date,  -- When Position went to Open State
            p.* exclude(state, c_state, p_state, open_date)
        from pos1 p
    ),
    pos as (
        select
            iff(opened_date = position_start_date, true, false) open_position_flag,
            iff(
                filled_date = position_start_date,
                datediff('d', opened_date, filled_date) + 1,
                null
            ) days_to_recruit,
            p.* exclude(opened_date, filled_date),
            j1.user_id,
            j1.job_start_date,
            j2.user_id user_id_hr
        from pos2 p
        left join  -- For User tagged to this position
            ji_pos j1
            on p.position_code = j1.position_code
            and p.position_start_date between j1.job_start_date and j1.job_end_date
        left join  -- For HR Manager Position
            {{ ref("position_relationship_v1") }} pl
            on p.position_code = pl.position_code
            and p.position_start_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            ji_pos j2
            on pl.related_position = j2.position_code
            and p.position_start_date between j2.job_start_date and j2.job_end_date
        qualify
            row_number() over (
                partition by p.position_code, position_start_date
                order by j1.job_start_date desc, j2.job_start_date desc
            )
            = 1
    ),
    final as (
        select
            pos.*,
            ji.personal_id,
            ji.job_entry_date,
            ji.contract_end_date,
            iff(
                employee_group_code = 'EG0008',  -- Expatriate (host)
                nvl(emp.last_group_seniority, emph.last_group_seniority),
                emp.last_group_seniority
            ) group_seniority_date,
            bi.date_of_birth,
            round(
                months_between(position_start_date, ji.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(position_start_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(position_start_date, bi.date_of_birth), 3
            ) age_seniority_months
        from pos
        left join
            job_info_calc ji
            on pos.user_id = ji.user_id
            and pos.job_start_date = ji.job_start_date
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            employee_details emp
            on pos.user_id = emp.user_id
            and pos.position_start_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            employee_details emph
            on ji.home_user_id = emph.user_id
            and pos.position_start_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
    )  -- select * from final
select
    f.position_code,
    f.position_start_date,
    f.position_end_date,
    nvl(emp.employee_sk, -1) as employee_sk,
    f.user_id,
    f.personal_id,
    to_char(f.position_start_date, 'YYYYMM')::integer as position_month_sk,
    nvl(job.job_sk, -1) as job_sk,
    nvl(org.organization_sk, -1) as organization_sk,
    nvl(org.business_unit_code, '') as business_unit_code,
    nvl(pos.position_sk, -1) as position_sk,
    nvl(pos.specialization_code, '') as specialization_code,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    nvl(hr.employee_sk, -1) as employee_hr_sk,
    f.user_id_hr,
    f.open_position_flag,
    f.days_to_recruit,
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
    jsr.range_name as job_seniority_range_name,
    gsr.range_name as group_seniority_range_name,
    asr.range_name as age_seniority_range_name,
    iff(
        group_seniority_range_name ilike '< 6 Months',
        'Newcomers only',
        'Without Newcomer'
    ) as newcomers
from final f
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_code = org.cost_center_code
    and f.position_start_date between organization_start_date and organization_end_date
    and org.is_ec_live_flag = true  -- and f.position_code=''
left join
    {{ ref("dim_employee_v1") }} emp
    on f.user_id = emp.user_id
    and f.position_start_date between emp.employee_start_date and emp.employee_end_date
left join
    {{ ref("dim_job") }} job
    on f.user_id = job.user_id
    and f.job_start_date = job.job_start_date
left join
    {{ ref("dim_position_v1") }} pos
    on f.position_code = pos.position_code
    and f.position_start_date = pos.position_start_date
left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on f.user_id = emgr.user_id
    and position_month_sk = emgr.month_sk
    and manager_level = 1
left join
    {{ ref("dim_employee_v1") }} hr
    on f.user_id_hr = hr.user_id
    and f.position_start_date between hr.employee_start_date and hr.employee_end_date
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
