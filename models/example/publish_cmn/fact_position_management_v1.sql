{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change='sync_all_columns',
        cluster_by=['position_code'],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_POSITION_MANAGEMENT_V1');"
    )
}}
with
    mnth_end as (
        select
            month_sk,
            end_date as month_end_date,
            start_date as month_start_date,
            month(month_end_date) month,
            year(month_end_date) year,
            iff(
                month_end_date < current_date(), month_end_date, current_date()
            ) as position_date
        from {{ ref("dim_date_by_month_vw") }}
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
        -- where ji.ec_employee_status <> 'Terminated'
        where ji.employee_status_id in (32245, 32238, 32244, 32241, 32236, 32243)
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
    pos as (
        select
            p.*,
            me.month_sk,
            me.position_date,
            jp.user_id,
            jp.job_start_date,
            hr.user_id user_id_hr,
            pl.related_position
        from {{ ref("position_v1") }} p
        join
            mnth_end me
            on me.position_date between p.position_start_date and p.position_end_date
            and p.position_status = 'A'
        left join
            ji_pos jp
            on p.position_code = jp.position_code
            and me.position_date between jp.job_start_date and jp.job_end_date
        left join
            {{ ref("position_relationship_v1") }} pl
            on p.position_code = pl.position_code
            and position_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            ji_pos hr
            on pl.related_position = hr.position_code
            and position_date between hr.job_start_date and hr.job_end_date
        qualify
            row_number() over (
                partition by p.position_code, me.position_date
                order by jp.job_start_date desc, hr.job_start_date desc
            )
            = 1
    ),
    pos_with_2_month_back as (
        select
            p.*,
            -- iff(p.user_id is null and p1.user_id is null and
            -- p.position_to_be_recruited_flag =false
            -- and p.position_change_reason_code not in ('requestrecruit',
            -- 'directhire') and
            -- p1.position_to_be_recruited_flag = false and
            -- p1.position_change_reason_code not in ('requestrecruit', 'directhire'),
            -- 'Yes' , 'No') as is_vacant_1,
            case
                when
                    p.position_to_be_recruited_flag = true
                    and p.position_change_reason_code
                    in ('requestrecruit', 'directhire')
                then 'No'
                when
                    p1.position_to_be_recruited_flag = true
                    and p1.position_change_reason_code
                    in ('requestrecruit', 'directhire')
                then 'No'
                when
                    p2.position_to_be_recruited_flag = true
                    and p2.position_change_reason_code
                    in ('requestrecruit', 'directhire')
                then 'No'
                when
                    p.user_id is not null
                    or p1.user_id is not null
                    or p2.user_id is not null
                then 'No'
                else 'Yes'
            end as is_vacant
        from pos p
        left join
            pos p1
            on p.position_code = p1.position_code
            and add_months(p.position_date, -2) = p1.position_date
        left join
            pos p2
            on p.position_code = p2.position_code
            and add_months(p.position_date, -1) = p2.position_date
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
                months_between(position_date, ji.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(position_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(position_date, bi.date_of_birth), 3
            ) age_seniority_months
        from pos_with_2_month_back pos
        left join
            job_info_calc ji
            on pos.user_id = ji.user_id
            and pos.job_start_date = ji.job_start_date
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            employee_details emp
            on pos.user_id = emp.user_id
            and pos.position_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            employee_details emph
            on ji.home_user_id = emph.user_id
            and pos.position_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
    )  -- select * from final
select
    f.position_code,
    f.position_name_en as position_title,
    nvl(emp.employee_sk, -1) as employee_sk,
    f.user_id,
    f.personal_id,
    f.month_sk as position_month_sk,
    nvl(job.job_sk, -1) as job_sk,
    nvl(org.organization_sk, -1) as organization_sk,
    nvl(org.business_unit_code, '') as business_unit_code,
    nvl(pos.position_sk, -1) as position_sk,
    nvl(pos.specialization_code, '') as specialization_code,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    nvl(hr.employee_sk, -1) as employee_hr_sk,
    f.related_position as hr_manager_position_code,
    pos_hr.position_sk as hr_manager_position_sk,
    f.user_id_hr,
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
    ) as newcomers,
    case
        when
            exists (
                select 1
                from {{ ref("position_right_to_return_v1") }} prtr
                where
                    position_right_to_return_status = 'A'
                    and f.position_code = prtr.position_code
                    and f.position_start_date = prtr.position_right_to_return_start_date
            )
        then 'Yes'
        else 'No'
    end as has_right_to_return,
    f.is_vacant
from final f
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_code = org.cost_center_code
    and f.position_date between organization_start_date and organization_end_date
    and org.is_ec_live_flag = true  -- and f.position_code=''
left join
    {{ ref("dim_employee_v1") }} emp
    on f.user_id = emp.user_id
    and f.position_date between emp.employee_start_date and emp.employee_end_date
left join
    {{ ref("dim_job") }} job
    on f.user_id = job.user_id
    and f.job_start_date = job.job_start_date
left join
    {{ ref("dim_position_v1") }} pos
    on f.position_code = pos.position_code
    and f.position_start_date = pos.position_start_date
left join
    {{ ref("dim_position_v1") }} pos_hr
    on f.related_position = pos_hr.position_code
    and f.position_date between pos_hr.position_start_date and pos_hr.position_end_date

left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on f.user_id = emgr.user_id
    and position_month_sk = emgr.month_sk
    and manager_level = 1
left join
    {{ ref("dim_employee_v1") }} hr
    on f.user_id_hr = hr.user_id
    and f.position_date between hr.employee_start_date and hr.employee_end_date
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
