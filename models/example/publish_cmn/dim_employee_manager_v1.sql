{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    mnth_end as (  -- Creating Month End Values and for Current Month keeping Current Date
        select
            dateadd(month, seq4(), '2020-01-31') as dt,
            iff(dt < current_date(), dt, current_date()) as month_end,
            to_char(month_end, 'YYYYMM')::int month_sk
        from table(generator(rowcount => 10000))
        where dt <= last_day(current_date())
    ),
    emp_mnth as (  -- This is needed for Employees terminated in any day other than month end
        select month_sk, employee_sk, user_id, employee_name
        from {{ ref("dim_employee_v1") }} emp
        join
            mnth_end m
            on m.month_end between employee_start_date and last_day(employee_end_date)  -- For Termination Month
        qualify
            row_number() over (  -- Taking only one record per month per user
                partition by month_sk, user_id order by employee_end_date desc
            )
            = 1
    ),
    job_info as (
        select
            * exclude(position_code),
            nvl(
                position_code,
                lag(position_code) ignore nulls over (
                    partition by user_id order by job_start_date
                )
            ) position_code
        from
            (
                select *
                from {{ ref("job_information_v1") }}  -- where user_id = '00542480'
                qualify
                    row_number() over (
                        partition by user_id, job_start_date
                        order by sequence_number desc
                    )
                    = 1
            )
    ),
    direct_mgr as (
        select
            m.*,
            pos.position_code,
            higher_position_code direct_manager_code,
            nvl(mgr.user_id, 'Missing Manager') direct_manager_id
        from mnth_end m
        join
            {{ ref("position_v1") }} pos
            on m.month_end between pos.position_start_date and pos.position_end_date
            and higher_position_code is not null
        left join  -- User is tagged to Manager Position
            job_info mgr
            on pos.higher_position_code = mgr.position_code
            and m.month_end between mgr.job_start_date and mgr.job_end_date
        qualify
            row_number() over (  -- Multiple users tagged to same Position for one set
                partition by pos.position_code, m.month_end
                order by mgr.job_start_date desc, mgr.job_end_date desc
            )
            = 1
    ),
    all_mgr as (  -- Looping the data to get nth hierarchy
        select
            *,
            1 manager_level,
            direct_manager_id as manager_id,
            direct_manager_code
            || '('
            || direct_manager_id
            || ')' position_manager_hrchy
        from direct_mgr
        union all
        select
            dm.*,
            am.manager_level + 1 manager_level,
            am.manager_id,
            dm.direct_manager_code
            || '('
            || dm.direct_manager_id
            || '),'
            || am.position_manager_hrchy position_manager_hrchy
        from all_mgr am
        join
            direct_mgr dm
            on am.position_code = dm.direct_manager_code
            and am.month_sk = dm.month_sk
    )
select
    hash(a.month_sk, usr.user_id) as month_employee_sk,
    a.month_sk,
    usr.user_id,
    a.direct_manager_id,
    a.manager_level,
    a.manager_id,
    a.position_code || '=>' || a.position_manager_hrchy as position_manager_hrchy,
    edm.employee_sk direct_manager_employee_sk,
    edm.employee_name direct_manager_name,
    em.employee_sk manager_employee_sk,
    em.employee_name manager_name
from all_mgr a
join  -- User is tagged to Position
    job_info usr
    on a.position_code = usr.position_code
    and a.month_end between usr.job_start_date and usr.job_end_date
left join emp_mnth edm on direct_manager_id = edm.user_id and a.month_sk = edm.month_sk
left join emp_mnth em on manager_id = em.user_id and a.month_sk = em.month_sk
/* where a.month_sk = 202403 and usr.user_id in ('00564121', '00838959', '10001294', '00842021') and manager_level <= 2/**/
union all
select -1 month_employee_sk, null, null, null, 1, null, null, -1, null, -1, null
