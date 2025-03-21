{{
    config(
        materialized="incremental",
        unique_key="user_id",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','HEADCOUNT_MONTHLY');"
    )
}}
with
    mnth_end as (
        select
            dateadd(month, seq4(), '2020-01-31') as month_end_date,
            month(month_end_date) month,
            year(month_end_date) year
        from table(generator(rowcount => 10000))
        where month_end_date <= last_day(current_date())
    ),
    jobinfo as (
        select *
        from {{ ref("job_information_v1") }}
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    termjob as (
        select user_id, job_start_date
        from jobinfo j
        join {{ ref("termination_event_reasons_v1") }} t using (event_reasons_code)
        where termination_event_reasons_type_code = 'TER'
    )
select h.user_id, year, month, headcount_type_code, headcount_present_flag
from {{ ref("headcount_v1") }} h
join mnth_end m on m.month_end_date between h.job_start_date and h.job_end_date
left join termjob tj using (user_id, job_start_date)
where (tj.user_id is null or last_day(h.job_start_date, 'MON') = m.month_end_date)
order by 4, 2, 3, 1
