{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    job_info as (
        select
            user_id as ji_user_id,
            job_end_date as ji_job_end_date,
            job_start_date as ji_job_start_date,
            event_reasons_code as ji_event_reasons_code
        from {{ ref("job_information_v1") }}
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    reason as (select * from {{ ref("hiring_event_reasons_v1") }}),
    jn_reason as (
        select *
        from reason
        inner join
            job_info on reason.event_reasons_code = job_info.ji_event_reasons_code
    ),
    jn_hc as (
        select *
        from jn_reason jr
        left outer join
            {{ ref("headcount_v1") }} hc
            on jr.ji_user_id = hc.user_id
            and jr.ji_job_start_date = hc.job_start_date
            and hc.headcount_present_flag = 1
    ),
    mnth_end as (
        select
            dateadd(month, seq4(), '2023-01-31') as month_end_date,
            year(month_end_date) as year,
            month(month_end_date) as month
        from table(generator(rowcount => 10000))
        where month_end_date <= last_day(current_date())
    ),  -- select * from mnth_end;
    m as (
        select
            'M' as consolidation_type,
            me.year,
            me.month,
            ht.headcount_type_code,
            nvl(sum(headcount_present_flag), 0) hirings_count
        from mnth_end me
        join {{ ref("headcount_type_v1") }} ht on 1 = 1
        left join
            jn_hc
            on me.year = year(ji_job_start_date)
            and me.month = month(ji_job_start_date)
            and ht.headcount_type_code = jn_hc.headcount_type_code
        group by 1, 2, 3, 4
    ),
    c as (
        select
            'C' as consolidation_type,
            me.year,
            me.month,
            headcount_type_code,
            sum(hirings_count) hirings_count
        from mnth_end me
        join m on me.year = m.year and me.month >= m.month
        group by 1, 2, 3, 4
    )
select *
from m
union all
select *
from c
