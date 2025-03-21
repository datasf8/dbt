{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
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
            nvl(sum(tp.headcount_present_flag), 0) terminations_count
        from mnth_end me
        join {{ ref("headcount_type_v1") }} ht on 1 = 1
        left join
            {{ ref("termination_periods_v1") }} tp
            on me.year = year(tp.job_start_date)
            and me.month = month(tp.job_start_date)
            and ht.headcount_type_code = tp.headcount_type_code
        group by 1, 2, 3, 4
    ),
    c as (
        select
            'C' as consolidation_type,
            me.year,
            me.month,
            headcount_type_code,
            sum(terminations_count) terminations_count
        from mnth_end me
        join m on me.year = m.year and me.month >= m.month
        group by 1, 2, 3, 4
    )
select *
from m
union all
select *
from c
