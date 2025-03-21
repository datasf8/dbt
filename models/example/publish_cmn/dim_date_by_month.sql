{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    date_month as (
        select dateadd(month, seq4(), '2020-01-01')::date as dt, current_date() cdt
        from table(generator(rowcount => 10000))
        where dt <= cdt
    )
select
    to_char(dt, 'YYYYMM')::integer as month_sk,
    dt as start_date,
    iff(cdt < last_day(dt), cdt, last_day(dt)) end_date,
    collate(to_char(dt, 'YYYY-MM'), 'en-ci') as yyyy_mm,
    date_part(year, dt) year,
    date_part(month, dt) month,
    collate(
        case when end_date = cdt then 'Current Month' else to_char(dt, 'Mon YYYY') end,
        'en-ci'
    ) as year_month_label,
    collate(to_char(dt, 'MON'), 'en-ci') as short_month,
    collate(to_char(dt, 'MMMM'), 'en-ci') as long_month,
    to_char(dt - 1, 'YYYYMM')::integer as previous_month_sk,
    to_char(dateadd('mm', -6, dt), 'YYYYMM')::integer as previous_6_months_sk,
    month_sk - 100 as previous_year_sk,
    (year - 1) * 100 + 12 as previous_year_end_sk
from date_month
