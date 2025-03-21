{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        cluster_by=["date_sk"],
    )
}}
with
    date_day as (
        select
            dateadd(day, seq4(), '2020-01-01')::date as dt,
            current_date() cdt,
            dateadd(month, -1, cdt) prev_mon_dt
        from table(generator(rowcount => 10000))
        where dt <= dateadd(year, 1, last_day(current_date()))
    )
select
    to_char(dt, 'YYYYMMDD')::integer date_sk,
    dt as date,
    date_part(year, dt) year,
    collate(
        case when year(dt) = year(cdt) then 'Current Year' else to_char(dt, 'YYYY') end,
        'en-ci'
    ) as year_label,
    dayofyear(dt) day_of_year,
    date_part(week, dt) week_of_year,
    date_part(quarter, dt) quarter,
    date_part(month, dt) month,
    dayofmonth(dt) day_of_month,
    collate(to_char(dt, 'MON'), 'en-ci') as short_month,
    collate(to_char(dt, 'MMMM'), 'en-ci') as long_month,
    collate(
        case
            when last_day(dt) = last_day(cdt)
            then 'Current Month'
            else to_char(dt, 'MM')
        end,
        'en-ci'
    ) as month_label,
    collate(
        case when last_day(dt) = last_day(cdt) then 'Current Month' else long_month end,
        'en-ci'
    ) as long_month_label,
    collate(
        case
            when last_day(dt) = last_day(cdt)
            then 'Current Month'
            else to_char(dt, 'YYYY-MM')
        end,
        'en-ci'
    ) as year_month_label,
    -- weekofmonth(dt) week_of_month,
    dayofweek(dt) + 1 day_of_week,
    collate(dayname(dt), 'en-ci') day_name,
    collate(
        decode(
            dayofweek(dt),
            0,
            'Sunday',
            1,
            'Monday',
            2,
            'Tuesday',
            3,
            'Wednesday',
            4,
            'Thursday',
            5,
            'Friday',
            6,
            'Saturday'
        ),
        'en-ci'
    ) day_long_name,
    trunc(dt, 'Y') year_start_date,
    last_day(dt, 'Y') year_end_date,
    trunc(dt, 'Q') quarter_start_date,
    last_day(dt, 'Q') quarter_end_date,
    trunc(dt, 'MM') month_start_date,
    last_day(dt, 'MM') month_end_date,
    trunc(dt, 'W') week_start_date,
    last_day(dt, 'W') week_end_date
from date_day
