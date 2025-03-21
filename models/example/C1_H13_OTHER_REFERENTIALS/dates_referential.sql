{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    dates as (
        select
            current_date() cdt,
            dateadd(day, seq4(), '1970-01-01')::date as sdt,
            iff(cdt < last_day(sdt), cdt, last_day(sdt)) as month_end_date,
            iff(cdt < last_day(sdt), cdt, last_day(sdt)) as year_end_date,
        from table(generator(rowcount => 100000))
        where sdt <= cdt
    )
select
    to_char(sdt, 'YYYYMMDD')::integer as date_id,
    to_char(sdt, 'YYYY') as date_year,
    to_char(sdt, 'MM') as date_month,
    to_char(sdt, 'DD') as date_day,
    to_char(sdt, 'YYYYMM') as date_yyyymm,
    sdt as date_yyyy_mm_dd,
    sdt::timestamp as date_yyyy_mm_dd_datetime,
    to_char(sdt, 'MMMM') as date_month_name_en,
    decode(
        extract(dayofweek from sdt),
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
    ) as date_day_name_en,
    trunc(sdt, 'month') as date_month_start_date,
    iff(
        to_char(current_date, 'YYYYMM') = to_char(sdt, 'YYYYMM'),
        current_date,
        last_day(sdt)
    ) as date_month_end_date,
    trunc(sdt, 'year') as date_year_start_date,
    iff(
        year(current_date) = year(sdt),
        current_date,
        trunc(dateadd(year, 1, sdt), 'year') - 1
    ) as date_year_end_date
from dates
