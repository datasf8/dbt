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
        select
            current_date() cdt,
            dateadd(month, seq4(), '2020-01-01')::date as sdt,
            iff(cdt < last_day(sdt), cdt, last_day(sdt)) edt
        from table(generator(rowcount => 10000))
        where sdt <= cdt
    )
select
    to_char(edt, 'YYYYMM')::integer as date_month_code,
    sdt as date_month_start_date,
    edt as date_month_end_date,
    collate(to_char(edt, 'YYYY-MM'), 'en-ci') as date_month_code_2,
    date_part(year, edt) date_month_year,
    collate(
        case when edt = cdt then 'Current Month' else to_char(edt, 'Mon YYYY') end,
        'en-ci'
    ) as date_month_label,
    date_part(month, edt) date_month,
    collate(to_char(edt, 'MON'), 'en-ci') as month_short_label,
    collate(to_char(edt, 'MMMM'), 'en-ci') as month_long_label,
    to_char(sdt - 1, 'YYYYMM')::integer as previous_date_month_code,
    to_char(dateadd('mm', -6, sdt), 'YYYYMM')::integer as previous_6_date_month_code,
    date_month_code - 100 as previous_year_date_month_code
from date_month
