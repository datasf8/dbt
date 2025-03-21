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
        select dateadd(day, seq4(), current_date)::date as dt
        from table(generator(rowcount => 7))

    )
select dayofweek(dt) weekday_sk, dayname(dt) weekday_name
from date_month
