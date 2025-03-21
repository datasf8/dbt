{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    foeventreason as (
        select
            *,
            iff(
                i.enddate in ('9999-12-31'),
                lead(i.startdate - 1, 1, {d '9999-12-31'}) over (
                    partition by i.externalcode order by i.startdate
                ),
                i.enddate
            ) as c_enddate
        from {{ ref("stg_fo_event_reason_flatten") }} i
        where i.dbt_valid_to is null
    )
select
    hash(externalcode, startdate) as event_reasons_id,
    externalcode as event_reasons_code,
    startdate as event_reasons_start_date,
    c_enddate as event_reasons_end_date,
    name as event_reasons_name,
    event as events_id,
    status as event_reasons_status
from foeventreason
