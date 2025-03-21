{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    fopaycomponent as (
        select
            iff(
                enddate in ('9999-12-31'),
                lead(startdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by startdate
                ),
                enddate
            ) as c_enddate,
            *
        from {{ ref("stg_fo_pay_component_flatten") }}
        where dbt_valid_to is null

    )
select
    hash(externalcode, startdate) as pay_component_id,
    externalcode as pay_component_code,
    startdate as pay_component_start_date,
    c_enddate as pay_component_end_date,
    name as pay_component_name,
    frequencycode as frequency_code,
    currency as currency_code,
    cur.currency_id,
    status as pay_component_status
from fopaycomponent payco
left outer join
    {{ ref("currency") }} cur
    on cur.currency_code = payco.currency
    and payco.startdate <= cur.currency_end_date
    and payco.c_enddate >= cur.currency_start_date
qualify
    row_number() over (
        partition by externalcode, startdate
        order by
            cur.currency_start_date  desc
    )
    = 1