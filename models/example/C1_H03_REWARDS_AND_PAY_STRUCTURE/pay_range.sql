{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}


with
    fo_pay as (
        select
            externalcode as pay_range_code,
            startdate as pay_range_start_date,
            iff(
                enddate in ('9999-12-31'),
                lead(startdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by startdate
                ),
                enddate
            ) as pay_range_end_date,
            name as pay_range_name,
            minimumpay::number(38, 5) as pay_range_minimum,
            midpoint::number(38, 5) as pay_range_midpoint,
            maximumpay::number(38, 5) as pay_range_maximum,
            jobfunctionflx as professional_field_code,
            paygradeflx as local_pay_grade_code,
            geozoneflx as geo_zone_code,
            status as pay_grade_status
        from {{ ref('stg_fo_pay_range_flatten') }}
        where dbt_valid_to is null
    )
select
    pay_range_code,
    pay_range_start_date,
    pay_range_end_date,
    pay_range_name,
    pay_range_minimum,
    pay_range_midpoint,
    pay_range_maximum,
    fo_pay.professional_field_code,
    fo_pay.local_pay_grade_code,
    fo_pay.geo_zone_code,
    fo_pay.pay_grade_status,
    hash(pay_range_code, pay_range_start_date) as pay_range_id,
    prof.professional_field_id,
    geo.geozone_id,
    lpg.local_pay_grade_id
from fo_pay
left outer join
    {{ ref("professional_field_v1") }} prof
    on fo_pay.professional_field_code = prof.professional_field_code
    and fo_pay.pay_range_start_date <= prof.professional_field_end_date
    and fo_pay.pay_range_end_date >= prof.professional_field_start_date
left outer join
    {{ ref("geozone_v1") }} geo
    on fo_pay.geo_zone_code = geo.geozone_code
    and fo_pay.pay_range_start_date <= geo.geozone_end_date
    and fo_pay.pay_range_end_date >= geo.geozone_start_date
left outer join
    {{ ref("local_pay_grade_v2") }} lpg
    on lpg.local_pay_grade_code = fo_pay.local_pay_grade_code
    and fo_pay.pay_range_start_date <= lpg.local_pay_grade_end_date
    and fo_pay.pay_range_end_date >= lpg.local_pay_grade_start_date

qualify
    row_number() over (
        partition by fo_pay.pay_range_code, fo_pay.pay_range_start_date
        order by
            prof.professional_field_start_date desc,
            geo.geozone_start_date desc,
            lpg.local_pay_grade_start_date desc
    )
    = 1
