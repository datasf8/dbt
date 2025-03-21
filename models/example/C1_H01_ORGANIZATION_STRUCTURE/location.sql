{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    fo_loc as (
        select
            hash(externalcode, startdate) as location_id,
            externalcode as location_code,
            startdate as location_start_date,
            iff(
                enddate in ('9999-12-31'),
                lead(startdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by startdate
                ),
                enddate
            ) as location_end_date,
            name as location_name,
            timezone as location_time_zone,
            locationgroup as location_group_code,
            addressnavdeflt_address1 as location_address_1,
            addressnavdeflt_address2 as location_address_2,
            addressnavdeflt_address3 as location_address_3,
            addressnavdeflt_address5 as location_second_address_line,
            addressnavdeflt_address6 as location_town,
            addressnavdeflt_address7 as district_code,
            addressnavdeflt_address8 as location_building_number,
            addressnavdeflt_address9 as location_building,
            addressnavdeflt_address4 as location_apartment,
            addressnavdeflt_city as location_city,
            addressnavdeflt_county as location_county,
            addressnavdeflt_customstring1 as location_latitude,
            addressnavdeflt_customstring2 as location_longitude,
            -- addressnavdeflt_address18 as region_type_code,
            addressnavdeflt_country as country_code,
            addressnavdeflt_address12 as location_floor,
            addressnavdeflt_province as location_province,
            addressnavdeflt_state as location_state,
            addressnavdeflt_zipcode as location_zip_code,
            geozoneflx as geo_zone_code,
            standardhours::decimal(38, 2) as location_standard_weekly_hours,
            status as location_status
        from {{ ref("stg_fo_location_flatten") }}
        where dbt_valid_to is null
    )

select fo_loc.*, location_group_id, country_id, geozone_id,
from fo_loc
left outer join
    {{ ref("location_group_v1") }} locgrp
    on locgrp.location_group_code = fo_loc.location_group_code
    and fo_loc.location_start_date <= locgrp.location_group_end_date
    and fo_loc.location_end_date >= locgrp.location_group_start_date
left outer join
    {{ ref("country_v1") }} ctry
    on ctry.country_code = fo_loc.country_code
    and fo_loc.location_start_date <= ctry.country_end_date
    and fo_loc.location_end_date >= ctry.country_start_date
left outer join
    {{ ref("geozone_v1") }} geo
    on geo.geozone_code = fo_loc.geo_zone_code
    and fo_loc.location_start_date <= geo.geozone_end_date
    and fo_loc.location_end_date >= geo.geozone_start_date

qualify
    row_number() over (
        partition by location_id
        order by
            geo.geozone_start_date desc,
            ctry.country_start_date desc,
            locgrp.location_group_start_date desc
    )
    = 1
