{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startdate) as geozone_id,
    externalcode as geozone_code,
    startdate as geozone_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as geozone_end_date,
    name as geozone_name,
    status as geozone_status
from {{ ref("stg_fo_geozone_flatten") }}
where dbt_valid_to is null
