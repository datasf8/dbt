{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startdate) as location_group_id,
    externalcode as location_group_code,
    startdate as location_group_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as location_group_end_date,
    name as location_group_name,
    description as location_group_description,
    status as location_group_status
from {{ ref("stg_fo_location_group_flatten") }}
where dbt_valid_to is null
