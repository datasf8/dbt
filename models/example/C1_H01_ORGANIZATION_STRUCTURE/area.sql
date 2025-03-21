{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select
    hash(externalcode, startdate) as area_id,
    externalcode as area_code,
    startdate as area_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as area_end_date,
    name as area_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as area_name_fr,
    cust_businessunit_externalcode as hr_division_code,
    hr_division_id,
    status as area_status
from {{ ref("stg_fo_division_flatten") }} sfdf
left join
    {{ ref("hr_division") }} hd
    on sfdf.cust_businessunit_externalcode = hd.hr_division_code
    and sfdf.startdate between hd.hr_division_start_date and hd.hr_division_end_date
where sfdf.dbt_valid_to is null
