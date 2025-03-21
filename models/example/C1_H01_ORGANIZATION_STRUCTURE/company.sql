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
    hash(externalcode, startdate) company_id,
    externalcode as company_code,
    startdate as company_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as company_end_date,
    name as company_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as company_name_fr,
    description as company_description,
    description_fr_fr as company_description_fr,
    gz.geographic_zone_id,
    c.country_id,
    cust_geographic_zone as geographic_zone_code,
    country as country_code,
    standardhours::number(38,1) as company_std_weekly_hours,
    status as company_status
from {{ ref("stg_fo_company_flatten") }} sfcf
left join
    {{ ref("geographic_zone") }} gz
    on sfcf.cust_geographic_zone = gz.geographic_zone_code
    and sfcf.startdate
    between gz.geographic_zone_start_date and gz.geographic_zone_end_date
left join
    {{ ref("country") }} c
    on sfcf.country = c.country_code
    and sfcf.startdate between c.country_start_date and c.country_end_date
where sfcf.dbt_valid_to is null