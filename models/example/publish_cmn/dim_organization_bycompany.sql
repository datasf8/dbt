{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    company_v1 as (
        select *
        from {{ ref("company_v1") }}
        where company_start_date <= current_date()
        qualify
            row_number() over (
                partition by company_code order by company_start_date desc
            )
            = 1
    ),
    country_v1 as (
        select *
        from {{ ref("country_v1") }}
        where country_start_date <= current_date()
        qualify
            row_number() over (
                partition by country_code order by country_start_date desc
            )
            = 1
    ),
    geographic_zone_v1 as (
        select *
        from {{ ref("geographic_zone_v1") }}
        where geographic_zone_start_date <= current_date()
        qualify
            row_number() over (
                partition by geographic_zone_code
                order by geographic_zone_start_date desc
            )
            = 1
    )
select
    hash(company_code) as organization_bycompany_sk,
    company_code,
    company_name_en,
    country_code,
    country_name_en,
    geographic_zone_code,
    geographic_zone_name_en
from company_v1
left join country_v1 using (country_code)
left join geographic_zone_v1 using (geographic_zone_code)
union all
select -1, null, null, null, null, null, null
