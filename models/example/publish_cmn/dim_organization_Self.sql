{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
         )
}}
with
     business_unit_v1 as (
        select *
        from {{ ref("business_unit_v1") }}
        where business_unit_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_code order by business_unit_start_date desc
            )
            = 1
    ),
    business_unit_type_v1 as (
        select *
        from {{ ref("business_unit_type_v1") }}
        where business_unit_type_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_type_code
                order by business_unit_type_start_date desc
            )
            = 1
    ),
   company_v2 as (
        select *
        from {{ ref("company_v2") }}
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
Select 
business_unit_id,
business_unit_code,
business_unit_name_en,
business_unit_name_fr,
business_unit_type_id,
business_unit_type_code,
business_unit_type_name_en,
business_unit_type_name_fr,
company_id,
company_code,
company_name_en,
company_name_fr,
country_id,
country_code,
country_name_en,
country_name_fr,
geographic_zone_id,
geographic_zone_code,
geographic_zone_name_en,
geographic_zone_name_fr
from business_unit_v1
left join company_v2 using (company_code)
left join business_unit_type_v1 using (business_unit_type_code)
left join country_v1 using (country_id)
left join geographic_zone_v1 using (geographic_zone_id)
union all
select -1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null