{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        cluster_by=["organization_sk"],
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
    ),
    area_v1 as (
        select *
        from {{ ref("area_v1") }}
        where area_start_date <= current_date()
        qualify
            row_number() over (partition by area_code order by area_start_date desc) = 1
    ),
    hr_division_v1 as (
        select *
        from {{ ref("hr_division_v1") }}
        where hr_division_start_date <= current_date()
        qualify
            row_number() over (
                partition by hr_division_code order by hr_division_start_date desc
            )
            = 1
    ),
    hub_v1 as (
        select *
        from {{ ref("hub_v1") }}
        where hub_start_date <= current_date()
        qualify
            row_number() over (partition by hub_code order by hub_start_date desc) = 1
    )
select
    hash(cost_center_code, cost_center_start_date) as organization_sk,
    cost_center_code,
    cost_center_start_date as organization_start_date,
    cost_center_end_date as organization_end_date,
    cost_center_name_en || ' (' || cost_center_code || ')' as cost_center_name_en,
    business_unit_code,
    business_unit_name_en || ' (' || business_unit_code || ')' as business_unit_name_en,
    business_unit_type_code,
    business_unit_type_name_en,
    company_code,
    company_name_en || ' (' || company_code || ')' as company_name_en,
    country_code,
    country_name_en,
    is_ec_live_flag,
    geographic_zone_code,
    geographic_zone_name_en,
    area_code,
    area_name_en || ' (' || area_code || ')' as area_name_en,
    hr_division_code,
    hr_division_name_en || ' (' || hr_division_code || ')' as hr_division_name_en,
    hub_code,
    hub_name_en,
    collate(
        iff(
            business_unit_type_name_en like 'Country - PLANT',
            'Plants only',
            'Without Plants'
        ),
        'en-ci'
    ) as plant,
    collate(
        iff(
            business_unit_type_name_en like 'Country - Central',
            'DCs only',
            'Without DC'
        ),
        'en-ci'
    ) as dc
from {{ ref("cost_center_v1") }}
left join business_unit_v1 using (business_unit_code)
left join business_unit_type_v1 using (business_unit_type_code)
left join company_v1 using (company_code)
left join country_v1 using (country_code)
left join geographic_zone_v1 using (geographic_zone_code)
left join area_v1 using (area_code)
left join hr_division_v1 using (hr_division_code)
left join hub_v1 on business_unit_v1.hub_id = hub_v1.hub_code
union all
select
    -1,
    '',
    '1900-01-01',
    '9999-12-31',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    'Without Plants',
    'Without DC'
