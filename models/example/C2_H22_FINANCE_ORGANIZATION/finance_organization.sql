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
    fin_org as (
        select * replace (trunc(dbt_valid_from, 'day') as dbt_valid_from)
        from {{ ref("stg_btdp_finance_dim_business_units") }}
        qualify
            row_number() over (
                partition by business_unit_code, trunc(dbt_valid_from, 'day')
                order by dbt_valid_from desc
            )
            = 1
    ),
    business_unit as (
        select *
        from {{ ref("business_unit") }}
        where business_unit_start_date <= current_date
        qualify
            row_number() over (
                partition by business_unit_code order by business_unit_start_date desc
            )
            = 1
    )
select
    hash(business_unit_code, dbt_valid_from) finance_organization_id,
    dbt_valid_from as finance_organization_start_date,
    nvl(dbt_valid_to, '9999-12-31') as finance_organization_end_date,
    multidivision_region,
    multidivision_region_code,
    multidivision_zone,
    multidivision_zone_code,
    multidivision_sub_zone,
    multidivision_sub_zone_code,
    multidivision_cluster,
    multidivision_cluster_code,
    country_id,
    country_name_en,
    country_code,
    company_id,
    company_name_en,
    company_code,
    business_unit_id,
    business_unit as business_unit_name_en,
    business_unit_code  -- select count(*)
from fin_org forg  -- 5388
join business_unit bu using (business_unit_code)
left join {{ ref("company") }} using (company_id)
left join {{ ref("country") }} using (country_id)
