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
        select * 
        from {{ ref("stg_btdp_finance_dim_business_units") }}
        qualify
            row_number() over (
                partition by business_unit_code
                order by dbt_valid_from desc
            )
            = 1
    ),
    business_unit as (
        select *
        from {{ ref("business_unit") }}
        where business_unit_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_code order by business_unit_start_date desc
            )
            = 1
    )
select
    business_unit_id,
    multidivision_region_code,
    multidivision_region,
    multidivision_zone_code,
    multidivision_zone,
    multidivision_sub_zone_code,
    multidivision_sub_zone,
    multidivision_cluster_code,
    multidivision_cluster,
    country_code as multidivision_country_code,
    country_name_en as multidivision_country,
    legal_entity_code,
    legal_entity,
    business_unit_code,
    business_unit
from fin_org forg 
join business_unit bu using (business_unit_code)
left join  {{ ref("company") }} using (company_id)
left join  {{ ref("country") }}  using (country_id)
union all
select -1, null, null, null, null, null, null, null, null, null, null, null, null, null, null