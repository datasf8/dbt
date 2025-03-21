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
    cost_center_id as organization_id,
    cost_center_start_date as organization_start_date,
    cost_center_end_date as organization_end_date,
    cost_center_code,
    cost_center_name_en,
    cost_center_group_id,
    cost_center_group_code,
    cost_center_group_name_en,
    business_unit_id,
    business_unit_code,
    business_unit_name_en,
    business_unit_type_id,
    business_unit_type_code,
    business_unit_type_name_en,
    hub_id,
    hub_code,
    hub_name_en,
    company_id,
    company_code,
    company_name_en,
    geographic_zone_id,
    geographic_zone_code,
    geographic_zone_name_en,
    hr_division_id,
    hr_division_code,
    hr_division_name_en,
    area_id,
    area_code,
    area_name_en,
    country_id,
    country_code,
    country_name_en,
    is_ec_live_flag
from {{ ref("cost_center") }}
left join {{ ref("cost_center_group") }} using (cost_center_group_id)  -- From Cost Center
left join {{ ref("business_unit") }} using (business_unit_id)  -- From Cost Center
left join {{ ref("business_unit_type") }} using (business_unit_type_id)  -- From Business Unit
left join {{ ref("company") }} using (company_id)  -- From Business Unit
left join {{ ref("country") }} using (country_id)  -- From Company
left join {{ ref("geographic_zone") }} using (geographic_zone_id)  -- From Company
left join {{ ref("area") }} using (area_id)  -- From Business Unit
left join {{ ref("hr_division") }} using (hr_division_id)  -- From Business Unit
left join {{ ref("hub") }} using (hub_id)  -- From Business Unit
