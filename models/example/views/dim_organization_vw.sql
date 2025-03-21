select
    orgn_pk_organisation_orgn as organization_key,
    orgn_position_code_orgn as position_code,
    orgn_position_orgn as position,
    orgn_dt_begin_orgn as begin_date,
    orgn_dt_end_orgn as end_date,
    orgn_cost_center_code_orgn as cost_center_code,
    orgn_cost_center_orgn as cost_center,
    orgn_business_unit_code_orgn as business_unit_code,
    orgn_business_unit_orgn as business_unit,
    collate(
        orgn_business_unit_code_orgn || ' (' || orgn_business_unit_orgn || ')', 'en-ci'
    ) as business_unit_full,
    orgn_type_business_unit_code_orgn as type_business_unit_code,
    orgn_type_business_unit_orgn as type_business_unit,
    orgn_area_code_orgn as area_code,
    orgn_area_orgn as area,
    collate(orgn_area_code_orgn || ' (' || orgn_area_orgn || ')', 'en-ci') as area_full,

    orgn_brand_code_orgn as brand_code,
    orgn_brand_orgn as brand,
    orgn_division_code_orgn as division_code,
    orgn_division_orgn as division,
    collate(
        orgn_division_code_orgn || ' (' || orgn_division_orgn || ')', 'en-ci'
    ) as division_full,

    orgn_legal_entity_code_orgn as company,
    orgn_legal_entity_orgn as legal_entity,
    orgn_location_code_orgn as location_code,
    orgn_location_orgn as location,
    orgn_location_group_code_orgn as location_group_code,
    orgn_location_group_orgn as location_group,
    orgn_country_code_orgn as country_code,
    orgn_country_orgn as country,
    orgn_zone_code_orgn as zone_code,
    collate(
        case
            when orgn_country_orgn = 'Panama'
            then 'Latin America Zone'
            when orgn_zone_orgn in ('Western Europe', 'Eastern Europe')
            then 'Europe'
            else orgn_zone_orgn
        end,
        'en-ci'
    ) as zone,
    collate(
        iff(
            upper(orgn_type_business_unit_orgn) = upper('Country - PLANT'),
            'Plants Only',
            'Without Plants'
        ),
        'en-ci'
    ) as plant,
    collate(
        iff(
            upper(orgn_type_business_unit_orgn) = upper('Country - Central'),
            'DC Only',
            'Without DC'
        ),
        'en-ci'
    ) as dc
from {{ ref("dim_organization_snapshot") }}
where nvl(orgn_country_code_orgn || orgn_zone_code_orgn, 'NULL') <> 'USAEO'
