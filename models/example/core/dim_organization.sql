{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    organization as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_organization
        where orgn_sk_orgn <> '-1'
    )

select
    orgn_sk_orgn as orgn_pk_organisation_orgn,
    orgn_position_code_orgn,
    orgn_position_orgn,
    orgn_dt_begin_orgn,
    orgn_dt_end_orgn,
    orgn_cost_center_code_orgn,
    orgn_cost_center_orgn,
    orgn_business_unit_code_orgn,
    orgn_business_unit_orgn,
    orgn_type_business_unit_code_orgn,
    orgn_type_business_unit_orgn,
    orgn_area_code_orgn,
    orgn_area_orgn,
    orgn_brand_code_orgn,
    orgn_brand_orgn,
    orgn_division_code_orgn,
    orgn_division_orgn,
    orgn_legal_entity_code_orgn,
    orgn_legal_entity_orgn,
    orgn_location_code_orgn,
    orgn_location_orgn,
    orgn_location_group_code_orgn,
    orgn_location_group_orgn,
    orgn_country_code_orgn,
    orgn_country_orgn,
    orgn_zone_code_orgn,
    orgn_zone_orgn,
    '' as orgn_creation_date_orgn,
    '' as orgn_modification_date_orgn
from organization
union
select '-1', Null, Null, null, null,null,null,null,null,null,null,null,null,null,Null, 
Null, null, null,null,null,null,null,null,null,null,null,null,null,null