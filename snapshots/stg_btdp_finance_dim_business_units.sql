{% snapshot stg_btdp_finance_dim_business_units %}
    {{
        config(
            unique_key="business_unit_code",
            strategy="check",
            check_cols="all",
            target_schema="SDDS_STG_SCH",
        )
    }}

    select
        parse_json($1):affair_type::string as affair_type,
        parse_json($1):affair_type_code::string as affair_type_code,
        parse_json($1):am_nam::string as am_nam,
        parse_json($1):am_nam_code::string as am_nam_code,
        parse_json($1):animation_division::string as animation_division,
        parse_json($1):animation_division_code::string as animation_division_code,
        parse_json($1):animation_grand_zone::string as animation_grand_zone,
        parse_json($1):animation_grand_zone_code::string as animation_grand_zone_code,
        parse_json($1):animation_region::string as animation_region,
        parse_json($1):animation_region_code::string as animation_region_code,
        parse_json($1):animation_sub_zone::string as animation_sub_zone,
        parse_json($1):animation_sub_zone_code::string as animation_sub_zone_code,
        parse_json($1):animation_zone::string as animation_zone,
        parse_json($1):animation_zone_code::string as animation_zone_code,
        parse_json($1):business_unit::string as business_unit,
        parse_json($1):business_unit_code::string as business_unit_code,
        parse_json($1):business_unit_long_description::string
        as business_unit_long_description,
        parse_json($1):currency::string as currency,
        parse_json($1):currency_factor::string as currency_factor,
        parse_json($1):division_code::string as division_code,
        parse_json($1):group_affair_type::string as group_affair_type,
        parse_json($1):group_affair_type_code::string as group_affair_type_code,
        parse_json($1):iso_country::string as iso_country,
        parse_json($1):iso_country_code::string as iso_country_code,
        parse_json($1):legal_entity::string as legal_entity,
        parse_json($1):legal_entity_code::string as legal_entity_code,
        parse_json($1):multidivision_cluster::string as multidivision_cluster,
        parse_json($1):multidivision_cluster_code::string as multidivision_cluster_code,
        parse_json($1):multidivision_region::string as multidivision_region,
        parse_json($1):multidivision_region_code::string as multidivision_region_code,
        parse_json($1):multidivision_sub_zone::string as multidivision_sub_zone,
        parse_json($1):multidivision_sub_zone_code::string
        as multidivision_sub_zone_code,
        parse_json($1):multidivision_zone::string as multidivision_zone,
        parse_json($1):multidivision_zone_code::string as multidivision_zone_code
    from
        '@HRDP_LND_{{ env_var("DBT_SNOWFLAKE_ACCOUNT") }}_DB.PMG_LND_SCH.GCS_INT_STAGE_{{ env_var("DBT_SNOWFLAKE_ACCOUNT") }}/btdp/finance/dim_bu/'
        (file_format => "HRDP_LND_{{ env_var("DBT_SNOWFLAKE_ACCOUNT") }}_DB"."PMG_LND_SCH"."JSON_FF")

{% endsnapshot %}


 