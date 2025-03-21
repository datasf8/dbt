{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="
        ALTER TABLE {{ this }} SET DATA_RETENTION_TIME_IN_DAYS=90;",
    )
}}


select
    business_unit_code,
    concat(
        business_unit_name_en, ' (', business_unit_code, ')'
    ) as business_unit_name_en,
    business_unit_type_code,
    business_unit_type_name_en,
    company_code,
    concat(company_name_en, ' (', company_code, ')') as company_name_en,
    country_code,
    country_name_en,
    geographic_zone_code,
    geographic_zone_name_en,
    csrd_measure_id,
    date_id,
    csrd_value_amount,
    'CSR' as headcount_type_code
from {{ ref("fact_csrd_agg_histo") }}
union all
select
    business_unit_code,
    concat(
        business_unit_name_en, ' (', business_unit_code, ')'
    ) as business_unit_name_en,
    business_unit_type_code,
    business_unit_type_name_en,
    company_code,
    concat(company_name_en, ' (', company_code, ')') as company_name_en,
    country_code,
    country_name_en,
    geographic_zone_code,
    geographic_zone_name_en,
    csrd_measure_id,
    date_id,
    csrd_value_amount,
    'STA' as headcount_type_code
from {{ ref("fact_sta_agg") }}
