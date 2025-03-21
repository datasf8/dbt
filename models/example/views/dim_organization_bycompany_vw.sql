{{ config(schema="cmn_pub_sch") }}
select
    organization_bycompany_sk,
    company_code,
    company_name_en,
    country_code,
    collate(country_name_en,'en-ci') as country_name_en,
    geographic_zone_code,
    collate(geographic_zone_name_en,'en-ci') as geographic_zone_name_en
from {{ ref('dim_organization_bycompany') }}
