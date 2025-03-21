{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_hires_ec")) }}
from {{ ref("fact_hires_ec") }}
