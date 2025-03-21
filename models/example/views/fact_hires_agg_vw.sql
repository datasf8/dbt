{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_hires_agg")) }}
from {{ ref("fact_hires_agg") }}
