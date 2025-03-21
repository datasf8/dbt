{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_internal_mobility_agg")) }}
from {{ ref("fact_internal_mobility_agg") }}