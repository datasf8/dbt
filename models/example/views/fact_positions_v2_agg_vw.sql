{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_positions_v2_agg")) }}
from {{ ref("fact_positions_v2_agg") }}