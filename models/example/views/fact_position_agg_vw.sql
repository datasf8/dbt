{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_position_agg")) }}
from {{ ref("fact_position_agg") }}
