{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_agg_personalinfo")) }}
from {{ ref("dim_agg_personalinfo") }}
