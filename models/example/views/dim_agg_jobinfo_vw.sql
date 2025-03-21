{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_agg_jobinfo")) }}
from {{ ref("dim_agg_jobinfo") }}
