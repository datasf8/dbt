{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_indicators_referential")) }}
from {{ ref("dim_indicators_referential") }}
