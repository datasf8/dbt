{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_turnover")) }}
from {{ ref("fact_turnover") }}
