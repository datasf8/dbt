{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_hires")) }}
from {{ ref("fact_hires") }}
