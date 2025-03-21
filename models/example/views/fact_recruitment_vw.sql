{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_recruitment")) }}
from {{ ref("fact_recruitment") }}
