{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_headcount_worls")) }}
from {{ ref("fact_headcount_worls") }}
