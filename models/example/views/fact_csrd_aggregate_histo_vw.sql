{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_csrd_aggregate_histo")) }}
from {{ ref("fact_csrd_aggregate_histo") }}
