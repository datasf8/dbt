{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_time_management_leaves")) }}
from {{ ref("fact_time_management_leaves") }}