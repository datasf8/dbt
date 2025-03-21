{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_time_management_presence")) }}
from {{ ref("fact_time_management_presence") }}