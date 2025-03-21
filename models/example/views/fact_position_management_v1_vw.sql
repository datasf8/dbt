{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_position_management_v1")) }}
from {{ ref("fact_position_management_v1") }}
