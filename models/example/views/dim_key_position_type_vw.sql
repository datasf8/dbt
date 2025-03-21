{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_key_position_type")) }}
from {{ ref("dim_key_position_type") }}
