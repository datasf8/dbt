{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_date")) }}
from {{ ref("dim_date") }}
