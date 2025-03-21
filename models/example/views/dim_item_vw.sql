{{ config(schema="lrn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_item")) }}
from {{ ref("dim_item") }}
