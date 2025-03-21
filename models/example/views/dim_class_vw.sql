{{ config(schema="lrn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_class")) }}
from {{ ref("dim_class") }}
