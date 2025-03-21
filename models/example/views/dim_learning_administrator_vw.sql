{{ config(schema="lrn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_learning_administrator")) }}
from {{ ref("dim_learning_administrator") }}
