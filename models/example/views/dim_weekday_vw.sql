{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_weekday")) }}
from {{ ref("dim_weekday") }}