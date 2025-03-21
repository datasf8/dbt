{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_date_by_month")) }}
from {{ ref("dim_date_by_month") }}
