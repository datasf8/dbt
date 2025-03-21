{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_pay_grade")) }}
from {{ ref("dim_pay_grade") }}
