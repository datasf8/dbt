{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_job")) }}
from {{ ref("dim_job") }}
