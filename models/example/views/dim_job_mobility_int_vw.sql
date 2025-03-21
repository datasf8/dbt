{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_job_mobility_int")) }}
from {{ ref("dim_job_mobility_int") }}
