{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_job_architecture_v1")) }}
from {{ ref("dim_job_architecture_v1") }}
