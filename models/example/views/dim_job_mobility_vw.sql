{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_job_mobility")) }}
from {{ ref("dim_job_mobility") }}
