{{ config(schema="lrn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_dq_missing_completion")) }}
from {{ ref("fact_dq_missing_completion") }}
