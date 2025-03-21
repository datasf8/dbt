{{ config(schema="lrn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_dq_inaccurate_history")) }}
from {{ ref("fact_dq_inaccurate_history") }}
