{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_recruitment_agg")) }}
from {{ ref("fact_recruitment_agg") }}
