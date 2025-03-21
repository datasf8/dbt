{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_csrd_headcount_details_histo")) }}
from {{ ref("fact_csrd_headcount_details_histo") }}
