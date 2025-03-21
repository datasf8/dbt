{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_hr_cockpit")) }}
from {{ ref("fact_hr_cockpit") }}
