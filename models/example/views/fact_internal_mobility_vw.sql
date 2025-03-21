{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_internal_mobility")) }}
from {{ ref("fact_internal_mobility") }}
