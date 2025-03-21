{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_organization_v1")) }}
from {{ ref("dim_organization_v1") }}
where is_ec_live_flag = true
