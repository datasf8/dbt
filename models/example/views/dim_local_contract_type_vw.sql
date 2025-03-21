{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_local_contract_type")) }}
from {{ ref("dim_local_contract_type") }}
