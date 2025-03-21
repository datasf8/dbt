{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_csrd_local_contract_type_mapping")) }}
from {{ ref("dim_csrd_local_contract_type_mapping") }}
