select
    coty_pk_coty as contract_type_key,
    coty_code_coty as contract_type_code,
    coty_label_coty as contract_type_label

from {{ ref("dim_contract_type_snapshot") }}
