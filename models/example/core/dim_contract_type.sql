{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ec_contract as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_contract_type
        where coty_sk_coty <> '-1'
    )
select
    coty_sk_coty as coty_pk_coty,
    coty_id_coty,
    coty_code_coty,
    coty_label_coty,
    coty_status_coty
from ec_contract
union 
select '-1',Null,Null,Null,Null
