{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ec_position as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_key_position
        where kpos_sk_kpos <> '-1'
    )

select
    kpos_sk_kpos as kpos_pk_kpos,
    kpos_id_kpos,
    kpos_code_kpos,
    kpos_label_kpos,
    kpos_status_kpos
from ec_position
union
select '-1',NUll,Null,Null,NUll
