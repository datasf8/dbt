{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ec_home_host as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_home_host
        where hoho_sk_hoho <> '-1'
    )
select
    hoho_sk_hoho as hoho_pk_hoho,
    hoho_id_hoho,
    hoho_code_hoho,
    hoho_label_hoho,
    hoho_status_hoho
from ec_home_host
union
select '-1',NUll,Null,Null,NUll
