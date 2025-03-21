{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    local_pay_grade as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_local_pay_grade
        where lpgr_sk_lpgr <> '-1'
    )
select
    lpgr_sk_lpgr as lpgr_pk_lpgr,
    lpgr_code_lpgr,
    lpgr_label_lpgr,
    lpgr_dt_begin_lpgr,
    lpgr_dt_end_lpgr,
    lpgr_status_lpgr,
    lpgr_sk_gpgr as lpgr_pk_gpgr,
    lpgr_code_country_lpgr
from local_pay_grade
union
select '-1',NUll,Null,Null,NUll,Null,Null,NUll