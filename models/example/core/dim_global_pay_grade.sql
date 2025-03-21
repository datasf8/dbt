{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    global_pay_grade as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_global_pay_grade
        where gpgr_sk_gpgr <> '-1'
    )

select
    gpgr_sk_gpgr as gpgr_pk_gpgr,
    gpgr_code_gpgr,
    gpgr_label_gpgr,
    gpgr_dt_begin_gpgr,
    gpgr_dt_end_gpgr,
    gpgr_status_gpgr,
    gpgr_level_gpgr
from global_pay_grade
union
select '-1',NUll,Null,Null,NUll,Null,NUll
