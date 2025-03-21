{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    emp_job_level as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_job_level
        where jlvl_sk_jlvl <> '-1'
    )

select
    jlvl_sk_jlvl as jlvl_pk_jlvl,
    jlvl_id_jlvl,
    jlvl_code_jlvl,
    jlvl_label_jlvl,
    jlvl_status_jlvl
from emp_job_level
union
select '-1',NUll,Null,Null,NUll
