{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    job_arch as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_job_architecture WHERE joar_sk_joar <> '-1'
    )

select
    joar_sk_joar as joar_pk_joar,
    joar_dt_begin_joar,
    joar_dt_end_joar,
    joar_job_role_code_joar,
    joar_job_role_joar,
    joar_specialization_code_joar,
    joar_specialization_joar,
    joar_professional_field_code_joar,
    joar_professional_field_joar
from job_arch
union
select '-1', Null, Null, null, null,null,null,null,null

