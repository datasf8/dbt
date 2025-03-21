{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ecpja as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_ec_position_job_architecture
        --where posj_sk_orgn <> '-1' and  posj_sk_joar <>'-1'
    )

select
    posj_sk_orgn as posj_pk_orgn,
    posj_sk_joar as posj_pk_joar,
    posj_dt_begin_posj,
    posj_dt_end_posj
from ecpja
union
select '-1',NUll,Null,Null
