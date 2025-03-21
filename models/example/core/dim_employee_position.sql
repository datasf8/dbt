{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    ecp as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_ec_employee_position
        where empp_sk_empl <> '-1' and empp_sk_orgn <> '-1'
    )
select
    empp_sk_empl as empp_pk_empl,
    empp_sk_orgn as empp_pk_orgn,
    empp_dt_begin_empp,
    empp_dt_end_empp
from ecp
union
select '-1',Null,Null,Null
