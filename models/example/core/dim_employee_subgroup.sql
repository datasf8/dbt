{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    employee_subgroup as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_employee_subgroup
        where emsg_sk_emsg <> '-1'
    )
select
    emsg_sk_emsg as emsg_pk_emsg,
    emsg_code_emsg,
    emsg_effectivestartdate_emsg,
    emsg_label_emsg
from employee_subgroup
union
select '-1',Null,Null,Null
