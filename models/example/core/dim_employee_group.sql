{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ec_emp_grp as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_employee_group
        where emgp_sk_emgp <> '-1'
    )

select emgp_sk_emgp as emgp_pk_emgp, emgp_code_emgp, emgp_label_emgp
from ec_emp_grp
union
select '-1',Null,Null
